# ─────────────────────────────────────────────
# Random suffix to ensure globally unique names
# ─────────────────────────────────────────────
resource "random_id" "suffix" {
  byte_length = 4
}

# ─────────────────────────────────────────────
# Enable required GCP APIs
# ─────────────────────────────────────────────
resource "google_project_service" "apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "secretmanager.googleapis.com",
    "bigquerystorage.googleapis.com",
  ])

  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

# ─────────────────────────────────────────────
# GCS — Raw Data Landing Zone
# ─────────────────────────────────────────────
resource "google_storage_bucket" "raw_landing" {
  name          = "sec-raw-${var.project_id}-${random_id.suffix.hex}"
  project       = var.project_id
  location      = var.region
  force_destroy = false

  # Prevent accidental public access
  public_access_prevention = "enforced"

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = {
    environment = var.environment
    project     = "sec-pipeline"
    managed_by  = "terraform"
  }

  depends_on = [google_project_service.apis]
}

# ─────────────────────────────────────────────
# BigQuery Datasets — Four Layer Architecture
# ─────────────────────────────────────────────
resource "google_bigquery_dataset" "raw" {
  dataset_id    = "sec_raw"
  friendly_name = "SEC Raw"
  description   = "Direct load from GCS. Append-only. No transformations."
  project       = var.project_id
  location      = var.bq_location

  labels = {
    layer       = "raw"
    environment = var.environment
    managed_by  = "terraform"
  }

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_dataset" "staging" {
  dataset_id    = "sec_staging"
  friendly_name = "SEC Staging"
  description   = "dbt staging models: rename columns, cast types, standardize formatting."
  project       = var.project_id
  location      = var.bq_location

  labels = {
    layer       = "staging"
    environment = var.environment
    managed_by  = "terraform"
  }

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_dataset" "intermediate" {
  dataset_id    = "sec_intermediate"
  friendly_name = "SEC Intermediate"
  description   = "dbt intermediate models: joins, deduplication, business logic."
  project       = var.project_id
  location      = var.bq_location

  labels = {
    layer       = "intermediate"
    environment = var.environment
    managed_by  = "terraform"
  }

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_dataset" "marts" {
  dataset_id    = "sec_marts"
  friendly_name = "SEC Marts"
  description   = "Final aggregated tables optimized for Streamlit reporting."
  project       = var.project_id
  location      = var.bq_location

  labels = {
    layer       = "marts"
    environment = var.environment
    managed_by  = "terraform"
  }

  depends_on = [google_project_service.apis]
}

# ─────────────────────────────────────────────
# Service Account — Pipeline Identity
# ─────────────────────────────────────────────
resource "google_service_account" "pipeline_sa" {
  account_id   = "sa-sec-pipeline"
  display_name = "SEC Pipeline Service Account"
  description  = "Least-privilege SA for Spark ingestion, BigQuery loading, dbt, and GE"
  project      = var.project_id

  depends_on = [google_project_service.apis]
}

# ─────────────────────────────────────────────
# IAM — Least Privilege Bindings
# ─────────────────────────────────────────────

# GCS: Read/Write raw bucket only
resource "google_storage_bucket_iam_member" "sa_gcs_raw" {
  bucket = google_storage_bucket.raw_landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# BigQuery: Data Editor on all four datasets
resource "google_bigquery_dataset_iam_member" "sa_bq_raw" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "sa_bq_staging" {
  dataset_id = google_bigquery_dataset.staging.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "sa_bq_intermediate" {
  dataset_id = google_bigquery_dataset.intermediate.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "sa_bq_marts" {
  dataset_id = google_bigquery_dataset.marts.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# BigQuery: Job User (required to run queries/jobs)
resource "google_project_iam_member" "sa_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Secret Manager: Accessor (for Slack webhook)
resource "google_project_iam_member" "sa_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# ─────────────────────────────────────────────
# Secret Manager — Slack Webhook
# ─────────────────────────────────────────────
resource "google_secret_manager_secret" "slack_webhook" {
  secret_id = "slack-webhook-url"
  project   = var.project_id

  replication {
    auto {}
  }

  labels = {
    managed_by = "terraform"
  }

  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "slack_webhook_val" {
  count = var.slack_webhook_url != "" ? 1 : 0

  secret      = google_secret_manager_secret.slack_webhook.id
  secret_data = var.slack_webhook_url

  lifecycle {
    ignore_changes = [secret_data]
  }
}

# ─────────────────────────────────────────────
# Service Account Key (for GitHub Actions auth)
# ─────────────────────────────────────────────
resource "google_service_account_key" "pipeline_sa_key" {
  service_account_id = google_service_account.pipeline_sa.name
  public_key_type    = "TYPE_X509_PEM_FILE"
}