output "raw_bucket_name" {
  description = "Name of the GCS raw landing zone bucket"
  value       = google_storage_bucket.raw_landing.name
}

output "raw_bucket_url" {
  description = "GCS URL of the raw landing zone"
  value       = "gs://${google_storage_bucket.raw_landing.name}"
}

output "bq_raw_dataset" {
  description = "BigQuery RAW dataset ID"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "bq_staging_dataset" {
  description = "BigQuery STAGING dataset ID"
  value       = google_bigquery_dataset.staging.dataset_id
}

output "bq_intermediate_dataset" {
  description = "BigQuery INTERMEDIATE dataset ID"
  value       = google_bigquery_dataset.intermediate.dataset_id
}

output "bq_marts_dataset" {
  description = "BigQuery MARTS dataset ID"
  value       = google_bigquery_dataset.marts.dataset_id
}

output "pipeline_service_account_email" {
  description = "Email of the pipeline service account"
  value       = google_service_account.pipeline_sa.email
}

output "pipeline_sa_key_base64" {
  description = "Base64-encoded service account key — add this as GCP_SA_KEY secret in GitHub"
  value       = google_service_account_key.pipeline_sa_key.private_key
  sensitive   = true
}

output "slack_secret_name" {
  description = "Secret Manager resource name for the Slack webhook"
  value       = google_secret_manager_secret.slack_webhook.name
}
