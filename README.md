# SEC Insider Trading Pipeline

An end-to-end data engineering project that ingests SEC Form 4 insider trading filings from EDGAR, processes and models the data through a multi-layer warehouse architecture, and serves insights through an interactive dashboard.

Streamlit App @ https://sec-insider-report.streamlit.app
[![Pipeline](https://github.com/tonybaby16/sec-insider-report/actions/workflows/pipeline.yml/badge.svg)](https://github.com/tonybaby16/sec-insider-report/actions)
<img width="1203" height="793" alt="image" src="https://github.com/user-attachments/assets/80e1cef5-67c5-4dd1-a5b8-7de8a8dff4cc" />
<img width="1060" height="525" alt="image" src="https://github.com/user-attachments/assets/9c6cb430-e69b-4a1a-b4e9-a570940cc7ea" />


---

## What It Does

Corporate insiders — directors, officers, and major shareholders — are required to report their trades to the SEC within two business days via Form 4 filings. This pipeline collects those filings, structures them into a queryable data warehouse, and answers questions like:

- Which insiders have sold the most shares in the last 90 days?
- Which companies have the highest ratio of insider sells to buys?
- What is the net insider sentiment by company?
- Who are the most active insider traders by transaction value?

---

## Architecture
<img width="1408" height="768" alt="architecture" src="https://github.com/user-attachments/assets/5031abfe-ea8d-491e-8242-ec94f3333894" />

```
SEC EDGAR (Form 4 XML)
        │
        ▼
  Apache Spark ──────────► Google Cloud Storage
  (Extract & parse)         (Raw Parquet, partitioned by quarter)
                                    │
                                    ▼
                             BigQuery RAW Layer
                             (sec_raw dataset)
                                    │
                                    ▼
                               dbt Core
                    ┌──────────────┼──────────────┐
                    ▼              ▼               ▼
                Staging      Intermediate        Marts
              (sec_staging) (sec_intermediate) (sec_marts)
                                    │
                                    ▼
                            Streamlit Dashboard
                         (sec-insider-report.streamlit.app)

        Orchestrated by GitHub Actions · Infrastructure by Terraform
```

---

## Tech Stack

| Tool | Category | Purpose |
|---|---|---|
| **Terraform** | Infrastructure | Provisions all GCP resources as code — GCS bucket, BigQuery datasets, service accounts, IAM bindings |
| **Apache Spark** | Ingestion | Distributed extraction and parsing of SEC EDGAR Form 4 XML filings |
| **Google Cloud Storage** | Storage | Raw data lake — immutable Parquet files partitioned by quarter |
| **BigQuery** | Warehouse | Four-layer data architecture (raw → staging → intermediate → marts) |
| **dbt Core** | Transformation | SQL models, schema tests, and lineage across staging, intermediate, and mart layers |
| **GitHub Actions** | Orchestration | Monthly pipeline schedule — ingest → load → transform → test |
| **Streamlit** | Serving | Interactive dashboard with KPI metrics, trend charts, and insider leaderboards |

---

## Data Source

**SEC EDGAR Form 4 — Insider Trading Disclosures**

- URL: `https://www.sec.gov/Archives/edgar/full-index/`
- Format: Quarterly bulk index files + individual XML filings
- Volume: ~60,000–80,000 Form 4 filings per quarter
- Update frequency: Continuous — processed monthly by the pipeline
- Cost: Free, public domain

---

## Project Structure

```
sec-insider-report/
├── .devcontainer/                  # GitHub Codespaces environment
│   ├── devcontainer.json
│   └── post-create.sh
├── .github/
│   └── workflows/
│       └── pipeline.yml            # Production orchestration
├── terraform/                      # GCP infrastructure as code
│   ├── main.tf                     # All GCP resources
│   ├── variables.tf
│   ├── outputs.tf
│   ├── providers.tf
│   ├── bootstrap.sh                # One-time state bucket creation
│   └── terraform.tfvars.example
├── spark/                          # PySpark ingestion
│   ├── ingest_sec_form4.py         # Main ingestion script
│   └── tests/
│       ├── conftest.py
│       └── test_ingest_sec_form4.py
├── scripts/
│   └── load_gcs_to_bq.py          # GCS → BigQuery load job
├── dbt/                            # Transformation layer
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/                # Clean and standardise raw data
│       ├── intermediate/           # Deduplicate and enrich
│       └── marts/                  # Business-ready aggregates
├── streamlit/
│   ├── app.py                      # Dashboard application
│   └── requirements.txt
├── requirements.txt                # Full pipeline dependencies
└── README.md
```

---

## Pipeline Layers

### 1. Ingestion — Apache Spark
PySpark downloads quarterly bulk index files from SEC EDGAR, filters to Form 4 filings, fetches and parses individual XML filings, enforces a strict schema, and writes partitioned Parquet files to GCS. Rate limiting (500ms delay) and retry logic (3 attempts with exponential backoff) are built in to comply with SEC's request limits.

### 2. Raw Storage — Google Cloud Storage
Parquet files land at `gs://{bucket}/form4/quarter={YYYY}Q{N}/`. The raw zone is immutable — nothing overwrites it. This separation of storage from compute mirrors production data lake patterns.

### 3. Loading — BigQuery Native Load Job
The Python BigQuery client triggers native load jobs from GCS into `sec_raw.form4_transactions`. Load jobs from GCS are free and handle schema inference from Parquet metadata automatically.

### 4. Transformation — dbt Core

Three model tiers run inside BigQuery:

| Model | Layer | Purpose |
|---|---|---|
| `stg_form4_transactions` | Staging | Clean column names, cast types, add `buy_sell_flag` and `transaction_value_usd` |
| `int_insider_transactions` | Intermediate | Deduplicate 4/A amendments, add time dimensions, calculate `days_to_file` |
| `mrt_monthly_insider_activity` | Mart | Monthly buy/sell volume by ticker |
| `mrt_top_insider_traders` | Mart | Insider leaderboard ranked by transaction value |
| `mrt_company_insider_sentiment` | Mart | Company buy ratio, net position, sentiment label |

28 schema tests run after every `dbt run` to validate data integrity.

### 5. Orchestration — GitHub Actions
A single workflow DAG runs on the 1st of every month and on manual trigger. Inputs allow controlling which years, quarters, and pipeline steps to run — useful for backfills and debugging specific phases.

### 6. Dashboard — Streamlit
The app connects to BigQuery mart tables via service account credentials stored in Streamlit Cloud secrets. Four tabs: Monthly Trends, Top Insiders, Company Sentiment, and Raw Data Explorer with CSV download.

---

## Getting Started

### Prerequisites
- GCP project with billing enabled
- GitHub account
- `gcloud` CLI installed and authenticated

### 1. Clone the repo
```bash
git clone https://github.com/tonybaby16/sec-insider-report.git
cd sec-insider-report
```

### 2. Open in GitHub Codespaces (recommended)
Click **Code → Codespaces → Create codespace on main**. The environment installs all dependencies automatically via `post-create.sh`.

### 3. Provision infrastructure
```bash
# Create Terraform state bucket (run once)
bash terraform/bootstrap.sh

# Configure variables
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Edit terraform.tfvars with your project_id and state_bucket_name

# Apply infrastructure
cd terraform
terraform init -backend-config="bucket=YOUR_STATE_BUCKET"
terraform plan
terraform apply
```

### 4. Configure GitHub Secrets

| Secret | Description |
|---|---|
| `GCP_PROJECT_ID` | Your GCP project ID |
| `GCP_SA_KEY` | Base64 SA key: `terraform output -raw pipeline_sa_key_base64` |
| `GCS_RAW_BUCKET` | Bucket name: `terraform output raw_bucket_name` |
| `SEC_USER_AGENT` | Required by SEC: `"Your Name email@example.com"` |

### 5. Run the pipeline
Go to **Actions → SEC Pipeline → Run workflow**. For a test run set `max_filings = 50`.

### 6. Deploy the dashboard
- Go to [share.streamlit.io](https://share.streamlit.io) → New app
- Set main file path to `streamlit/app.py`
- Add secrets (see `terraform/terraform.tfvars.example` for format)

---

## Key Design Decisions

**GitHub Actions over Airflow for production** — Airflow is demonstrated locally for portfolio purposes, but GitHub Actions is used for the actual scheduled runs. This keeps the project at zero cost while showcasing understanding of DAG-based orchestration.

**Local write then GCS upload for Spark** — The Hadoop GCS connector has Guava version conflicts with Spark 3.5. Writing Parquet locally and uploading via the Python GCS client is simpler, faster on a single machine, and avoids JAR dependency issues entirely.

**Regex XML parser over lxml** — SEC EDGAR Form 4 XML is parsed with regex rather than a DOM parser. This avoids a heavy dependency and handles the malformed XML that EDGAR occasionally produces. The nested `<value>` tag pattern is handled explicitly.

**GCS as an immutable raw layer** — Raw Parquet files are never overwritten. New pipeline runs append new quarter partitions. This preserves a full audit trail and allows reprocessing any quarter independently.

---

## Live Dashboard

[sec-insider-report.streamlit.app](https://sec-insider-report.streamlit.app)

---

*Data sourced from SEC EDGAR public filings. Not financial advice.*
