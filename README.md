# SEC Insider Trading Pipeline

> A full-stack data engineering portfolio project — end-to-end pipeline from SEC EDGAR raw data to interactive dashboard.

[![Pipeline Status](https://github.com/tonybaby16/sec-insider-pipeline/actions/workflows/pipeline.yml/badge.svg)](https://github.com/tonybaby16/sec-insider-pipeline/actions)

---

## Architecture Overview

```
SEC EDGAR (Form 4)
        │
        ▼
  Apache Spark ──► Google Cloud Storage (Raw Parquet)
                          │
                          ▼
                   BigQuery RAW Layer
                          │
                          ▼
                  Great Expectations (Quality Gate)
                          │
                    Pass ─┴─ Fail → Alert
                          │
                          ▼
                      dbt Core
                  (staging → marts)
                          │
                          ▼
                 BigQuery MART Layer
                          │
                          ▼
                     Streamlit App
```

**Orchestration:**
- **Production:** GitHub Actions (scheduled monthly, free tier)
- **Development:** Apache Airflow (local Docker, for skill showcase)

**Infrastructure:** All GCP resources provisioned via Terraform.

---

## Tech Stack

| Tool | Layer | Purpose |
|---|---|---|
| Terraform | Infrastructure | GCP provisioning as code |
| Docker + Compose | Containerization | Local dev environment |
| Apache Spark | Ingestion | Distributed SEC data extraction |
| Google Cloud Storage | Storage | Raw Parquet landing zone |
| BigQuery | Warehouse | 4-layer data architecture |
| Great Expectations | Quality | Validation gates before transforms |
| dbt Core | Transformation | SQL models + tests + lineage |
| Apache Airflow | Orchestration (Dev) | Local DAG dev & testing |
| GitHub Actions | Orchestration (Prod) | Production scheduling |
| Streamlit | Serving | Interactive dashboard |

---

## Project Structure

```
sec-insider-pipeline/
├── .devcontainer/          # GitHub Codespaces configuration
│   ├── devcontainer.json
│   └── post-create.sh
├── .github/
│   └── workflows/
│       └── pipeline.yml    # Production GitHub Actions workflow
├── terraform/              # Phase 1 — GCP infrastructure
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── providers.tf
│   ├── bootstrap.sh        # Run once to create state bucket
│   └── terraform.tfvars.example
├── spark/                  # Phase 2 — PySpark ingestion
├── dbt/                    # Phase 3 — Transformation models
├── great_expectations/     # Phase 4 — Data quality suites
├── airflow/                # Phase 5 (Dev) — Local DAGs
├── streamlit/              # Phase 5 — Dashboard app
├── requirements.txt
└── README.md
```

---

## Getting Started

### Option A: GitHub Codespaces (Recommended)

1. Click **Code → Codespaces → Create codespace on main**
2. Wait ~3 minutes for the environment to build
3. Follow Phase 1 setup below

### Option B: Local Development

```bash
git clone https://github.com/YOUR_USERNAME/sec-insider-pipeline.git
cd sec-insider-pipeline
pip install -r requirements.txt
```

---

## Phase 1: Infrastructure Setup

### Prerequisites
- GCP project created with billing enabled
- `gcloud` CLI authenticated

### Steps

**Step 1 — Create Terraform state bucket (run once):**
```bash
# Edit PROJECT_ID in the script first
bash terraform/bootstrap.sh
```

**Step 2 — Configure variables:**
```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Edit terraform.tfvars with your project_id and state_bucket_name
```

**Step 3 — Initialize Terraform:**
```bash
cd terraform
terraform init -backend-config="bucket=YOUR_STATE_BUCKET_NAME"
```

**Step 4 — Preview and apply:**
```bash
terraform plan
terraform apply
```

**Step 5 — Export SA key for GitHub Actions:**
```bash
# Get the key and add it as GCP_SA_KEY secret in GitHub
terraform output -raw pipeline_sa_key_base64
```

### GitHub Secrets Required

Add these secrets to your GitHub repo (Settings → Secrets → Actions):

| Secret | Value |
|---|---|
| `GCP_PROJECT_ID` | Your GCP project ID |
| `GCP_SA_KEY` | Output of `terraform output -raw pipeline_sa_key_base64` |
| `GCS_RAW_BUCKET` | Output of `terraform output raw_bucket_name` |
| `SLACK_WEBHOOK_URL` | Your Slack webhook (optional) |

---

## Phase Roadmap

| Phase | Status | Description |
|---|---|---|
| **Phase 1** | 🔨 In Progress | Terraform — GCP infrastructure provisioning |
| **Phase 2** | ⏳ Planned | PySpark — SEC Form 4 ingestion to GCS |
| **Phase 3** | ⏳ Planned | BigQuery load + dbt models |
| **Phase 4** | ⏳ Planned | Great Expectations quality gates |
| **Phase 5** | ⏳ Planned | Streamlit dashboard (live on Streamlit Cloud) |
| **Phase 6** | ⏳ Planned | Polish — README diagram, Airflow screenshots, docs |

---

## Data Source

**SEC EDGAR Form 4** — Insider Trading Disclosures
- Source: [https://www.sec.gov/cgi-bin/browse-edgar](https://www.sec.gov/cgi-bin/browse-edgar)
- Format: XML bulk downloads (quarterly ZIPs)
- Update frequency: Continuous (processed monthly)
- Key fields: CIK, insider name, company, transaction date, shares, price, transaction type

---

## Key Business Questions Answered

1. Which insiders have sold the most shares in the last 90 days?
2. Which companies have the highest ratio of insider sells to buys?
3. Are there clusters of insider selling activity before earnings dates?
4. What is the net insider sentiment by sector?
5. Which executives are the most active traders?

---

*Built as a data engineering portfolio project. Not financial advice.*
