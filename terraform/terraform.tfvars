# ─────────────────────────────────────────────────────────────
# terraform.tfvars — fill in your values before running apply
# DO NOT commit this file to GitHub (it is in .gitignore)
# ─────────────────────────────────────────────────────────────

project_id        = "sec-insider-report"
region            = "us-central1"
bq_location       = "US"
state_bucket_name = "tfstate-sec-pipeline-sec-insider-report"
environment       = "dev"

# Optional — leave empty string if you don't have Slack yet
slack_webhook_url = ""
