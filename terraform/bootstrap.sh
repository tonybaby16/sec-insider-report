#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# bootstrap.sh
# Run ONCE before `terraform init` to create the GCS state bucket.
# Terraform cannot manage the bucket it uses for its own state.
# ─────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Edit these two values ──
PROJECT_ID="YOUR_GCP_PROJECT_ID"
REGION="us-central1"
# ───────────────────────────

BUCKET_NAME="tfstate-sec-pipeline-${PROJECT_ID}"

echo "▶ Authenticating with GCP..."
gcloud auth application-default login

echo "▶ Setting project to ${PROJECT_ID}..."
gcloud config set project "${PROJECT_ID}"

echo "▶ Creating Terraform state bucket: ${BUCKET_NAME}..."
gcloud storage buckets create "gs://${BUCKET_NAME}" \
  --project="${PROJECT_ID}" \
  --location="${REGION}" \
  --uniform-bucket-level-access \
  --public-access-prevention

echo "▶ Enabling versioning on state bucket..."
gcloud storage buckets update "gs://${BUCKET_NAME}" --versioning

echo ""
echo "✅ Done! Your state bucket is: ${BUCKET_NAME}"
echo ""
echo "Next steps:"
echo "  1. Copy terraform/terraform.tfvars.example → terraform/terraform.tfvars"
echo "  2. Fill in your project_id and state_bucket_name = \"${BUCKET_NAME}\""
echo "  3. Run: cd terraform && terraform init -backend-config=\"bucket=${BUCKET_NAME}\""
echo "  4. Run: terraform plan"
echo "  5. Run: terraform apply"
