#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# .devcontainer/post-create.sh
# Runs once after the Codespace container is created.
# ─────────────────────────────────────────────────────────────────────
set -euo pipefail

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  SEC Pipeline — Codespace Setup"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── Python dependencies ──
echo "▶ Installing Python dependencies..."
pip install --upgrade pip --quiet
pip install \
  pyspark==3.5.1 \
  google-cloud-bigquery==3.20.1 \
  google-cloud-storage==2.16.0 \
  great-expectations==0.18.15 \
  dbt-bigquery==1.7.7 \
  apache-airflow==2.9.1 \
  streamlit==1.35.0 \
  pandas==2.2.2 \
  pyarrow==16.1.0 \
  requests==2.32.3 \
  python-dotenv==1.0.1 \
  black==24.4.2 \
  ruff==0.4.4 \
  --quiet

echo "✅ Python dependencies installed"

# ── Make bootstrap script executable ──
chmod +x terraform/bootstrap.sh

# ── Verify tool versions ──
echo ""
echo "▶ Tool versions:"
echo "  Python:    $(python --version)"
echo "  Terraform: $(terraform version -json | python -c 'import sys,json; print(json.load(sys.stdin)[\"terraform_version\"])')"
echo "  gcloud:    $(gcloud version 2>/dev/null | head -1)"
echo "  dbt:       $(dbt --version 2>/dev/null | grep 'installed' | awk '{print $NF}')"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✅ Codespace ready! Start with Phase 1:"
echo "     1. Run: bash terraform/bootstrap.sh"
echo "     2. Fill in: terraform/terraform.tfvars"
echo "     3. Run: cd terraform && terraform init ..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
