#!/usr/bin/env bash
set -euo pipefail

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  SEC Pipeline — Codespace Setup"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── Remove broken Yarn repo (expired GPG key in base image) ──
rm -f /etc/apt/sources.list.d/yarn.list
rm -f /usr/share/keyrings/yarnkey.gpg

# ── System packages ──
echo "▶ Installing system packages..."
apt-get update -qq
apt-get install -y -qq \
  default-jdk-headless \
  unzip \
  curl \
  > /dev/null 2>&1
echo "✅ System packages installed"

# ── Terraform ──
echo "▶ Installing Terraform 1.7.5..."
TERRAFORM_VERSION="1.7.5"
curl -sLo /tmp/terraform.zip \
  "https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip"
unzip -q /tmp/terraform.zip -d /usr/local/bin/
rm /tmp/terraform.zip
chmod +x /usr/local/bin/terraform
echo "✅ Terraform: $(terraform version | head -1)"

# ── Google Cloud CLI ──
echo "▶ Installing Google Cloud CLI..."
curl -sSL https://sdk.cloud.google.com \
  | bash -s -- --disable-prompts --install-dir=/usr/local/lib > /dev/null 2>&1
ln -sf /usr/local/lib/google-cloud-sdk/bin/gcloud /usr/local/bin/gcloud
ln -sf /usr/local/lib/google-cloud-sdk/bin/gsutil /usr/local/bin/gsutil
ln -sf /usr/local/lib/google-cloud-sdk/bin/bq     /usr/local/bin/bq
echo "✅ gcloud: $(gcloud --version | head -1)"

# ── Python dependencies ──
echo "▶ Installing Python dependencies (~2 min)..."
pip install --upgrade pip --quiet
pip install \
  pyspark==3.5.1 \
  google-cloud-bigquery==3.20.1 \
  google-cloud-storage==2.16.0 \
  google-cloud-secret-manager==2.20.0 \
  great-expectations==0.18.15 \
  dbt-bigquery==1.7.7 \
  apache-airflow==2.9.1 \
  apache-airflow-providers-google==10.18.0 \
  streamlit==1.35.0 \
  pandas==2.2.2 \
  pyarrow==16.1.0 \
  plotly==5.22.0 \
  requests==2.32.3 \
  python-dotenv==1.0.1 \
  black==24.4.2 \
  ruff==0.4.4 \
  pytest==8.2.2 \
  --quiet
echo "✅ Python dependencies installed"

# ── Make scripts executable ──
chmod +x terraform/bootstrap.sh

# ── Summary ──
echo ""
echo "▶ Tool versions:"
echo "  Python:    $(python --version)"
echo "  Terraform: $(terraform version | head -1)"
echo "  gcloud:    $(gcloud --version | head -1)"
echo "  Java:      $(java -version 2>&1 | head -1)"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✅ Codespace ready!"
echo "     1. Run: bash terraform/bootstrap.sh"
echo "     2. Fill in: terraform/terraform.tfvars"
echo "     3. cd terraform && terraform init -backend-config=..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"