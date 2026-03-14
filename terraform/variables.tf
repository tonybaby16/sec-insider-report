variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "state_bucket_name" {
  description = "Name of the GCS bucket used for Terraform state (must be pre-created manually)"
  type        = string
}

variable "environment" {
  description = "Deployment environment label"
  type        = string
  default     = "dev"
}

variable "slack_webhook_url" {
  description = "Slack webhook URL for pipeline failure alerts"
  type        = string
  sensitive   = true
  default     = ""
}
