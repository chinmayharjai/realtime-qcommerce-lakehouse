variable "project_id" {
  description = "GCP project ID. Also the bucket-name prefix (bucket names are globally unique)."
  type        = string
}

variable "region" {
  description = "GCP region. Keep the bucket, dataset and Dataproc cluster in the same region to avoid cross-region egress charges."
  type        = string
  default     = "asia-south1" # Mumbai — matches the quick-commerce scenario's geography
}

variable "environment" {
  description = "dev or prod. Drives force_destroy, table expiry and deletion protection."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod."
  }
}

variable "gold_retention_days" {
  description = "Days to keep gold exports in GCS. A copy of Delta data, so it need not be permanent."
  type        = number
  default     = 30
}
