output "gold_bucket" {
  value = google_storage_bucket.lakehouse.name
}

output "dataproc_staging_bucket" {
  value = google_storage_bucket.dataproc_staging.name
}

output "bigquery_dataset" {
  value = google_bigquery_dataset.gold.dataset_id
}

output "job_service_account" {
  description = "Attach this to the Dataproc cluster so the batch job and BigQuery load run as a least-privilege identity."
  value       = google_service_account.dataproc_job.email
}

output "dataproc_submit_env" {
  description = "Values dataproc_submit.sh needs. Export these before running it."
  value = {
    GCP_PROJECT      = var.project_id
    GCP_REGION       = var.region
    GOLD_BUCKET      = google_storage_bucket.lakehouse.name
    STAGING_BUCKET   = google_storage_bucket.dataproc_staging.name
    BQ_DATASET       = google_bigquery_dataset.gold.dataset_id
    JOB_SERVICE_ACCT = google_service_account.dataproc_job.email
  }
}
