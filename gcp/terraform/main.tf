/*
  Minimal GCP footprint for the multi-cloud path: a GCS bucket for the gold
  exports and the Dataproc staging, and a BigQuery dataset + tables for the
  serving copy.

  Deliberately minimal. The lakehouse's centre of gravity is Databricks + Delta on
  the primary cloud; GCP here is the "same job runs on a second cloud" proof, not a
  parallel production stack. So this provisions exactly what the Dataproc batch job
  and the BigQuery load need and nothing else — every resource that costs money when
  idle is either absent or documented with its teardown.

  Everything is parameterized so it can be created and destroyed cheaply during a
  free-trial window (see gcp/README.md for the trial setup/teardown).
*/

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# --- GCS: staging + gold exports -------------------------------------------

resource "google_storage_bucket" "lakehouse" {
  name     = "${var.project_id}-qcommerce-gold"
  location = var.region

  uniform_bucket_level_access = true
  # Uniform access disables per-object ACLs, so access is governed by IAM alone.
  # The GCP equivalent of the S3 BucketOwnerEnforced setting in Project 2 — one
  # permission system to audit, not two.

  force_destroy = var.environment != "prod"
  # Same discipline as the AWS project: a bucket with objects can only be destroyed
  # outside prod, so `terraform destroy` during a trial teardown works while prod
  # cannot delete data by accident.

  lifecycle_rule {
    condition {
      age = var.gold_retention_days
    }
    action {
      type = "Delete"
    }
    # The gold export is a copy of data that lives authoritatively in Delta on the
    # primary cloud, so it does not need to be kept forever here — expiring it keeps
    # the trial bill bounded. This is a copy, not a source of truth.
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
    # Same reasoning as the S3 module: a failed multipart upload leaves parts that
    # bill as storage and never expire on their own.
  }
}

# A separate prefix for Dataproc's own staging/temp files, so they can be lifecycle-
# expired aggressively without touching the gold exports. Dataproc writes a lot of
# short-lived scratch data; keeping it 3 days is plenty.
resource "google_storage_bucket" "dataproc_staging" {
  name                        = "${var.project_id}-qcommerce-dataproc-staging"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true # always disposable — it is scratch

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }
}

# --- BigQuery: the serving dataset -----------------------------------------

resource "google_bigquery_dataset" "gold" {
  dataset_id  = "qcommerce_gold"
  location    = var.region
  description = "Gold summaries loaded from the lakehouse. A serving copy, not the source of truth."

  default_table_expiration_ms = var.environment == "prod" ? null : 604800000
  # 7-day table expiry outside prod, so a forgotten trial dataset does not bill
  # indefinitely. null (never) in prod. BigQuery storage is cheap but not free, and
  # a trial account's whole point is that it gets torn down.

  delete_contents_on_destroy = var.environment != "prod"
}

resource "google_bigquery_table" "orders_by_zone" {
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "orders_by_zone_5min"
  deletion_protection = var.environment == "prod"

  # Partitioned by the window date and clustered by zone — the two dimensions the
  # dashboard filters on. Partitioning is what keeps a "yesterday's orders" query
  # from scanning the whole table (BigQuery bills by bytes scanned, so an
  # unpartitioned table means every query pays for all history), and clustering
  # co-locates a zone's rows so a zone filter reads fewer blocks.
  time_partitioning {
    type  = "DAY"
    field = "window_start"
  }
  clustering = ["zone", "city"]

  schema = jsonencode([
    { name = "window_start", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "window_end", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "zone", type = "STRING", mode = "REQUIRED" },
    { name = "city", type = "STRING", mode = "NULLABLE" },
    { name = "order_count", type = "INTEGER", mode = "NULLABLE" },
    { name = "total_value", type = "FLOAT", mode = "NULLABLE" },
    { name = "avg_order_value", type = "FLOAT", mode = "NULLABLE" },
    { name = "active_stores", type = "INTEGER", mode = "NULLABLE" },
    { name = "distinct_customers", type = "INTEGER", mode = "NULLABLE" },
    { name = "cancelled_count", type = "INTEGER", mode = "NULLABLE" },
    { name = "cancel_rate", type = "FLOAT", mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "stockout_alerts" {
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "stockout_alerts"
  deletion_protection = var.environment == "prod"

  time_partitioning {
    type  = "DAY"
    field = "detected_at"
  }
  clustering = ["store_id", "sku"]

  schema = jsonencode([
    { name = "store_id", type = "STRING", mode = "REQUIRED" },
    { name = "sku", type = "STRING", mode = "REQUIRED" },
    { name = "current_level", type = "INTEGER", mode = "NULLABLE" },
    { name = "units_ordered", type = "INTEGER", mode = "NULLABLE" },
    { name = "demand_per_minute", type = "FLOAT", mode = "NULLABLE" },
    { name = "minutes_to_stockout", type = "FLOAT", mode = "NULLABLE" },
    { name = "horizon_minutes", type = "INTEGER", mode = "NULLABLE" },
    { name = "detected_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

# --- Service account for the batch job -------------------------------------

resource "google_service_account" "dataproc_job" {
  account_id   = "qcommerce-dataproc-job"
  display_name = "Runs the qcommerce batch job on Dataproc and loads BigQuery"
}

# Least-privilege: the job reads/writes its own buckets and loads BigQuery, and
# nothing else. project-level roles are avoided in favour of resource-level bindings
# so this SA cannot touch other datasets or buckets in the project.
resource "google_storage_bucket_iam_member" "job_gold" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc_job.email}"
}

resource "google_storage_bucket_iam_member" "job_staging" {
  bucket = google_storage_bucket.dataproc_staging.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc_job.email}"
}

resource "google_bigquery_dataset_iam_member" "job_bq" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dataproc_job.email}"
  # dataEditor on the dataset, not bigquery.admin on the project. The job loads
  # tables in this one dataset; it has no business creating datasets or reading
  # others.
}
