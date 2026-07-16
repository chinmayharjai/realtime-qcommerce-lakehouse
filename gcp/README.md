# GCP path — Dataproc + BigQuery

The multi-cloud proof: the **same batch job** that runs on Databricks
(`batch/nightly_backfill.py`) and the **same Scala jar** tested in CI
(`batch/scala/…/NightlyBackfill.scala`) run here on **GCP Dataproc**, unchanged, and
the gold summaries land in **BigQuery**. Portable, idempotent behaviour across
engines and clouds — that's the claim, and this is where it's exercised.

```
gcp/
├── terraform/          GCS bucket + BigQuery dataset/tables + least-privilege SA
├── dataproc_submit.sh  ephemeral-cluster submit for both PySpark and the Scala jar
└── bigquery_load.py    gold Parquet -> partitioned BigQuery tables
```

> **What is verified vs not.** `terraform validate` passes and `bigquery_load.py`'s
> config logic is unit-tested (both in CI). **Nothing has been applied to a real GCP
> project** — no cluster created, no table loaded, no cost incurred. The steps below
> are the runbook for doing it against a trial account, not a record of a run.

## Why ephemeral clusters and aggressive expiry

Everything here is shaped by one fact: **a free trial gets drained by things left
running.** So:

- **`dataproc_submit.sh` creates a cluster, submits, and deletes it** — via an
  `EXIT` trap *and* a `--max-idle 10m` safety net, two independent guards, because a
  leaked cluster is the single most expensive mistake on this platform. A standing
  cluster bills per second whether or not it's working; a nightly batch needs a
  cluster for minutes a day.
- **BigQuery tables expire after 7 days** outside prod, and the **GCS gold bucket
  expires after 30** — the gold data lives authoritatively in Delta on the primary
  cloud, so the GCP copy is disposable.
- **`force_destroy` on buckets outside prod**, so `terraform destroy` actually works
  during teardown.

## Setup (trial account)

```bash
# 1. A trial project with billing linked (the trial credit covers this).
gcloud projects create qcommerce-trial-XXXX
gcloud config set project qcommerce-trial-XXXX
gcloud services enable dataproc.googleapis.com bigquery.googleapis.com storage.googleapis.com

# 2. Provision the minimal footprint.
cd gcp/terraform
terraform init
terraform apply -var="project_id=qcommerce-trial-XXXX"

# 3. Export the outputs the submit script needs.
eval "$(terraform output -json dataproc_submit_env | \
  python -c 'import json,sys; [print(f"export {k}={v}") for k,v in json.load(sys.stdin).items()]')"

# 4. Stage the job artifacts to GCS.
gsutil cp ../../batch/nightly_backfill.py "gs://${STAGING_BUCKET}/jobs/"
cd ../../batch/scala && sbt package
gsutil cp target/scala-2.12/nightly-backfill_2.12-1.0.0.jar "gs://${STAGING_BUCKET}/jars/"

# 5. Run the backfill — both engines, same result.
cd ../../gcp
./dataproc_submit.sh 2026-06-20 pyspark
./dataproc_submit.sh 2026-06-20 scala
```

Each run creates its cluster, submits, loads BigQuery, and deletes the cluster.

## Teardown — do this, it is the point

The trial credit is finite and the fastest way to burn it is to forget a resource.
When done:

```bash
cd gcp/terraform
terraform destroy -var="project_id=qcommerce-trial-XXXX"

# Confirm nothing survives that bills:
gcloud dataproc clusters list --region "$GCP_REGION"   # must be empty
gsutil ls                                              # buckets gone
bq ls                                                  # dataset gone
```

If a cluster somehow survived (a hard kill before the trap fired), the `--max-idle
10m` on the cluster deletes it automatically — but verify anyway. **A leaked
Dataproc cluster is the one thing on this platform that turns a $0 trial into a
real bill.**

## The idempotency carries across clouds

The reason the same job runs on both clouds without divergence is that the
idempotency mechanism is the same on both:

- On Delta (Databricks/Dataproc): dynamic partition overwrite scoped to `event_date`.
- On BigQuery: `WRITE_TRUNCATE` scoped to a `$YYYYMMDD` partition decorator
  (`bigquery_load.py`, `--replace-partition`).

Both mean a reloaded day **overwrites** rather than appends, so rerunning the
backfill for a date — on either cloud — produces the same result and does not double
the dashboard's counts. `test_bigquery_load.py` pins the BigQuery half (truncate not
append, correct partition decorator); the chaos-style idempotency of the Delta half
is the same property the batch job's tests assert.
