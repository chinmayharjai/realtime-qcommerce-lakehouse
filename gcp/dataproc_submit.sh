#!/usr/bin/env bash
#
# Run the nightly backfill on GCP Dataproc — both the PySpark and the Scala jar.
#
# This is the multi-cloud proof: the SAME batch job that runs on Databricks
# (batch/nightly_backfill.py) and the SAME Scala jar tested in CI
# (batch/scala/.../NightlyBackfill.scala) run here on Dataproc, unchanged. The only
# thing that differs is the submit command and the storage URIs (gs:// instead of
# the Delta path). Portable idempotent behaviour across engines is the claim; this
# script is where it is exercised.
#
# Uses an EPHEMERAL cluster per job (create -> submit -> delete), not a long-running
# one. A standing Dataproc cluster bills per second whether or not it is doing
# anything, and a nightly batch needs a cluster for minutes a day. Create-on-demand
# is both cheaper and the pattern that keeps a free trial from being drained by an
# idle cluster left running over a weekend.
#
# Prereqs: `terraform apply` in gcp/terraform, then export its dataproc_submit_env
# outputs. See gcp/README.md for the free-trial setup and — importantly — teardown.
set -euo pipefail

: "${GCP_PROJECT:?export GCP_PROJECT (from terraform output)}"
: "${GCP_REGION:?export GCP_REGION}"
: "${STAGING_BUCKET:?export STAGING_BUCKET}"
: "${GOLD_BUCKET:?export GOLD_BUCKET}"
: "${JOB_SERVICE_ACCT:?export JOB_SERVICE_ACCT}"

PROCESS_DATE="${1:?usage: dataproc_submit.sh YYYY-MM-DD [pyspark|scala]}"
JOB_TYPE="${2:-pyspark}"
CLUSTER="qcommerce-backfill-$(date +%s)"

RAW_PATH="gs://${GOLD_BUCKET}/bronze_orders"
SILVER_PATH="gs://${GOLD_BUCKET}/silver_orders"

# Delta on Dataproc needs the delta-spark package. --properties passes it to the
# cluster's Spark, the same coordinate the local jobs use, so the runtime is
# consistent across clouds rather than "works on Databricks, mystery on Dataproc".
DELTA_PROPS="spark:spark.jars.packages=io.delta:delta-spark_2.12:3.2.1,spark:spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,spark:spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

cleanup() {
  echo ">> deleting ephemeral cluster ${CLUSTER}"
  gcloud dataproc clusters delete "${CLUSTER}" \
    --project "${GCP_PROJECT}" --region "${GCP_REGION}" --quiet || true
}
# trap on EXIT so the cluster is deleted even if the job fails or the script is
# interrupted — a cluster left running after a failed job is the classic way a
# "just testing" run turns into a surprise bill.
trap cleanup EXIT

echo ">> creating ephemeral cluster ${CLUSTER}"
gcloud dataproc clusters create "${CLUSTER}" \
  --project "${GCP_PROJECT}" \
  --region "${GCP_REGION}" \
  --service-account "${JOB_SERVICE_ACCT}" \
  --bucket "${STAGING_BUCKET}" \
  --single-node \
  --master-machine-type n2-standard-2 \
  --image-version 2.2-debian12 \
  --max-idle 10m \
  --labels project=qcommerce,purpose=backfill
  # --single-node: this is a demo-scale backfill, not a production cluster. One node
  # is enough and costs a fraction of a multi-worker cluster.
  # --max-idle 10m: a safety net on top of the EXIT trap. If the trap somehow does
  # not fire (a hard kill), Dataproc auto-deletes the cluster after 10 idle minutes.
  # Two independent guards against a leaked cluster, because a leaked cluster is the
  # single most expensive mistake on this platform.

if [[ "${JOB_TYPE}" == "scala" ]]; then
  # The Scala jar, built by `sbt package` in batch/scala. Same logic, same tests as
  # the PySpark job — this is the branch that proves the Scala implementation runs
  # on a real cluster, not just in sbt test.
  JAR="gs://${STAGING_BUCKET}/jars/nightly-backfill_2.12-1.0.0.jar"
  echo ">> submitting SCALA backfill for ${PROCESS_DATE}"
  gcloud dataproc jobs submit spark \
    --project "${GCP_PROJECT}" --region "${GCP_REGION}" --cluster "${CLUSTER}" \
    --class com.qcommerce.NightlyBackfill \
    --jars "${JAR}" \
    --properties "${DELTA_PROPS}" \
    -- --raw-path "${RAW_PATH}" --silver-path "${SILVER_PATH}" --process-date "${PROCESS_DATE}"
else
  echo ">> submitting PYSPARK backfill for ${PROCESS_DATE}"
  gcloud dataproc jobs submit pyspark \
    "gs://${STAGING_BUCKET}/jobs/nightly_backfill.py" \
    --project "${GCP_PROJECT}" --region "${GCP_REGION}" --cluster "${CLUSTER}" \
    --properties "${DELTA_PROPS}" \
    -- --raw-path "${RAW_PATH}" --silver-path "${SILVER_PATH}" --process-date "${PROCESS_DATE}"
fi

echo ">> backfill submitted; loading gold summaries to BigQuery"
python "$(dirname "$0")/bigquery_load.py" \
  --project "${GCP_PROJECT}" \
  --dataset qcommerce_gold \
  --source "gs://${GOLD_BUCKET}/gold_orders_by_zone_5min/*.parquet" \
  --table orders_by_zone_5min

echo ">> done. cluster will be deleted by the EXIT trap."
