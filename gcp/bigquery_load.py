"""Load gold summaries from GCS into BigQuery, into partitioned tables.

The last hop of the multi-cloud path: the gold Parquet the Dataproc job wrote to
GCS is loaded into BigQuery, where the ops dashboard queries it. BigQuery is the
serving layer here for the same reason Postgres was in Project 2 — it answers
open-ended analytical questions ("orders by zone yesterday") that a lakehouse
scan would be slow at — but at GCP scale and with the partitioning that keeps the
bill down.

The load is a pure function of its arguments plus the client, so the load-job
config (partitioning, write disposition, schema handling) is unit-testable against
a mocked client without a real BigQuery.
"""

from __future__ import annotations

import argparse


def build_load_config(table: str, partition_field: str, clustering: list[str],
                      replace_partition: str | None):
    """Construct the BigQuery LoadJobConfig. Separated so the config decisions are
    testable without a live BigQuery.

    The decisions that matter:
      - Parquet source: schema comes from the file, so a schema drift in the gold
        job surfaces as a load error rather than a silent column mismatch.
      - Partitioned by the query's time column, so BigQuery prunes partitions and
        bills only the bytes a dated query actually scans. An unpartitioned table
        means every query pays for all history — the fastest way to a large BigQuery
        bill.
      - Clustered by the dashboard's filter columns, for block-level skipping within
        a partition.
    """
    from google.cloud import bigquery

    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        ),
        clustering_fields=clustering,
    )

    if replace_partition:
        # Replace exactly one day's partition, not the whole table. This is the
        # BigQuery equivalent of the Delta dynamic partition overwrite the batch job
        # uses — a reloaded day overwrites rather than appends, so the load is
        # idempotent and a rerun does not double the dashboard's counts. Expressed
        # via WRITE_TRUNCATE scoped to a partition decorator ($YYYYMMDD) on the
        # destination.
        config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    return config


def partition_decorator(table: str, replace_partition: str | None) -> str:
    """Append a $YYYYMMDD partition decorator when replacing one day.

    'orders_by_zone_5min' + '2026-06-20' -> 'orders_by_zone_5min$20260620'. This is
    what scopes WRITE_TRUNCATE to a single partition instead of truncating the whole
    table — get it wrong and an idempotent reload becomes a full-table wipe.
    """
    if not replace_partition:
        return table
    return f"{table}${replace_partition.replace('-', '')}"


def load(project: str, dataset: str, source: str, table: str,
         partition_field: str = "window_start", clustering: list[str] | None = None,
         replace_partition: str | None = None) -> int:
    """Run the load. Returns the number of rows loaded."""
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    clustering = clustering or ["zone", "city"]

    destination = f"{project}.{dataset}.{partition_decorator(table, replace_partition)}"
    config = build_load_config(table, partition_field, clustering, replace_partition)

    job = client.load_table_from_uri(source, destination, job_config=config)
    result = job.result()  # blocks until done; raises on a schema/format error

    print(f"loaded {result.output_rows:,} rows into {destination}")
    return result.output_rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--project", required=True)
    ap.add_argument("--dataset", default="qcommerce_gold")
    ap.add_argument("--source", required=True, help="gs:// URI or wildcard")
    ap.add_argument("--table", required=True)
    ap.add_argument("--partition-field", default="window_start")
    ap.add_argument("--clustering", nargs="*", default=["zone", "city"])
    ap.add_argument("--replace-partition", default=None,
                    help="YYYY-MM-DD; truncate+load just this day's partition (idempotent reload)")
    args = ap.parse_args()

    load(args.project, args.dataset, args.source, args.table,
         args.partition_field, args.clustering, args.replace_partition)


if __name__ == "__main__":
    main()
