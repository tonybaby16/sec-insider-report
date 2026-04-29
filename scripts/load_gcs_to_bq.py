"""
scripts/load_gcs_to_bq.py
─────────────────────────────────────────────────────────────────────
Phase 3: Load GCS Parquet files into BigQuery RAW layer.

Uses the BigQuery native Load Job (not Spark connector) — fast,
free within limits, and handles schema inference from Parquet.

Flow:
  1. List quarters available in GCS
  2. For each quarter, trigger a BigQuery Load Job
  3. Load into sec_raw.form4_transactions (append or overwrite)
  4. Log row counts after load
─────────────────────────────────────────────────────────────────────
"""

import os
import sys
import logging
import argparse
from datetime import datetime

from google.cloud import bigquery, storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("load_gcs_to_bq")

# ── Constants ─────────────────────────────────────────────────────────
BQ_RAW_DATASET = "sec_raw"
BQ_RAW_TABLE = "form4_transactions"
GCS_PREFIX = "form4"


def list_available_quarters(bucket_name: str) -> list[str]:
    """List quarter partitions available in GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = client.list_blobs(bucket_name, prefix=f"{GCS_PREFIX}/")

    quarters = set()
    for blob in blobs:
        # Path: form4/quarter=2024Q1/part-*.parquet
        parts = blob.name.split("/")
        for part in parts:
            if part.startswith("quarter="):
                quarters.add(part.replace("quarter=", ""))

    sorted_quarters = sorted(quarters)
    log.info(f"Found {len(sorted_quarters)} quarter(s) in GCS: {sorted_quarters}")
    return sorted_quarters


def load_quarter_to_bq(
    bq_client: bigquery.Client,
    bucket_name: str,
    quarter: str,
    project_id: str,
    write_disposition: str = "WRITE_APPEND",
) -> int:
    """
    Load a single quarter's Parquet files from GCS into BigQuery RAW table.
    Returns the number of rows loaded.
    """
    gcs_uri = f"gs://{bucket_name}/{GCS_PREFIX}/quarter={quarter}/*.parquet"
    table_ref = f"{project_id}.{BQ_RAW_DATASET}.{BQ_RAW_TABLE}"

    log.info(f"Loading {gcs_uri} → {table_ref} ({write_disposition})")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        autodetect=True,  # infer schema from Parquet metadata
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        # Add ingestion metadata as a label
        labels={"quarter": quarter.lower(), "phase": "raw-load"},
    )

    load_job = bq_client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config,
    )

    log.info(f"Load job started: {load_job.job_id}")
    load_job.result()  # wait for completion

    table = bq_client.get_table(table_ref)
    row_count = table.num_rows
    log.info(f"✅ Loaded quarter={quarter} — table now has {row_count:,} rows")
    return row_count


def main():
    parser = argparse.ArgumentParser(description="Load GCS Parquet to BigQuery RAW")
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--quarters",
        default="all",
        help="Comma-separated quarters e.g. 2024Q1,2024Q2 or 'all'",
    )
    parser.add_argument(
        "--overwrite", action="store_true", help="Overwrite table instead of append"
    )
    args = parser.parse_args()

    bq_client = bigquery.Client(project=args.project)

    # Determine which quarters to load
    if args.quarters == "all":
        quarters = list_available_quarters(args.bucket)
    else:
        quarters = [q.strip() for q in args.quarters.split(",")]

    if not quarters:
        log.error("No quarters found to load. Exiting.")
        sys.exit(1)

    write_disposition = "WRITE_TRUNCATE" if args.overwrite else "WRITE_APPEND"

    # For first quarter use WRITE_TRUNCATE to reset table, rest append
    failed = []
    for i, quarter in enumerate(quarters):
        disposition = "WRITE_TRUNCATE" if (i == 0 or args.overwrite) else "WRITE_APPEND"
        try:
            load_quarter_to_bq(
                bq_client, args.bucket, quarter, args.project, disposition
            )
        except Exception as e:
            log.error(f"Failed loading quarter={quarter}: {e}", exc_info=True)
            failed.append(quarter)

    if failed:
        log.error(f"❌ Load completed with failures: {failed}")
        sys.exit(1)

    log.info("✅ All quarters loaded into BigQuery RAW successfully")


if __name__ == "__main__":
    main()
