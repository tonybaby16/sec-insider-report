"""
spark/ingest_sec_form4.py
─────────────────────────────────────────────────────────────────────
Phase 2: SEC Form 4 Ingestion Pipeline

Flow:
  1. Discover available quarterly bulk ZIPs from SEC EDGAR full-index
  2. Download each ZIP (respecting SEC rate limits)
  3. Parse Form 4 index entries from the quarterly idx files
  4. Download and parse individual Form 4 XML filings
  5. Enforce schema and convert to Parquet
  6. Write partitioned Parquet to GCS raw landing zone

SEC EDGAR Bulk Data: https://www.sec.gov/Archives/edgar/full-index/
Rate limit:         10 requests/second max (enforced via sleep)
User-Agent:         Required by SEC — set via env var SEC_USER_AGENT
─────────────────────────────────────────────────────────────────────
"""

import os
import sys
import time
import logging
import zipfile
import io
import re
import argparse
from datetime import datetime, date
from typing import Iterator

import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType,
    LongType,
    TimestampType,
)
from google.cloud import storage

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sec_form4_ingestion")

# ── Constants ─────────────────────────────────────────────────────────
EDGAR_BASE_URL = "https://www.sec.gov/Archives/edgar/full-index"
REQUEST_DELAY = 0.5  # 150ms between requests = ~6 req/s (well under 10/s limit)
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds, doubles on each retry

# ── Form 4 Raw Schema ─────────────────────────────────────────────────
FORM4_SCHEMA = StructType(
    [
        StructField("accession_number", StringType(), nullable=False),
        StructField("cik", StringType(), nullable=False),
        StructField("company_name", StringType(), nullable=True),
        StructField("form_type", StringType(), nullable=True),
        StructField("filing_date", DateType(), nullable=True),
        StructField("period_of_report", DateType(), nullable=True),
        StructField("issuer_cik", StringType(), nullable=True),
        StructField("issuer_name", StringType(), nullable=True),
        StructField("issuer_ticker", StringType(), nullable=True),
        StructField("reporting_owner_name", StringType(), nullable=True),
        StructField("is_director", StringType(), nullable=True),
        StructField("is_officer", StringType(), nullable=True),
        StructField("is_ten_pct_owner", StringType(), nullable=True),
        StructField("officer_title", StringType(), nullable=True),
        StructField("transaction_date", DateType(), nullable=True),
        StructField("transaction_code", StringType(), nullable=True),
        StructField("transaction_shares", DoubleType(), nullable=True),
        StructField("price_per_share", DoubleType(), nullable=True),
        StructField("shares_owned_after", DoubleType(), nullable=True),
        StructField("direct_or_indirect", StringType(), nullable=True),
        StructField("security_title", StringType(), nullable=True),
        StructField("quarter", StringType(), nullable=True),  # e.g. 2024Q1
        StructField("ingested_at", TimestampType(), nullable=True),
    ]
)


# ── HTTP Helpers ──────────────────────────────────────────────────────


def get_user_agent() -> str:
    """SEC requires a descriptive User-Agent with contact email."""
    agent = os.environ.get("SEC_USER_AGENT")
    if not agent:
        raise EnvironmentError(
            "SEC_USER_AGENT env var is required. "
            "Format: 'Your Name yourname@email.com'"
        )
    return agent


def fetch_url(url: str, retries: int = MAX_RETRIES) -> bytes:
    """Fetch a URL with retry logic and rate limiting."""
    headers = {"User-Agent": get_user_agent()}
    for attempt in range(retries):
        try:
            time.sleep(REQUEST_DELAY)
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            wait = RETRY_BACKOFF**attempt
            log.warning(
                f"Attempt {attempt + 1}/{retries} failed for {url}: {e}. Retrying in {wait}s..."
            )
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts")


# ── Quarter Discovery ─────────────────────────────────────────────────


def get_quarters_to_process(years: list[int]) -> list[tuple[int, int]]:
    """Return list of (year, quarter) tuples to process."""
    quarters = []
    today = date.today()
    for year in years:
        for q in range(1, 5):
            # Don't process future quarters
            quarter_end_month = q * 3
            quarter_end = date(year, quarter_end_month, 1)
            if quarter_end <= today:
                quarters.append((year, q))
    return quarters


# ── Index Parsing ─────────────────────────────────────────────────────


def fetch_form4_index(year: int, quarter: int) -> list[dict]:
    """
    Download and parse the EDGAR full-index company.idx for a quarter.
    Returns list of Form 4 filing metadata dicts.
    """
    url = f"{EDGAR_BASE_URL}/{year}/QTR{quarter}/company.idx"
    log.info(f"Fetching index: {url}")

    content = fetch_url(url)
    lines = content.decode("utf-8", errors="replace").splitlines()

    # Skip header lines (first 10 lines are headers/separators)
    filings = []
    for line in lines[10:]:
        if len(line) < 40:
            continue
        form_type = line[62:74].strip()
        if form_type not in ("4", "4/A"):
            continue

        cik = line[74:86].strip()
        filing_date = line[86:98].strip()
        company_name = line[0:62].strip()

        # Find filename by locating 'edgar/' — avoids fixed-width offset issues
        edgar_pos = line.find("edgar/")
        if edgar_pos == -1:
            continue
        filename = line[edgar_pos:].strip()

        # Build accession number from filename
        parts = filename.replace(".txt", "").split("/")
        accession_num = parts[-1] if parts else filename

        filings.append(
            {
                "cik": cik,
                "company_name": company_name,
                "form_type": form_type,
                "filing_date": filing_date,
                "filename": filename,
                "accession_number": accession_num,
                "quarter": f"{year}Q{quarter}",
            }
        )

    log.info(f"Found {len(filings)} Form 4 filings for {year} Q{quarter}")
    return filings


# ── XML Parsing ───────────────────────────────────────────────────────


def extract_xml_value(xml: str, tag: str) -> str | None:
    """Simple regex-based XML value extractor — avoids heavy XML parser overhead."""
    pattern = rf"<{tag}[^>]*>([^<]*)</{tag}>"
    match = re.search(pattern, xml, re.IGNORECASE)
    return match.group(1).strip() if match else None


def safe_float(value: str | None) -> float | None:
    try:
        return float(value) if value else None
    except (ValueError, TypeError):
        return None


def safe_date(value: str | None) -> date | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    return None


def parse_form4_xml(xml_content: str, filing_meta: dict) -> list[dict]:
    """
    Parse a Form 4 XML filing into a list of transaction records.
    One record per non-derivative transaction row.
    """
    records = []
    ingested_at = datetime.utcnow()

    # Issuer info
    issuer_cik = extract_xml_value(xml_content, "issuerCik")
    issuer_name = extract_xml_value(xml_content, "issuerName")
    issuer_ticker = extract_xml_value(xml_content, "issuerTradingSymbol")

    # Reporting owner
    owner_name = extract_xml_value(xml_content, "rptOwnerName")
    is_director = extract_xml_value(xml_content, "isDirector")
    is_officer = extract_xml_value(xml_content, "isOfficer")
    is_10pct = extract_xml_value(xml_content, "isTenPercentOwner")
    officer_title = extract_xml_value(xml_content, "officerTitle")
    period = safe_date(extract_xml_value(xml_content, "periodOfReport"))
    filing_date = safe_date(filing_meta.get("filing_date"))

    # Non-derivative transactions
    tx_pattern = re.compile(
        r"<nonDerivativeTransaction>(.*?)</nonDerivativeTransaction>",
        re.DOTALL | re.IGNORECASE,
    )
    for tx_match in tx_pattern.finditer(xml_content):
        tx_xml = tx_match.group(1)

        security_title = extract_xml_value(tx_xml, "securityTitle")
        tx_date = safe_date(extract_xml_value(tx_xml, "transactionDate"))
        tx_code = extract_xml_value(tx_xml, "transactionCode")
        tx_shares = safe_float(extract_xml_value(tx_xml, "transactionShares"))
        price_per_share = safe_float(
            extract_xml_value(tx_xml, "transactionPricePerShare")
        )
        shares_after = safe_float(
            extract_xml_value(tx_xml, "sharesOwnedFollowingTransaction")
        )
        direct_indirect = extract_xml_value(tx_xml, "directOrIndirectOwnership")

        records.append(
            {
                "accession_number": filing_meta["accession_number"],
                "cik": filing_meta["cik"],
                "company_name": filing_meta["company_name"],
                "form_type": filing_meta["form_type"],
                "filing_date": filing_date,
                "period_of_report": period,
                "issuer_cik": issuer_cik,
                "issuer_name": issuer_name,
                "issuer_ticker": issuer_ticker,
                "reporting_owner_name": owner_name,
                "is_director": is_director,
                "is_officer": is_officer,
                "is_ten_pct_owner": is_10pct,
                "officer_title": officer_title,
                "transaction_date": tx_date,
                "transaction_code": tx_code,
                "transaction_shares": tx_shares,
                "price_per_share": price_per_share,
                "shares_owned_after": shares_after,
                "direct_or_indirect": direct_indirect,
                "security_title": security_title,
                "quarter": filing_meta["quarter"],
                "ingested_at": ingested_at,
            }
        )

    # If no non-derivative transactions found, still record the filing header
    if not records:
        records.append(
            {
                "accession_number": filing_meta["accession_number"],
                "cik": filing_meta["cik"],
                "company_name": filing_meta["company_name"],
                "form_type": filing_meta["form_type"],
                "filing_date": filing_date,
                "period_of_report": period,
                "issuer_cik": issuer_cik,
                "issuer_name": issuer_name,
                "issuer_ticker": issuer_ticker,
                "reporting_owner_name": owner_name,
                "is_director": is_director,
                "is_officer": is_officer,
                "is_ten_pct_owner": is_10pct,
                "officer_title": officer_title,
                "transaction_date": None,
                "transaction_code": None,
                "transaction_shares": None,
                "price_per_share": None,
                "shares_owned_after": None,
                "direct_or_indirect": None,
                "security_title": None,
                "quarter": filing_meta["quarter"],
                "ingested_at": ingested_at,
            }
        )

    return records


# ── Filing Fetcher ────────────────────────────────────────────────────


def fetch_and_parse_filing(filing: dict) -> list[dict]:
    """Download a single Form 4 filing XML and parse it."""
    url = f"https://www.sec.gov/Archives/{filing['filename']}"
    try:
        content = fetch_url(url)
        xml_str = content.decode("utf-8", errors="replace")
        return parse_form4_xml(xml_str, filing)
    except Exception as e:
        log.warning(f"Failed to parse filing {filing['accession_number']}: {e}")
        return []


# ── Spark Processing ──────────────────────────────────────────────────


def process_quarter_with_spark(
    spark: SparkSession,
    filings: list[dict],
    quarter_label: str,
    max_filings: int | None = None,
) -> "pyspark.sql.DataFrame":
    """
    Use Spark to process a quarter's filings in parallel.
    Returns a DataFrame of transaction records.
    """
    if max_filings:
        filings = filings[:max_filings]
        log.info(f"Limiting to {max_filings} filings for {quarter_label}")

    log.info(f"Processing {len(filings)} filings for {quarter_label} via Spark...")

    # Distribute filings across Spark workers
    filings_rdd = spark.sparkContext.parallelize(filings, numSlices=1)

    # Each worker fetches and parses its subset of filings
    records_rdd = filings_rdd.flatMap(fetch_and_parse_filing)

    if records_rdd.isEmpty():
        log.warning(f"No records parsed for {quarter_label}")
        return spark.createDataFrame([], FORM4_SCHEMA)

    # Convert to DataFrame with enforced schema
    records_df = spark.createDataFrame(records_rdd, schema=FORM4_SCHEMA)

    row_count = records_df.count()
    log.info(f"Parsed {row_count:,} transaction records for {quarter_label}")

    return records_df


# ── GCS Writer ────────────────────────────────────────────────────────


import tempfile
import shutil
from google.cloud import storage as gcs


def write_to_gcs(df, bucket_name: str, quarter_label: str) -> str:
    """
    Write DataFrame as Parquet locally then upload to GCS.
    Avoids Hadoop GCS connector Guava version conflicts.
    """
    gcs_prefix = f"form4/quarter={quarter_label}"
    local_dir = f"/tmp/form4_quarter={quarter_label}"

    # Write Parquet to local disk first
    log.info(f"Writing Parquet locally to {local_dir}...")
    df.write.mode("overwrite").parquet(local_dir)

    # Upload each file to GCS
    log.info(f"Uploading to gs://{bucket_name}/{gcs_prefix}/...")
    client = gcs.Client()
    bucket = client.bucket(bucket_name)

    uploaded = 0
    for root, dirs, files in os.walk(local_dir):
        for filename in files:
            if not filename.endswith(".parquet"):
                continue
            local_path = os.path.join(root, filename)
            blob_name = f"{gcs_prefix}/{filename}"
            bucket.blob(blob_name).upload_from_filename(local_path)
            uploaded += 1

    # Clean up local temp files
    shutil.rmtree(local_dir, ignore_errors=True)

    gcs_path = f"gs://{bucket_name}/{gcs_prefix}"
    log.info(f"✅ Uploaded {uploaded} Parquet file(s) to {gcs_path}")
    return gcs_path


# ── Smoke Test ────────────────────────────────────────────────────────


def verify_gcs_output(bucket_name: str, quarter_label: str) -> bool:
    """Verify that Parquet files were written to GCS successfully."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = f"form4/quarter={quarter_label}/"

    blobs = list(bucket.list_blobs(prefix=prefix))
    parquet_files = [b for b in blobs if b.name.endswith(".parquet")]

    if not parquet_files:
        log.error(f"❌ No Parquet files found at gs://{bucket_name}/{prefix}")
        return False

    total_size = sum(b.size for b in parquet_files)
    log.info(
        f"✅ Smoke test passed: {len(parquet_files)} Parquet file(s) "
        f"at gs://{bucket_name}/{prefix} "
        f"({total_size / 1024 / 1024:.2f} MB)"
    )
    return True


# ── Main Entry Point ──────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="SEC Form 4 Ingestion Pipeline")
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument(
        "--years", required=True, help="Comma-separated years e.g. 2023,2024"
    )
    parser.add_argument(
        "--quarters", default="all", help="Comma-separated quarters e.g. 1,2 or 'all'"
    )
    parser.add_argument(
        "--max-filings", type=int, help="Limit filings per quarter (for testing)"
    )
    parser.add_argument(
        "--smoke-test", action="store_true", help="Verify GCS output after write"
    )
    args = parser.parse_args()

    years = [int(y.strip()) for y in args.years.split(",")]
    quarters_to_run = get_quarters_to_process(years)

    if args.quarters != "all":
        allowed_qs = [int(q.strip()) for q in args.quarters.split(",")]
        quarters_to_run = [(y, q) for y, q in quarters_to_run if q in allowed_qs]

    log.info(
        f"Starting SEC Form 4 ingestion for {len(quarters_to_run)} quarter(s): {quarters_to_run}"
    )

    # ── Init Spark ──
    spark = (
        SparkSession.builder.appName("sec_form4_ingestion")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    failed_quarters = []

    for year, quarter in quarters_to_run:
        quarter_label = f"{year}Q{quarter}"
        try:
            log.info(f"─── Processing {quarter_label} ───")

            # Step 1: Fetch filing index
            filings = fetch_form4_index(year, quarter)
            if not filings:
                log.warning(f"No Form 4 filings found for {quarter_label}, skipping")
                continue

            # Step 2: Process with Spark
            df = process_quarter_with_spark(
                spark, filings, quarter_label, max_filings=args.max_filings
            )

            # Step 3: Write to GCS
            write_to_gcs(df, args.bucket, quarter_label)

            # Step 4: Smoke test
            if args.smoke_test:
                if not verify_gcs_output(args.bucket, quarter_label):
                    failed_quarters.append(quarter_label)

        except Exception as e:
            log.error(f"Failed processing {quarter_label}: {e}", exc_info=True)
            failed_quarters.append(quarter_label)

    spark.stop()

    if failed_quarters:
        log.error(f"❌ Ingestion completed with failures: {failed_quarters}")
        sys.exit(1)

    log.info("✅ Ingestion complete — all quarters processed successfully")


if __name__ == "__main__":
    main()
