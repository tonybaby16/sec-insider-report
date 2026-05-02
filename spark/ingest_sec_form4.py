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
Rate limit:         10 requests/second max (enforced via semaphore)
User-Agent:         Required by SEC — set via env var SEC_USER_AGENT
─────────────────────────────────────────────────────────────────────
"""

import os
import sys
import time
import logging
import re
import shutil
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from threading import Semaphore

import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType,
    TimestampType,
)
from google.cloud import storage as gcs

# ── Logging ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sec_form4_ingestion")

# ── Constants ──────────────────────────────────────────────────────────
EDGAR_BASE_URL = "https://www.sec.gov/Archives/edgar/full-index"
REQUEST_DELAY = 0.1  # 100ms per thread — 5 threads = ~5 req/s aggregate
MAX_RETRIES = 3
RETRY_BACKOFF = 2
MAX_WORKERS = 5  # concurrent HTTP threads

# Shared rate limiter — max 5 concurrent requests at any time
_rate_limiter = Semaphore(MAX_WORKERS)

# ── Schema ─────────────────────────────────────────────────────────────
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
        StructField("quarter", StringType(), nullable=True),
        StructField("ingested_at", TimestampType(), nullable=True),
    ]
)


# ── HTTP Helpers ───────────────────────────────────────────────────────


def get_user_agent() -> str:
    agent = os.environ.get("SEC_USER_AGENT")
    if not agent:
        raise EnvironmentError(
            "SEC_USER_AGENT env var is required. "
            "Format: 'Your Name yourname@email.com'"
        )
    return agent


def fetch_url(url: str, retries: int = MAX_RETRIES) -> bytes:
    """Fetch URL with shared rate limiter and retry logic."""
    headers = {"User-Agent": get_user_agent()}
    for attempt in range(retries):
        try:
            with _rate_limiter:
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


# ── Quarter Discovery ──────────────────────────────────────────────────


def get_quarters_to_process(years: list[int]) -> list[tuple[int, int]]:
    quarters = []
    today = date.today()
    for year in years:
        for q in range(1, 5):
            quarter_end = date(year, q * 3, 1)
            if quarter_end <= today:
                quarters.append((year, q))
    return quarters


# ── Index Parsing ──────────────────────────────────────────────────────


def fetch_form4_index(year: int, quarter: int) -> list[dict]:
    url = f"{EDGAR_BASE_URL}/{year}/QTR{quarter}/company.idx"
    log.info(f"Fetching index: {url}")
    content = fetch_url(url)
    lines = content.decode("utf-8", errors="replace").splitlines()

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

        edgar_pos = line.find("edgar/")
        if edgar_pos == -1:
            continue
        filename = line[edgar_pos:].strip()

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


# ── XML Parsing ────────────────────────────────────────────────────────


def extract_xml_value(xml: str, tag: str) -> str | None:
    """
    Handles both direct and nested <value> SEC EDGAR patterns:
      Direct: <issuerName>Apple Inc.</issuerName>
      Nested: <transactionShares><value>50000</value></transactionShares>
    """
    pattern = rf"<{tag}[^>]*>(.*?)</{tag}>"
    match = re.search(pattern, xml, re.DOTALL | re.IGNORECASE)
    if not match:
        return None

    inner = match.group(1).strip()
    value_match = re.search(r"<value[^>]*>([^<]*)</value>", inner, re.IGNORECASE)
    if value_match:
        return value_match.group(1).strip()

    direct = re.sub(r"<[^>]+>", "", inner).strip()
    return direct if direct else None


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
    records = []
    ingested_at = datetime.utcnow()

    issuer_cik = extract_xml_value(xml_content, "issuerCik")
    issuer_name = extract_xml_value(xml_content, "issuerName")
    issuer_ticker = extract_xml_value(xml_content, "issuerTradingSymbol")
    owner_name = extract_xml_value(xml_content, "rptOwnerName")
    is_director = extract_xml_value(xml_content, "isDirector")
    is_officer = extract_xml_value(xml_content, "isOfficer")
    is_10pct = extract_xml_value(xml_content, "isTenPercentOwner")
    officer_title = extract_xml_value(xml_content, "officerTitle")
    period = safe_date(extract_xml_value(xml_content, "periodOfReport"))
    filing_date = safe_date(filing_meta.get("filing_date"))

    tx_pattern = re.compile(
        r"<nonDerivativeTransaction>(.*?)</nonDerivativeTransaction>",
        re.DOTALL | re.IGNORECASE,
    )
    for tx_match in tx_pattern.finditer(xml_content):
        tx_xml = tx_match.group(1)
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
                "transaction_date": safe_date(
                    extract_xml_value(tx_xml, "transactionDate")
                ),
                "transaction_code": extract_xml_value(tx_xml, "transactionCode"),
                "transaction_shares": safe_float(
                    extract_xml_value(tx_xml, "transactionShares")
                ),
                "price_per_share": safe_float(
                    extract_xml_value(tx_xml, "transactionPricePerShare")
                ),
                "shares_owned_after": safe_float(
                    extract_xml_value(tx_xml, "sharesOwnedFollowingTransaction")
                ),
                "direct_or_indirect": extract_xml_value(
                    tx_xml, "directOrIndirectOwnership"
                ),
                "security_title": extract_xml_value(tx_xml, "securityTitle"),
                "quarter": filing_meta["quarter"],
                "ingested_at": ingested_at,
            }
        )

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


# ── Thread-based Fetcher ───────────────────────────────────────────────


def fetch_and_parse_filing(filing: dict) -> list[dict]:
    """Fetch and parse a single filing — designed to run in a thread pool."""
    url = f"https://www.sec.gov/Archives/{filing['filename']}"
    try:
        content = fetch_url(url)
        xml_str = content.decode("utf-8", errors="replace")
        return parse_form4_xml(xml_str, filing)
    except Exception as e:
        log.warning(f"Failed to parse filing {filing['accession_number']}: {e}")
        return []


def fetch_all_filings(
    filings: list[dict], max_filings: int | None = None
) -> list[dict]:
    """
    Fetch all filings concurrently using a thread pool.
    Replaces Spark RDD for HTTP fetching — I/O bound work suits threads better.
    """
    if max_filings:
        filings = filings[:max_filings]
        log.info(f"Limiting to {max_filings} filings")

    log.info(f"Fetching {len(filings)} filings with {MAX_WORKERS} threads...")
    all_records = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_and_parse_filing, f): f for f in filings}
        for future in as_completed(futures):
            records = future.result()
            all_records.extend(records)
            completed += 1
            if completed % 100 == 0:
                log.info(f"  Progress: {completed}/{len(filings)} filings fetched")

    log.info(
        f"Fetched {len(all_records):,} transaction records from {len(filings)} filings"
    )
    return all_records


# ── Spark Processing ───────────────────────────────────────────────────


def build_spark_dataframe(
    spark: SparkSession, records: list[dict]
) -> "pyspark.sql.DataFrame":
    """
    Convert fetched records to a Spark DataFrame with enforced schema.
    Spark is used here for schema enforcement and Parquet writing only.
    """
    if not records:
        return spark.createDataFrame([], FORM4_SCHEMA)

    df = spark.createDataFrame(records, schema=FORM4_SCHEMA)
    log.info(f"Spark DataFrame created: {df.count():,} rows")
    return df


# ── GCS Writer ─────────────────────────────────────────────────────────


def write_to_gcs(df, bucket_name: str, quarter_label: str) -> str:
    """Write Parquet locally then upload to GCS (avoids Hadoop connector conflicts)."""
    gcs_prefix = f"form4/quarter={quarter_label}"
    local_dir = f"/tmp/form4_quarter={quarter_label}"

    log.info(f"Writing Parquet locally to {local_dir}...")
    df.write.mode("overwrite").parquet(local_dir)

    log.info(f"Uploading to gs://{bucket_name}/{gcs_prefix}/...")
    client = gcs.Client()
    bucket = client.bucket(bucket_name)
    uploaded = 0

    for root, dirs, files in os.walk(local_dir):
        for filename in files:
            if not filename.endswith(".parquet"):
                continue
            local_path = os.path.join(root, filename)
            bucket.blob(f"{gcs_prefix}/{filename}").upload_from_filename(local_path)
            uploaded += 1

    shutil.rmtree(local_dir, ignore_errors=True)
    gcs_path = f"gs://{bucket_name}/{gcs_prefix}"
    log.info(f"✅ Uploaded {uploaded} Parquet file(s) to {gcs_path}")
    return gcs_path


# ── Smoke Test ─────────────────────────────────────────────────────────


def verify_gcs_output(bucket_name: str, quarter_label: str) -> bool:
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = f"form4/quarter={quarter_label}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    parquet = [b for b in blobs if b.name.endswith(".parquet")]

    if not parquet:
        log.error(f"❌ No Parquet files found at gs://{bucket_name}/{prefix}")
        return False

    total_size = sum(b.size for b in parquet)
    log.info(
        f"✅ Smoke test passed: {len(parquet)} file(s) "
        f"at gs://{bucket_name}/{prefix} "
        f"({total_size / 1024 / 1024:.2f} MB)"
    )
    return True


# ── Main ───────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="SEC Form 4 Ingestion Pipeline")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--years", required=True)
    parser.add_argument("--quarters", default="all")
    parser.add_argument("--max-filings", type=int)
    parser.add_argument("--smoke-test", action="store_true")
    args = parser.parse_args()

    years = [int(y.strip()) for y in args.years.split(",")]
    quarters_to_run = get_quarters_to_process(years)

    if args.quarters != "all":
        allowed_qs = [int(q.strip()) for q in args.quarters.split(",")]
        quarters_to_run = [(y, q) for y, q in quarters_to_run if q in allowed_qs]

    log.info(
        f"Starting ingestion for {len(quarters_to_run)} quarter(s): {quarters_to_run}"
    )

    spark = (
        SparkSession.builder.appName("sec_form4_ingestion")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    failed_quarters = []

    for year, quarter in quarters_to_run:
        quarter_label = f"{year}Q{quarter}"
        start_time = time.time()
        try:
            log.info(f"─── Processing {quarter_label} ───")

            # Step 1: Fetch index
            filings = fetch_form4_index(year, quarter)
            if not filings:
                log.warning(f"No filings found for {quarter_label}, skipping")
                continue

            # Step 2: Fetch all filings concurrently via thread pool
            records = fetch_all_filings(filings, max_filings=args.max_filings)

            # Step 3: Build Spark DataFrame for schema enforcement + Parquet write
            df = build_spark_dataframe(spark, records)

            # Step 4: Write to GCS
            write_to_gcs(df, args.bucket, quarter_label)

            elapsed = time.time() - start_time
            log.info(f"✅ {quarter_label} complete in {elapsed:.0f}s")

            # Step 5: Smoke test
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
