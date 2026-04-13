"""
spark/tests/test_ingest_sec_form4.py
─────────────────────────────────────
Unit tests for SEC Form 4 ingestion logic.
Tests parsing functions without requiring Spark or network access.
Run: pytest spark/tests/
"""

import pytest
from datetime import date, datetime
from unittest.mock import patch, MagicMock

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingest_sec_form4 import (
    extract_xml_value,
    safe_float,
    safe_date,
    parse_form4_xml,
    get_quarters_to_process,
)


# ── extract_xml_value ─────────────────────────────────────────────────


class TestExtractXmlValue:
    def test_basic_extraction(self):
        xml = "<issuerName>APPLE INC</issuerName>"
        assert extract_xml_value(xml, "issuerName") == "APPLE INC"

    def test_with_attributes(self):
        xml = '<transactionCode format="H">S</transactionCode>'
        assert extract_xml_value(xml, "transactionCode") == "S"

    def test_missing_tag_returns_none(self):
        xml = "<issuerName>APPLE INC</issuerName>"
        assert extract_xml_value(xml, "notATag") is None

    def test_empty_tag_returns_empty_string(self):
        xml = "<officerTitle></officerTitle>"
        assert extract_xml_value(xml, "officerTitle") == ""

    def test_case_insensitive(self):
        xml = "<IssuerName>TESLA INC</IssuerName>"
        assert extract_xml_value(xml, "issuerName") == "TESLA INC"


# ── safe_float ────────────────────────────────────────────────────────


class TestSafeFloat:
    def test_valid_float(self):
        assert safe_float("12345.67") == 12345.67

    def test_valid_integer_string(self):
        assert safe_float("1000") == 1000.0

    def test_none_returns_none(self):
        assert safe_float(None) is None

    def test_empty_string_returns_none(self):
        assert safe_float("") is None

    def test_invalid_string_returns_none(self):
        assert safe_float("not-a-number") is None

    def test_negative_value(self):
        assert safe_float("-500.5") == -500.5


# ── safe_date ─────────────────────────────────────────────────────────


class TestSafeDate:
    def test_iso_format(self):
        assert safe_date("2024-03-15") == date(2024, 3, 15)

    def test_slash_format(self):
        assert safe_date("03/15/2024") == date(2024, 3, 15)

    def test_compact_format(self):
        assert safe_date("20240315") == date(2024, 3, 15)

    def test_none_returns_none(self):
        assert safe_date(None) is None

    def test_empty_string_returns_none(self):
        assert safe_date("") is None

    def test_invalid_date_returns_none(self):
        assert safe_date("not-a-date") is None

    def test_whitespace_stripped(self):
        assert safe_date("  2024-03-15  ") == date(2024, 3, 15)


# ── parse_form4_xml ───────────────────────────────────────────────────

SAMPLE_FORM4_XML = """
<?xml version="1.0"?>
<ownershipDocument>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerName>Cook Timothy D</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>0</isDirector>
      <isOfficer>1</isOfficer>
      <isTenPercentOwner>0</isTenPercentOwner>
      <officerTitle>Chief Executive Officer</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <periodOfReport>2024-03-01</periodOfReport>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle>Common Stock</securityTitle>
      <transactionDate>2024-03-01</transactionDate>
      <transactionCoding>
        <transactionCode>S</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares>50000</transactionShares>
        <transactionPricePerShare>182.63</transactionPricePerShare>
      </transactionAmounts>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction>3280500</sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership>D</directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""

SAMPLE_FILING_META = {
    "accession_number": "0000320193-24-000012",
    "cik": "0000320193",
    "company_name": "Apple Inc.",
    "form_type": "4",
    "filing_date": "2024-03-01",
    "quarter": "2024Q1",
}


class TestParseForm4Xml:
    def test_returns_list(self):
        records = parse_form4_xml(SAMPLE_FORM4_XML, SAMPLE_FILING_META)
        assert isinstance(records, list)
        assert len(records) >= 1

    def test_issuer_fields_populated(self):
        records = parse_form4_xml(SAMPLE_FORM4_XML, SAMPLE_FILING_META)
        r = records[0]
        assert r["issuer_cik"] == "0000320193"
        assert r["issuer_name"] == "Apple Inc."
        assert r["issuer_ticker"] == "AAPL"

    def test_owner_fields_populated(self):
        records = parse_form4_xml(SAMPLE_FORM4_XML, SAMPLE_FILING_META)
        r = records[0]
        assert r["reporting_owner_name"] == "Cook Timothy D"
        assert r["is_officer"] == "1"
        assert r["officer_title"] == "Chief Executive Officer"

    def test_transaction_fields_populated(self):
        records = parse_form4_xml(SAMPLE_FORM4_XML, SAMPLE_FILING_META)
        r = records[0]
        assert r["transaction_code"] == "S"
        assert r["transaction_shares"] == 50000.0
        assert r["price_per_share"] == 182.63
        assert r["shares_owned_after"] == 3280500.0
        assert r["direct_or_indirect"] == "D"
        assert r["security_title"] == "Common Stock"

    def test_dates_are_date_objects(self):
        records = parse_form4_xml(SAMPLE_FORM4_XML, SAMPLE_FILING_META)
        r = records[0]
        assert r["transaction_date"] == date(2024, 3, 1)
        assert r["period_of_report"] == date(2024, 3, 1)
        assert r["filing_date"] == date(2024, 3, 1)

    def test_meta_fields_preserved(self):
        records = parse_form4_xml(SAMPLE_FORM4_XML, SAMPLE_FILING_META)
        r = records[0]
        assert r["accession_number"] == "0000320193-24-000012"
        assert r["cik"] == "0000320193"
        assert r["quarter"] == "2024Q1"

    def test_ingested_at_is_datetime(self):
        records = parse_form4_xml(SAMPLE_FORM4_XML, SAMPLE_FILING_META)
        assert isinstance(records[0]["ingested_at"], datetime)

    def test_empty_xml_returns_header_record(self):
        records = parse_form4_xml(
            "<ownershipDocument></ownershipDocument>", SAMPLE_FILING_META
        )
        assert len(records) == 1
        assert records[0]["transaction_code"] is None


# ── get_quarters_to_process ───────────────────────────────────────────


class TestGetQuartersToProcess:
    def test_returns_list_of_tuples(self):
        result = get_quarters_to_process([2023])
        assert isinstance(result, list)
        assert all(isinstance(q, tuple) and len(q) == 2 for q in result)

    def test_all_four_quarters_for_past_year(self):
        result = get_quarters_to_process([2023])
        years_quarters = [(y, q) for y, q in result if y == 2023]
        assert len(years_quarters) == 4

    def test_no_future_quarters(self):
        result = get_quarters_to_process([2099])
        assert len(result) == 0

    def test_multiple_years(self):
        result = get_quarters_to_process([2022, 2023])
        years = {y for y, _ in result}
        assert 2022 in years
        assert 2023 in years
