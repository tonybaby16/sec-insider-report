{{
  config(
    materialized = 'view',
    schema       = 'sec_intermediate',
    description  = 'Deduplicated insider transactions enriched with derived fields'
  )
}}

with stg as (

    select * from {{ ref('stg_form4_transactions') }}

),

-- ── Deduplicate: keep latest filing per accession + transaction ────────
-- A 4/A amendment supersedes the original Form 4 for the same event
deduplicated as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by
                    accession_number,
                    transaction_date,
                    transaction_code,
                    transaction_shares
                order by
                    -- Prefer 4/A (amendment) over 4 (original)
                    case form_type when '4/A' then 1 else 2 end,
                    filing_date desc
            ) as row_num
        from stg
    )
    where row_num = 1

),

-- ── Enrich with time dimensions ───────────────────────────────────────
enriched as (

    select
        -- ── Core fields ────────────────────────────────────────────────
        accession_number,
        cik,
        issuer_cik,
        issuer_name,
        issuer_ticker,
        insider_name,
        officer_title,
        insider_role,
        is_director,
        is_officer,
        is_ten_pct_owner,

        -- ── Transaction ────────────────────────────────────────────────
        transaction_date,
        filing_date,
        transaction_code,
        transaction_type,
        buy_sell_flag,
        transaction_shares,
        price_per_share,
        transaction_value_usd,
        shares_owned_after,
        ownership_type,
        security_title,

        -- ── Time dimensions ────────────────────────────────────────────
        extract(year  from transaction_date)                    as transaction_year,
        extract(month from transaction_date)                    as transaction_month,
        extract(quarter from transaction_date)                  as transaction_quarter,
        format_date('%Y-%m', transaction_date)                  as transaction_month_key,
        format_date('%Y-Q%Q', transaction_date)                 as transaction_quarter_key,

        -- ── Days between transaction and filing (regulatory metric) ────
        date_diff(filing_date, transaction_date, day)           as days_to_file,

        -- ── Metadata ───────────────────────────────────────────────────
        form_type,
        quarter,
        ingested_at

    from deduplicated

)

select * from enriched
