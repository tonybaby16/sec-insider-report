{{
  config(
    materialized = 'view',
    schema       = 'sec_staging',
    description  = 'Cleaned and standardised Form 4 transactions from raw layer'
  )
}}

with source as (

    select * from {{ source('sec_raw', 'form4_transactions') }}

),

cleaned as (

    select
        -- ── Identifiers ────────────────────────────────────────────────
        accession_number,
        trim(cik)                                       as cik,
        trim(issuer_cik)                                as issuer_cik,

        -- ── Company / Issuer ───────────────────────────────────────────
        initcap(trim(issuer_name))                      as issuer_name,
        upper(trim(issuer_ticker))                      as issuer_ticker,
        initcap(trim(company_name))                     as filer_company_name,

        -- ── Reporting Owner ────────────────────────────────────────────
        initcap(trim(reporting_owner_name))             as insider_name,
        initcap(trim(officer_title))                    as officer_title,

        -- ── Owner flags — cast to boolean ──────────────────────────────
        case when is_director     = '1' then true else false end as is_director,
        case when is_officer      = '1' then true else false end as is_officer,
        case when is_ten_pct_owner = '1' then true else false end as is_ten_pct_owner,

        -- ── Classify insider role ──────────────────────────────────────
        case
            when is_officer   = '1' then 'Officer'
            when is_director  = '1' then 'Director'
            when is_ten_pct_owner = '1' then '10% Owner'
            else 'Other'
        end                                             as insider_role,

        -- ── Dates ──────────────────────────────────────────────────────
        transaction_date,
        filing_date,
        period_of_report,

        -- ── Transaction ────────────────────────────────────────────────
        upper(trim(transaction_code))                   as transaction_code,

        -- Human-readable transaction type
        case upper(trim(transaction_code))
            when 'S' then 'Sale'
            when 'P' then 'Purchase'
            when 'M' then 'Option Exercise'
            when 'G' then 'Gift'
            when 'F' then 'Tax Withholding'
            when 'A' then 'Grant/Award'
            when 'D' then 'Sale Back to Issuer'
            when 'X' then 'Option Exercise (Exempt)'
            else 'Other'
        end                                             as transaction_type,

        cast(transaction_shares as numeric)             as transaction_shares,
        price_per_share,
        shares_owned_after,

        -- ── Derived: transaction value ─────────────────────────────────
        case
            when transaction_shares is not null and price_per_share is not null
            then round(transaction_shares * price_per_share, 2)
            else null
        end                                             as transaction_value_usd,

        -- ── Ownership ──────────────────────────────────────────────────
        case upper(trim(direct_or_indirect))
            when 'D' then 'Direct'
            when 'I' then 'Indirect'
            else direct_or_indirect
        end                                             as ownership_type,

        upper(trim(security_title))                     as security_title,

        -- ── Buy / Sell flag ────────────────────────────────────────────
        case upper(trim(transaction_code))
            when 'P' then 'Buy'
            when 'S' then 'Sell'
            when 'M' then 'Exercise'
            when 'G' then 'Gift'
            else 'Other'
        end                                             as buy_sell_flag,

        -- ── Metadata ───────────────────────────────────────────────────
        form_type,
        quarter,
        ingested_at

    from source

    where
        -- Filter out records with no transaction data (header-only records)
        transaction_code is not null

)

select * from cleaned