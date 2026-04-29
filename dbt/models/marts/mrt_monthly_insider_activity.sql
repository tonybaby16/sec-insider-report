{{
  config(
    materialized = 'table',
    schema       = 'sec_marts',
    description  = 'Monthly insider trading activity aggregated by company and month',
    partition_by = {
      'field': 'transaction_month_key',
      'data_type': 'string'
    },
    cluster_by   = ['issuer_ticker', 'buy_sell_flag']
  )
}}

/*
  Business Question: What is the monthly insider trading activity per company?

  Used by Streamlit dashboard for:
  - Monthly trend charts by company
  - Buy vs sell volume over time
  - Net insider sentiment (buy value - sell value)
*/

with transactions as (

    select * from {{ ref('int_insider_transactions') }}
    where buy_sell_flag in ('Buy', 'Sell')

),

monthly_agg as (

    select
        -- ── Dimensions ─────────────────────────────────────────────────
        transaction_month_key,
        transaction_year,
        transaction_month,
        issuer_ticker,
        issuer_name,
        buy_sell_flag,

        -- ── Transaction counts ─────────────────────────────────────────
        count(distinct accession_number)                as num_filings,
        count(distinct insider_name)                    as num_unique_insiders,
        count(*)                                        as num_transactions,

        -- ── Volume metrics ─────────────────────────────────────────────
        sum(transaction_shares)                         as total_shares,
        sum(transaction_value_usd)                      as total_value_usd,
        avg(price_per_share)                            as avg_price_per_share,
        max(transaction_value_usd)                      as max_single_transaction_usd,

        -- ── Metadata ───────────────────────────────────────────────────
        max(ingested_at)                                as last_ingested_at

    from transactions
    where issuer_ticker is not null
    group by 1, 2, 3, 4, 5, 6

),

-- ── Pivot to add net sentiment columns ───────────────────────────────
with_net_sentiment as (

    select
        transaction_month_key,
        transaction_year,
        transaction_month,
        issuer_ticker,
        issuer_name,
        buy_sell_flag,
        num_filings,
        num_unique_insiders,
        num_transactions,
        total_shares,
        total_value_usd,
        avg_price_per_share,
        max_single_transaction_usd,
        last_ingested_at,

        -- Net sentiment: positive = net buying, negative = net selling
        sum(case when buy_sell_flag = 'Buy'  then total_value_usd else 0 end)
            over (partition by issuer_ticker, transaction_month_key)
        - sum(case when buy_sell_flag = 'Sell' then total_value_usd else 0 end)
            over (partition by issuer_ticker, transaction_month_key)
        as net_sentiment_usd

    from monthly_agg

)

select * from with_net_sentiment
