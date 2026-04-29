{{
  config(
    materialized = 'table',
    schema       = 'sec_marts',
    description  = 'Top insider traders ranked by transaction value'
  )
}}

/*
  Business Question: Who are the most active insider traders?

  Used by Streamlit dashboard for:
  - Top 10 insiders leaderboard by sell/buy value
  - Officer vs Director activity comparison
*/

with transactions as (

    select * from {{ ref('int_insider_transactions') }}
    where buy_sell_flag in ('Buy', 'Sell')
      and transaction_value_usd is not null

),

insider_agg as (

    select
        insider_name,
        issuer_ticker,
        issuer_name,
        insider_role,
        officer_title,
        buy_sell_flag,

        count(*)                                        as num_transactions,
        count(distinct transaction_month_key)           as num_active_months,
        sum(transaction_shares)                         as total_shares,
        sum(transaction_value_usd)                      as total_value_usd,
        avg(transaction_value_usd)                      as avg_transaction_value_usd,
        max(transaction_value_usd)                      as max_transaction_value_usd,
        min(transaction_date)                           as first_transaction_date,
        max(transaction_date)                           as last_transaction_date,
        max(ingested_at)                                as last_ingested_at

    from transactions
    group by 1, 2, 3, 4, 5, 6

)

select
    *,
    rank() over (
        partition by buy_sell_flag
        order by total_value_usd desc
    ) as rank_by_value

from insider_agg
