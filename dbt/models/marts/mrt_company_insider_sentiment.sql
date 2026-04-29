{{
  config(
    materialized = 'table',
    schema       = 'sec_marts',
    description  = 'Company-level insider sentiment — buy/sell ratio and net position'
  )
}}

/*
  Business Question: Which companies have the strongest insider buy or sell signal?

  Used by Streamlit dashboard for:
  - Company sentiment heatmap
  - Buy/sell ratio ranking
  - Identifying clusters of insider selling
*/

with transactions as (

    select * from {{ ref('int_insider_transactions') }}
    where buy_sell_flag in ('Buy', 'Sell')
      and issuer_ticker is not null

),

company_totals as (

    select
        issuer_ticker,
        issuer_name,

        -- ── Buy metrics ────────────────────────────────────────────────
        countif(buy_sell_flag = 'Buy')                  as num_buy_transactions,
        sum(case when buy_sell_flag = 'Buy'
            then transaction_shares else 0 end)         as total_buy_shares,
        sum(case when buy_sell_flag = 'Buy'
            then transaction_value_usd else 0 end)      as total_buy_value_usd,

        -- ── Sell metrics ───────────────────────────────────────────────
        countif(buy_sell_flag = 'Sell')                 as num_sell_transactions,
        sum(case when buy_sell_flag = 'Sell'
            then transaction_shares else 0 end)         as total_sell_shares,
        sum(case when buy_sell_flag = 'Sell'
            then transaction_value_usd else 0 end)      as total_sell_value_usd,

        -- ── Unique insiders ────────────────────────────────────────────
        count(distinct insider_name)                    as num_unique_insiders,
        count(distinct case when buy_sell_flag = 'Buy'
            then insider_name end)                      as num_buyers,
        count(distinct case when buy_sell_flag = 'Sell'
            then insider_name end)                      as num_sellers,

        -- ── Date range ─────────────────────────────────────────────────
        min(transaction_date)                           as earliest_transaction,
        max(transaction_date)                           as latest_transaction,
        max(ingested_at)                                as last_ingested_at

    from transactions
    group by 1, 2

),

with_ratios as (

    select
        *,

        -- Net value: positive = net buying signal, negative = net selling
        total_buy_value_usd - total_sell_value_usd      as net_value_usd,

        -- Buy ratio: 1.0 = all buys, 0.0 = all sells
        safe_divide(
            num_buy_transactions,
            num_buy_transactions + num_sell_transactions
        )                                               as buy_ratio,

        -- Sentiment label
        case
            when safe_divide(num_buy_transactions,
                num_buy_transactions + num_sell_transactions) >= 0.7
                then 'Strong Buy Signal'
            when safe_divide(num_buy_transactions,
                num_buy_transactions + num_sell_transactions) >= 0.5
                then 'Moderate Buy Signal'
            when safe_divide(num_buy_transactions,
                num_buy_transactions + num_sell_transactions) >= 0.3
                then 'Moderate Sell Signal'
            else 'Strong Sell Signal'
        end                                             as sentiment_label,

        -- Rank companies by net selling (most selling = most interesting for analysis)
        rank() over (order by total_sell_value_usd desc) as sell_rank,
        rank() over (order by total_buy_value_usd desc)  as buy_rank

    from company_totals
    where num_buy_transactions + num_sell_transactions >= 3  -- minimum activity threshold

)

select * from with_ratios
order by abs(net_value_usd) desc