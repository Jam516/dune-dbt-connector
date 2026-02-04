{{ config(
    alias = 'builder_code_metrics'
    , materialized = 'incremental'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'builder_code']
    , properties = {
        "partitioned_by": "ARRAY['block_date']"
    }
) }}

with combined as (
    select
        block_date
        , unique_id
        , tx_hash
        , from_address 
        , txn_fee_usd
        , codes_array
        , start_log_index
        , end_log_index
    from {{ ref('tx_builder_codes') }}
    where block_date >= date '2026-01-01'
        and block_date < current_date
        {% if is_incremental() %}
        and block_date >= current_date - interval '3' day
        {% endif %}

    union all

    select
        block_date
        , unique_id
        , tx_hash
        , from_address 
        , txn_fee_usd
        , codes_array
        , start_log_index
        , end_log_index
    from {{ ref('uo_builder_codes') }}
    where block_date >= date '2026-01-01'
        and block_date < current_date
        {% if is_incremental() %}
        and block_date >= current_date - interval '3' day
        {% endif %}
)

, expanded as (
    select
        c.block_date
        , c.unique_id
        , c.tx_hash
        , c.from_address
        , c.txn_fee_usd
        , trim(code) as builder_code
        , c.start_log_index
        , c.end_log_index
    from combined as c
    cross join unnest(c.codes_array) as t(code)
)

, trade_vol as (
    select 
        e.unique_id,
        sum(t.amount_usd) as volume
    from expanded e
    inner join dex.trades t
        on e.tx_hash = t.tx_hash
        and t.evt_index >= e.start_log_index
        and t.evt_index <= e.end_log_index
        and t.blockchain = 'base'
        and t.block_month >= date '2026-01-01'
        and t.block_date < current_date
        {% if is_incremental() %}
        and block_date >= current_date - interval '3' day
        {% endif %}
    group by 1
)


select
    e.block_date
    , e.builder_code
    , count(distinct e.unique_id) as num_transactions
    , count(distinct e.from_address) as num_transacting_addresses
    , sum(e.txn_fee_usd) as txn_fee_usd
    , sum(v.volume) as trading_volume
from expanded e
left join trade_vol v
    on e.unique_id = v.unique_id
where e.builder_code is not null
    and e.builder_code != ''
group by e.block_date, e.builder_code
