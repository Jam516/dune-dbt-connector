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
        , txn_fee_usd
        , codes_array
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
        , txn_fee_usd
        , codes_array
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
        , c.txn_fee_usd
        , trim(code) as builder_code
    from combined as c
    cross join unnest(c.codes_array) as t(code)
)

select
    block_date
    , builder_code
    , count(distinct unique_id) as num_transactions
    , sum(txn_fee_usd) as txn_fee_usd
from expanded
where builder_code is not null
    and builder_code != ''
group by block_date, builder_code
