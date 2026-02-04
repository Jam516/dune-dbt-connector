{{ config(
    alias = 'tx_builder_codes'
    , materialized = 'incremental'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'unique_id']
    , properties = {
        "partitioned_by": "ARRAY['block_date']"
    }
) }}

with log_ranges as (
    select
        tx_hash
        , min(index) as start_log_index
        , max(index) as end_log_index
    from {{ source('base', 'logs') }}
    where block_date >= date '2026-01-01'
        and block_date < current_date
        {% if is_incremental() %}
        and block_date >= current_date - interval '3' day
        {% endif %}
    group by tx_hash
)

, txs as (
    select
        t.hash
        , t.block_time
        , t.block_date
        , t."from" as from_address
        , (t.l1_fee + t.gas_used * t.gas_price) / 1e18 as txn_fee_eth
        , p.price * (t.l1_fee + t.gas_used * t.gas_price) / 1e18 as txn_fee_usd
        , cast(t.data as varchar) as calldata
        , coalesce(l.start_log_index, 0) as start_log_index
        , coalesce(l.end_log_index, 0) as end_log_index
    from {{ source('base', 'transactions') }} as t
    inner join {{ source('prices', 'hour') }} as p
        on p.timestamp = date_trunc('hour', t.block_time)
        and p.contract_address = 0x0000000000000000000000000000000000000000
        and p.blockchain = 'base'
    left join log_ranges as l
        on t.hash = l.tx_hash
    where t.block_date >= date '2026-01-01'
        and t.block_date < current_date
        -- and t.success = true --we still want to count revenue from failed txns for now
        {% if is_incremental() %}
        and t.block_date >= current_date - interval '3' day
        {% endif %}
)

select
    block_date
    , block_time
    , hash as unique_id
    , hash as tx_hash
    , from_address
    , cast(null as varbinary) as op_hash
    , txn_fee_eth
    , txn_fee_usd
    , start_log_index
    , end_log_index
    , codes_array
from table(
    functions.base_l2.call_data_8021(
        input => table(
            select * from txs
        )
    )
) as x
