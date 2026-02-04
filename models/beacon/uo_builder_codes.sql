{{ config(
    alias = 'uo_builder_codes'
    , materialized = 'incremental'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'unique_id']
    , properties = {
        "partitioned_by": "ARRAY['block_date']"
    }
) }}

with log_indexs as (
    select
        evt_tx_hash as tx_hash
        , userOpHash as op_hash
        , evt_index as end_log_index
        , actualGasUsed as gas_used
        , actualGasCost as gas_cost
    from {{ source('erc4337_base', 'entrypoint_v0_6_evt_useroperationevent') }}
    where evt_block_date >= date '2026-01-01'
        and evt_block_date < current_date
        {% if is_incremental() %}
        and evt_block_date >= current_date - interval '3' day
        {% endif %}

    union all

    select
        evt_tx_hash as tx_hash
        , userOpHash as op_hash
        , evt_index as end_log_index
        , actualGasUsed as gas_used
        , actualGasCost as gas_cost
    from {{ source('erc4337_base', 'entrypoint_v0_7_evt_useroperationevent') }}
    where evt_block_date >= date '2026-01-01'
        and evt_block_date < current_date
        {% if is_incremental() %}
        and evt_block_date >= current_date - interval '3' day
        {% endif %}

    union all

    select
        evt_tx_hash as tx_hash
        , userOpHash as op_hash
        , evt_index as end_log_index
        , actualGasUsed as gas_used
        , actualGasCost as gas_cost
    from {{ source('erc4337_base', 'entrypoint_evt_useroperationevent') }}
    where evt_block_date >= date '2026-01-01'
        and evt_block_date < current_date
        {% if is_incremental() %}
        and evt_block_date >= current_date - interval '3' day
        {% endif %}
)

, log_ranges as (
    select
        tx_hash
        , op_hash
        , end_log_index
        , coalesce(
            lag(end_log_index) over (
                partition by tx_hash
                order by end_log_index, op_hash
            ) + 1
            , 0
        ) as start_log_index
        , gas_used
        , gas_cost
    from log_indexs
)

, uo_pct as (
    select
        tx_hash
        , op_hash
        , cast(gas_used as double) /
            sum(gas_used) over (partition by tx_hash) as pct_of_tx
    from log_indexs
)

, uos as (
    select
        u.tx_hash
        , u.op_hash
        , u.block_time
        , u.block_date
        , u.sender as from_address
        , pc.pct_of_tx * (t.l1_fee + t.gas_used * t.gas_price) / 1e18 as txn_fee_eth
        , pc.pct_of_tx * p.price * (t.l1_fee + t.gas_used * t.gas_price) / 1e18 as txn_fee_usd
        , cast(u.call_data as varchar) as calldata
        , l.start_log_index
        , l.end_log_index
    from {{ ref('base_useroperations') }} as u
    inner join log_ranges as l
        on l.op_hash = u.op_hash
    inner join uo_pct as pc
        on pc.op_hash = u.op_hash
    inner join {{ source('base', 'transactions') }} as t
        on t.hash = u.tx_hash
        and t.block_date = u.block_date
        and t.block_date >= date '2026-01-01'
        and t.block_date < current_date
        {% if is_incremental() %}
        and t.block_date >= current_date - interval '3' day
        {% endif %}
    inner join {{ source('prices', 'hour') }} as p
        on p.timestamp = date_trunc('hour', u.block_time)
        and p.contract_address = 0x0000000000000000000000000000000000000000
        and p.blockchain = 'base'
    where u.block_date >= date '2026-01-01'
        and u.block_date < current_date
        {% if is_incremental() %}
        and u.block_date >= current_date - interval '3' day
        {% endif %}
)

select
    block_date
    , block_time
    , op_hash as unique_id
    , op_hash
    , tx_hash
    , from_address
    , txn_fee_eth
    , txn_fee_usd
    , start_log_index
    , end_log_index
    , codes_array
from table(
    functions.base_l2.call_data_8021(
        input => table(
            select * from uos
        )
    )
) as x
