{{ config(
    alias = 'base_useroperations'
    , materialized = 'incremental'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'op_hash']
    , properties = {
        "partitioned_by": "ARRAY['block_date']"
    }
) }}

with base as (
    select
        from_hex(json_extract_scalar(json_extract_scalar(opInfo, '$.mUserOp'), '$.sender')) as sender
        , call_tx_hash
        , from_hex(json_extract_scalar(opInfo, '$.userOpHash')) as op_hash
        , call_trace_address
        , callData as call_data
        , call_block_time as block_time
        , call_block_date as block_date
    from {{ source('erc4337_base', 'EntryPoint_v0_6_call_innerHandleOp') }}
    where call_block_date >= date '2026-01-01'
        and call_block_date < current_date
        {% if is_incremental() %}
        and call_block_date >= current_date - interval '3' day
        {% endif %}

    union all

    select
        from_hex(json_extract_scalar(json_extract_scalar(opInfo, '$.mUserOp'), '$.sender')) as sender
        , call_tx_hash
        , from_hex(json_extract_scalar(opInfo, '$.userOpHash')) as op_hash
        , call_trace_address
        , callData as call_data
        , call_block_time as block_time
        , call_block_date as block_date
    from {{ source('erc4337_base', 'EntryPoint_v0_7_call_innerHandleOp') }}
    where call_block_date >= date '2026-01-01'
        and call_block_date < current_date
        {% if is_incremental() %}
        and call_block_date >= current_date - interval '3' day
        {% endif %}

    union all

    select
        from_hex(json_extract_scalar(json_extract_scalar(opInfo, '$.mUserOp'), '$.sender')) as sender
        , call_tx_hash
        , from_hex(json_extract_scalar(opInfo, '$.userOpHash')) as op_hash
        , call_trace_address
        , callData as call_data
        , call_block_time as block_time
        , call_block_date as block_date
    from {{ source('erc4337_base', 'entrypoint_call_innerhandleop') }}
    where call_block_date >= date '2026-01-01'
        and call_block_date < current_date
        {% if is_incremental() %}
        and call_block_date >= current_date - interval '3' day
        {% endif %}
)

, joined as (
    select * from (
    select
        b.*
        , t.to
        , t.trace_address
        , row_number() over (
            partition by b.sender, b.call_trace_address, b.call_tx_hash
            order by t.trace_address asc
        ) as first_call
    from base as b
    inner join {{ source('base', 'traces') }} as t
        on b.block_date = t.block_date
        and b.call_tx_hash = t.tx_hash
        and cardinality(t.trace_address) >= 3
        and element_at(b.call_trace_address, 1) = element_at(t.trace_address, 1)
        and b.sender = t."from"
        and t.call_type != 'delegatecall'
        and t.block_date >= date '2026-01-01'
        and t.block_date < current_date
        and t.success = true
        {% if is_incremental() %}
        and t.block_date >= current_date - interval '3' day
        {% endif %}
    )
    where first_call = 1
)

select
    block_time
    , call_tx_hash as tx_hash
    , op_hash
    , sender
    , call_data
    , block_date
from joined

