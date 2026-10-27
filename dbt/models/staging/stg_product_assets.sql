-- Staging model for product_assets.
-- 1) parse the raw JSON string into typed columns
-- 2) drop rows with no key, keep ONE row per asset_id (the latest version)
-- 3) flag (do not delete) suspicious values so later layers can decide

with parsed as (

    select
        value:asset_id::string         as asset_id,
        value:customer_id::string      as customer_id,
        value:product_model::string    as product_model,
        value:product_family::string   as product_family,
        value:serial_number::string    as serial_number,
        value:purchase_date::date      as purchase_date,
        value:purchase_channel::string as purchase_channel,
        value:asset_status::string     as asset_status,
        value:event_ts::timestamp      as event_ts,
        value:updated_at::timestamp    as updated_at,
        value:batch_id::string         as batch_id,
        _source_file,
        _loaded_at
    from {{ source('bronze', 'product_assets') }}

),

latest as (

    select *
    from parsed
    where asset_id is not null
    qualify row_number() over (
        partition by asset_id
        order by updated_at desc, _loaded_at desc
    ) = 1

)

select
    *,
    coalesce(purchase_date > current_date(), false) as is_future_purchase_date
from latest