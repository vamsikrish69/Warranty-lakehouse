-- Staging model for customers.
-- 1) parse the raw JSON string into typed columns
-- 2) drop rows with no key, keep ONE row per customer_id (the latest version)
-- 3) flag (do not delete) suspicious values so later layers can decide

with parsed as (

    select
        value:customer_id::string      as customer_id,
        value:first_name::string       as first_name,
        value:last_name::string        as last_name,
        value:email::string            as email,
        value:phone::string            as phone,
        value:region::string           as region,
        value:country::string          as country,
        value:city::string             as city,
        value:customer_segment::string as customer_segment,
        value:created_at::timestamp    as created_at,
        value:event_ts::timestamp      as event_ts,
        value:updated_at::timestamp    as updated_at,
        value:batch_id::string         as batch_id,
        _source_file,
        _loaded_at
    from {{ source('bronze', 'customers') }}

),

latest as (

    select *
    from parsed
    where customer_id is not null
    qualify row_number() over (
        partition by customer_id
        order by updated_at desc, _loaded_at desc
    ) = 1

)

select
    *,
    coalesce(email rlike '^[^@ ]+@[^@ ]+\\.[^@ ]+$', false) as is_valid_email
from latest
