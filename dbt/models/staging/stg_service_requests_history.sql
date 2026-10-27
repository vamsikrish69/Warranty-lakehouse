-- Staging model for service_requests, HISTORY grain: one row per sr_id + status change.
-- 1) parse the raw JSON string into typed columns
-- 2) drop rows with no key, remove exact duplicates (same sr_id, status, updated_at)
-- 3) keep every status version; the latest-version model is built on top of this one

with parsed as (

    select
        value:sr_id::string                       as sr_id,
        value:contact_id::string                  as contact_id,
        value:customer_id::string                 as customer_id,
        value:asset_id::string                    as asset_id,
        value:entitlement_id::string              as entitlement_id,
        value:issue_category::string              as issue_category,
        value:priority::string                    as priority,
        value:service_type::string                as service_type,
        value:status::string                      as status,
        value:created_ts::timestamp               as created_ts,
        value:sla_due_ts::timestamp               as sla_due_ts,
        value:closed_ts::timestamp                as closed_ts,
        value:escalated_flag::boolean             as escalated_flag,
        value:assigned_team::string               as assigned_team,
        value:billable_flag::boolean              as billable_flag,
        value:warranty_status_at_contact::string  as warranty_status_at_contact,
        value:region::string                      as region,
        value:country::string                     as country,
        value:event_ts::timestamp                 as event_ts,
        value:updated_at::timestamp               as updated_at,
        value:batch_id::string                    as batch_id,
        _source_file,
        _loaded_at
    from {{ source('bronze', 'service_requests') }}

)

select *
from parsed
where sr_id is not null
qualify row_number() over (
    partition by sr_id, status, updated_at
    order by _loaded_at desc
) = 1