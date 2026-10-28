-- Staging model for repair_events: one row per repair_event_id (latest version).
-- 1) parse the raw JSON string into typed columns
-- 2) drop rows with no key, keep the latest version per repair_event_id
-- 3) add is_* flag columns; suspicious rows are flagged, not removed

with parsed as (

    select
        value:repair_event_id::string                  as repair_event_id,
        value:sr_id::string                            as sr_id,
        value:event_type::string                       as event_type,
        value:technician_id::string                    as technician_id,
        value:repair_center::string                    as repair_center,
        value:part_number::string                      as part_number,
        try_cast(value:part_eta::string as timestamp)  as part_eta,
        value:labor_hours::double                      as labor_hours,
        value:repair_cost::double                      as repair_cost,
        value:event_ts::timestamp                      as event_ts,
        value:updated_at::timestamp                    as updated_at,
        value:batch_id::string                         as batch_id,
        _source_file,
        _loaded_at
    from {{ source('bronze', 'repair_events') }}

),

deduped as (

    select *
    from parsed
    where repair_event_id is not null
    qualify row_number() over (
        partition by repair_event_id
        order by updated_at desc, _loaded_at desc
    ) = 1

)

select
    *,
    coalesce(event_type, '') not in
        ('DIAGNOSED', 'PART_ORDERED', 'REPAIR_COMPLETED', 'TECH_DISPATCHED') as is_invalid_event_type,
    coalesce(labor_hours < 0 or repair_cost < 0, false)                      as is_negative_value
from deduped