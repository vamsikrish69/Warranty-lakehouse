-- Staging model for case_closures: one row per closure_id (latest version).
-- 1) parse the raw JSON string into typed columns
-- 2) drop rows with no key, keep the latest version per closure_id
--    (this table has no updated_at, so we order by event_ts)
-- 3) add is_* flag columns; suspicious rows are flagged, not removed

with parsed as (

    select
        value:closure_id::string                                    as closure_id,
        value:sr_id::string                                         as sr_id,
        try_cast(value:closed_ts::string as timestamp)              as closed_ts,
        value:resolution_code::string                               as resolution_code,
        try_cast(value:csat_score::string as int)                   as csat_score,
        try_cast(value:reopened_flag::string as boolean)            as reopened_flag,
        try_cast(value:event_ts::string as timestamp)               as event_ts,
        value:batch_id::string                                      as batch_id,
        _source_file,
        _loaded_at
    from {{ source('bronze', 'case_closures') }}

),

deduped as (

    select *
    from parsed
    where closure_id is not null
    qualify row_number() over (
        partition by closure_id
        order by event_ts desc, _loaded_at desc
    ) = 1

)

select
    *,
    coalesce(csat_score not between 1 and 5, false) as is_invalid_csat
from deduped