-- Staging model for warranty_entitlements.
-- 1) parse the raw JSON string into typed columns
-- 2) drop rows with no key, keep ONE row per entitlement_id (the latest version)
-- 3) flag (do not delete) suspicious values so later layers can decide
-- NOTE: entitlement_status can be stale. Do not trust it for coverage checks.
--       Later layers derive warranty status from coverage_start / coverage_end.

with parsed as (

    select
        value:entitlement_id::string     as entitlement_id,
        value:asset_id::string           as asset_id,
        value:warranty_plan::string      as warranty_plan,
        value:warranty_type::string      as warranty_type,
        value:coverage_years::int        as coverage_years,
        value:coverage_start::date       as coverage_start,
        value:coverage_end::date         as coverage_end,
        value:entitlement_status::string as entitlement_status,
        value:event_ts::timestamp        as event_ts,
        value:updated_at::timestamp      as updated_at,
        value:batch_id::string           as batch_id,
        _source_file,
        _loaded_at
    from {{ source('bronze', 'warranty_entitlements') }}

),

latest as (

    select *
    from parsed
    where entitlement_id is not null
    qualify row_number() over (
        partition by entitlement_id
        order by updated_at desc, _loaded_at desc
    ) = 1

)

select
    *,
    coalesce(coverage_end < coverage_start, false) as is_invalid_coverage_range
from latest