-- Intermediate model: validate the warranty status claimed at each contact
-- against the entitlement's coverage dates.
-- Grain: one row per contact_id (entitlements are 1 per asset, so the join cannot fan out).
-- Business rule lives here once; marts reuse validated_warranty_status.

with contacts as (

    select
        contact_id,
        customer_id,
        asset_id,
        event_ts                    as contact_ts,
        warranty_status_at_contact  as claimed_warranty_status
    from {{ ref('stg_customer_contacts') }}

),

entitlements as (

    select
        entitlement_id,
        asset_id,
        warranty_plan,
        warranty_type,
        coverage_start,
        coverage_end,
        is_invalid_coverage_range
    from {{ ref('stg_warranty_entitlements') }}

),

joined as (

    select
        c.contact_id,
        c.customer_id,
        c.asset_id,
        c.contact_ts,
        c.claimed_warranty_status,
        e.entitlement_id,
        e.warranty_plan,
        e.warranty_type,
        e.coverage_start,
        e.coverage_end,
        case
            when e.entitlement_id is null           then 'NO_ENTITLEMENT'
            when e.is_invalid_coverage_range        then 'INVALID_COVERAGE_RANGE'
            when to_date(c.contact_ts) between e.coverage_start and e.coverage_end
                                                    then 'IN_WARRANTY'
            else 'OUT_OF_WARRANTY'
        end as validated_warranty_status
    from contacts c
    left join entitlements e
        on e.asset_id = c.asset_id

)

select
    *,
    coalesce(
        validated_warranty_status in ('IN_WARRANTY', 'OUT_OF_WARRANTY')
        and validated_warranty_status <> claimed_warranty_status,
        false
    ) as is_claim_mismatch
from joined