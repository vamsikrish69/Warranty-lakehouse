-- Mart (fact): one row per service request.
-- Grain: sr_id. Every enrichment is reduced to sr_id (or contact_id, which is unique)
-- BEFORE joining, so no join can duplicate requests.
-- as_of_ts: latest updated_at in the data by default; override with
--   --vars '{as_of_ts: "YYYY-MM-DD HH:MM:SS"}'. Point to current_timestamp() for live data.

{{ config(materialized='table') }}

with requests as (

    select
        sr_id,
        contact_id,
        customer_id,
        asset_id,
        entitlement_id,
        issue_category,
        priority,
        service_type,
        status,
        created_ts,
        sla_due_ts,
        closed_ts,
        escalated_flag,
        assigned_team,
        billable_flag,
        region,
        country,
        updated_at
    from {{ ref('stg_service_requests') }}

),

as_of as (

    select
        coalesce(try_cast('{{ var("as_of_ts", "") }}' as timestamp), max(updated_at)) as as_of_ts
    from requests

),

warranty as (

    select
        contact_id,
        claimed_warranty_status,
        validated_warranty_status,
        is_claim_mismatch
    from {{ ref('int_warranty_validation') }}

),

repeats as (

    select
        sr_id,
        request_number_for_asset,
        is_follow_up_request,
        is_same_issue_as_prev,
        is_repeat_after_closure,
        hours_since_prev_request
    from {{ ref('int_repeat_service_requests') }}

),

repairs as (

    select
        sr_id,
        count(*)                                                        as repair_event_count,
        max(case when event_type = 'PART_ORDERED' then 1 else 0 end) = 1 as had_part_ordered,
        min(case when event_type = 'TECH_DISPATCHED' then event_ts end) as first_dispatch_ts,
        max(case when event_type = 'REPAIR_COMPLETED' then event_ts end) as repair_completed_ts
    from {{ ref('stg_repair_events') }}
    group by sr_id

),

closures as (

    select
        sr_id,
        closure_id,
        resolution_code,
        csat_score,
        is_invalid_csat,
        reopened_flag
    from {{ ref('stg_case_closures') }}
    qualify row_number() over (partition by sr_id order by closed_ts desc, closure_id desc) = 1

),

scored as (

    select
        r.*,
        a.as_of_ts,
        case
            when r.sla_due_ts is null                                  then 'NO_SLA_DUE'
            when r.closed_ts is not null and r.closed_ts <= r.sla_due_ts then 'MET'
            when r.closed_ts is not null and r.closed_ts >  r.sla_due_ts then 'BREACHED'
            when a.as_of_ts > r.sla_due_ts                             then 'OPEN_OVERDUE'
            else 'OPEN_WITHIN_SLA'
        end as sla_outcome
    from requests r
    cross join as_of a

)

select
    s.sr_id,
    s.contact_id,
    s.customer_id,
    s.asset_id,
    s.entitlement_id,
    s.region,
    s.country,
    s.priority,
    s.service_type,
    s.issue_category,
    s.assigned_team,
    s.escalated_flag,
    s.billable_flag,
    s.status,
    s.created_ts,
    s.sla_due_ts,
    s.closed_ts,
    s.as_of_ts,

    -- SLA
    round((unix_timestamp(s.sla_due_ts) - unix_timestamp(s.created_ts)) / 3600.0, 1)  as sla_target_hours,
    s.sla_outcome,
    s.sla_outcome in ('BREACHED', 'OPEN_OVERDUE')                                      as is_sla_breached,
    case when s.closed_ts is null
         then round((unix_timestamp(s.sla_due_ts) - unix_timestamp(s.as_of_ts)) / 3600.0, 1)
    end                                                                                as hours_until_sla_due_at_as_of,
    case when s.closed_ts is not null
         then round((unix_timestamp(s.closed_ts) - unix_timestamp(s.created_ts)) / 3600.0, 1)
    end                                                                                as resolution_hours,

    -- warranty at the time of contact
    w.claimed_warranty_status,
    coalesce(w.validated_warranty_status, 'NO_CONTACT_MATCH')                          as validated_warranty_status,
    coalesce(w.is_claim_mismatch, false)                                               as is_claim_mismatch,

    -- repeat behaviour
    rp.request_number_for_asset,
    rp.is_follow_up_request,
    rp.is_same_issue_as_prev,
    rp.is_repeat_after_closure,
    rp.hours_since_prev_request,

    -- repair summary
    coalesce(rs.repair_event_count, 0)                                                 as repair_event_count,
    coalesce(rs.had_part_ordered, false)                                               as had_part_ordered,
    rs.first_dispatch_ts,
    rs.repair_completed_ts,

    -- closure and customer feedback
    c.closure_id,
    c.resolution_code,
        c.csat_score,
    case when c.is_invalid_csat then null else c.csat_score end                        as csat_score_clean,
    c.reopened_flag

from scored s
left join warranty w  on w.contact_id = s.contact_id
left join repeats  rp on rp.sr_id     = s.sr_id
left join repairs  rs on rs.sr_id     = s.sr_id
left join closures c  on c.sr_id      = s.sr_id