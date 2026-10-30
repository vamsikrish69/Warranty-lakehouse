-- Mart (aggregate): SLA performance by region and priority.
-- Grain: one row per region + priority. Built ONLY from fct_service_requests,
-- so the SLA rule lives in one place. Totals must reconcile to the fact.

{{ config(materialized='table') }}

with base as (

    select
        coalesce(region, 'UNKNOWN') as region,
        priority,
        sla_outcome,
        is_sla_breached,
        closed_ts,
        resolution_hours,
        as_of_ts
    from {{ ref('fct_service_requests') }}

),

agg as (

    select
        region,
        priority,
        count(*)                                                        as total_requests,
        sum(case when closed_ts is not null then 1 else 0 end)          as closed_requests,
        sum(case when sla_outcome = 'MET' then 1 else 0 end)            as sla_met,
        sum(case when sla_outcome = 'BREACHED' then 1 else 0 end)       as sla_breached_closed,
        sum(case when sla_outcome = 'OPEN_OVERDUE' then 1 else 0 end)   as open_overdue,
        sum(case when sla_outcome = 'OPEN_WITHIN_SLA' then 1 else 0 end) as open_within_sla,
        round(sum(case when is_sla_breached then 1 else 0 end) / count(*), 4) as breach_rate,
                sum(case when sla_outcome in ('MET', 'BREACHED', 'OPEN_OVERDUE') then 1 else 0 end) as decided_requests,
        round(sum(case when is_sla_breached then 1 else 0 end) / nullif(sum(case when sla_outcome in ('MET', 'BREACHED', 'OPEN_OVERDUE') then 1 else 0 end), 0), 4) as breach_rate_decided,
        round(avg(resolution_hours), 1)                                 as avg_resolution_hours,
        max(as_of_ts)                                                   as as_of_ts
    from base
    group by region, priority

)

select
    concat(region, '|', priority) as summary_key,
    *
from agg