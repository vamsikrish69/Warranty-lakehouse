-- Datasets behind the Databricks dashboard "Warranty SLA Overview".
-- Every query reads only from the dbt marts schema (workspace.marts).
-- The dashboard itself is built in the Databricks UI; this file keeps its logic in Git.

-- dataset: sla_summary
-- Chart: breach rate (decided) by priority, faceted by region.
select
  region,
  priority,
  total_requests,
  decided_requests,
  breach_rate,
  breach_rate_decided
from workspace.marts.mart_sla_summary
order by region, priority;

-- dataset: sla_outcomes
-- Chart: requests by SLA outcome and priority (counts, so stacking is valid).
select
  priority,
  sla_outcome,
  count(*) as requests
from workspace.marts.fct_service_requests
group by priority, sla_outcome
order by priority, sla_outcome;

-- dataset: daily_breach
-- Chart: breach rate by creation day. Recent days are incomplete (right-censored):
-- most requests created in the last days are still open, so few count as decided.
select
  date_trunc('day', created_ts) as created_day,
  count(*) as requests,
  sum(case when sla_outcome in ('MET', 'BREACHED', 'OPEN_OVERDUE') then 1 else 0 end) as decided,
  sum(case when is_sla_breached then 1 else 0 end) as breached,
  sum(case when is_sla_breached then 1 else 0 end)
    / nullif(sum(case when sla_outcome in ('MET', 'BREACHED', 'OPEN_OVERDUE') then 1 else 0 end), 0) as breach_rate_decided
from workspace.marts.fct_service_requests
group by 1
order by 1;

-- dataset: due_day_priority
-- Chart: breach rate by SLA due date, one line per priority.
-- Only requests whose deadline has passed are included (fully decided).
-- Per priority on purpose: short-SLA priorities fall due first, so a blended line
-- would change with the priority mix rather than with performance.
select
  date_trunc('day', sla_due_ts) as due_day,
  priority,
  count(*) as due_requests,
  avg(case when is_sla_breached then 1.0 else 0.0 end) as breach_rate
from workspace.marts.fct_service_requests
where sla_due_ts <= as_of_ts
group by 1, 2
order by 1, 2;

-- dataset: headline
-- Counters: total requests, decided breach rate (%), average CSAT.
-- csat_score_clean excludes the invalid injected score; CSAT exists only for closed cases.
select
  count(*) as total_requests,
  sum(case when is_sla_breached then 1 else 0 end) as breached_requests,
  sum(case when sla_outcome in ('MET', 'BREACHED', 'OPEN_OVERDUE') then 1 else 0 end) as decided_requests,
  round(100 * sum(case when is_sla_breached then 1 else 0 end)
    / nullif(sum(case when sla_outcome in ('MET', 'BREACHED', 'OPEN_OVERDUE') then 1 else 0 end), 0), 1) as breach_rate_decided_pct,
  round(avg(csat_score_clean), 2) as avg_csat,
  count(csat_score_clean) as csat_responses
from workspace.marts.fct_service_requests;