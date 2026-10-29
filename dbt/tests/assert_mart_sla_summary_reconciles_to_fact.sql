-- Fails (returns a row) if the summary loses requests or if the outcome buckets
-- do not add up to the total (for example a new sla_outcome appears).
select *
from (
    select
        (select count(*) from {{ ref('fct_service_requests') }})                      as fct_rows,
        (select sum(total_requests) from {{ ref('mart_sla_summary') }})               as summary_total,
        (select sum(sla_met + sla_breached_closed + open_overdue + open_within_sla)
           from {{ ref('mart_sla_summary') }})                                        as summary_buckets
)
where fct_rows <> summary_total
   or fct_rows <> summary_buckets