-- Staging model for service_requests, LATEST grain: one row per sr_id.
-- Built on the history model. Ordered by updated_at (event time), NOT load time,
-- so a late-arriving older version can never overwrite a newer status.

select *
from {{ ref('stg_service_requests_history') }}
qualify row_number() over (
    partition by sr_id
    order by updated_at desc, _loaded_at desc
) = 1