-- Intermediate model: repeat service requests per asset.
-- Grain: one row per sr_id.
-- Facts about the previous request on the same asset are kept as columns, so
-- marts can pick their own rule. The 30-day rule lives only in the variable below.

{% set repeat_window_days = var('repeat_window_days', 30) %}

with requests as (

    select
        sr_id,
        asset_id,
        customer_id,
        region,
        issue_category,
        status,
        created_ts,
        closed_ts
    from {{ ref('stg_service_requests') }}

),

sequenced as (

    select
        *,
        row_number() over (partition by asset_id order by created_ts, sr_id) as request_number_for_asset,
        lag(sr_id)           over (partition by asset_id order by created_ts, sr_id) as prev_sr_id,
        lag(created_ts)      over (partition by asset_id order by created_ts, sr_id) as prev_created_ts,
        lag(status)          over (partition by asset_id order by created_ts, sr_id) as prev_status,
        lag(closed_ts)       over (partition by asset_id order by created_ts, sr_id) as prev_closed_ts,
        lag(issue_category)  over (partition by asset_id order by created_ts, sr_id) as prev_issue_category
    from requests

)

select
    sr_id,
    asset_id,
    customer_id,
    region,
    issue_category,
    status,
    created_ts,
    request_number_for_asset,
    prev_sr_id,
    prev_created_ts,
    prev_status,
    prev_issue_category,
    round((unix_timestamp(created_ts) - unix_timestamp(prev_created_ts)) / 3600.0, 1)
        as hours_since_prev_request,
    prev_sr_id is not null                                  as is_follow_up_request,
    coalesce(issue_category = prev_issue_category, false)   as is_same_issue_as_prev,
    coalesce(
        prev_closed_ts is not null
        and prev_closed_ts <= created_ts
        and created_ts <= timestampadd(DAY, {{ repeat_window_days }}, prev_closed_ts),
        false
    )                                                       as is_repeat_after_closure
from sequenced