-- Dimension: one row per customer.
-- Grain: customer_id. Built from stg_customers, which already keeps the latest version per customer.
-- email and phone are left out on purpose: marts and dashboards rarely need personal contact details.

{{ config(materialized='table') }}

select
    customer_id,
    first_name,
    last_name,
    concat_ws(' ', first_name, last_name) as customer_name,
    region,
    country,
    city,
    customer_segment,
    created_at,
    is_valid_email
from {{ ref('stg_customers') }}