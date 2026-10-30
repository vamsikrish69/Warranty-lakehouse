-- Dimension: one row per product asset (a device a customer owns).
-- Grain: asset_id. Built from stg_product_assets, which already keeps the latest version per asset.
-- Bad values are flagged, not removed: is_future_purchase_date marks impossible purchase dates.

{{ config(materialized='table') }}

select
    asset_id,
    customer_id,
    product_model,
    product_family,
    serial_number,
    purchase_date,
    purchase_channel,
    asset_status,
    is_future_purchase_date
from {{ ref('stg_product_assets') }}