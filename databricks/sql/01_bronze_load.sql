-- 01_bronze_load.sql
-- Loads the raw JSON files from the volume into BRONZE tables (the "RAW layer" of your Snowflake v1).
-- Each row = one JSON record stored exactly as received (a string), plus where it came from and when it was loaded.
-- COPY INTO remembers which files it already loaded, so you can re-run this after every new upload
-- and it only picks up the new files.
--
-- Run from the Databricks SQL Editor. Run the CUSTOMERS block first and check the verification
-- query at the bottom; if that works, run the rest.

-- ---------------------------------------------------------------- customers
CREATE TABLE IF NOT EXISTS workspace.warranty.bronze_customers (
  value STRING, _source_file STRING, _loaded_at TIMESTAMP
);
COPY INTO workspace.warranty.bronze_customers
FROM (
  SELECT value, _metadata.file_path AS _source_file, current_timestamp() AS _loaded_at
  FROM '/Volumes/workspace/warranty/raw/landing/customers/'
)
FILEFORMAT = TEXT;

-- ---------------------------------------------------------------- product_assets
CREATE TABLE IF NOT EXISTS workspace.warranty.bronze_product_assets (
  value STRING, _source_file STRING, _loaded_at TIMESTAMP
);
COPY INTO workspace.warranty.bronze_product_assets
FROM (
  SELECT value, _metadata.file_path AS _source_file, current_timestamp() AS _loaded_at
  FROM '/Volumes/workspace/warranty/raw/landing/product_assets/'
)
FILEFORMAT = TEXT;

-- ---------------------------------------------------------------- warranty_entitlements
CREATE TABLE IF NOT EXISTS workspace.warranty.bronze_warranty_entitlements (
  value STRING, _source_file STRING, _loaded_at TIMESTAMP
);
COPY INTO workspace.warranty.bronze_warranty_entitlements
FROM (
  SELECT value, _metadata.file_path AS _source_file, current_timestamp() AS _loaded_at
  FROM '/Volumes/workspace/warranty/raw/landing/warranty_entitlements/'
)
FILEFORMAT = TEXT;

-- ---------------------------------------------------------------- customer_contacts
CREATE TABLE IF NOT EXISTS workspace.warranty.bronze_customer_contacts (
  value STRING, _source_file STRING, _loaded_at TIMESTAMP
);
COPY INTO workspace.warranty.bronze_customer_contacts
FROM (
  SELECT value, _metadata.file_path AS _source_file, current_timestamp() AS _loaded_at
  FROM '/Volumes/workspace/warranty/raw/landing/customer_contacts/'
)
FILEFORMAT = TEXT;

-- ---------------------------------------------------------------- service_requests
CREATE TABLE IF NOT EXISTS workspace.warranty.bronze_service_requests (
  value STRING, _source_file STRING, _loaded_at TIMESTAMP
);
COPY INTO workspace.warranty.bronze_service_requests
FROM (
  SELECT value, _metadata.file_path AS _source_file, current_timestamp() AS _loaded_at
  FROM '/Volumes/workspace/warranty/raw/landing/service_requests/'
)
FILEFORMAT = TEXT;

-- ---------------------------------------------------------------- repair_events
CREATE TABLE IF NOT EXISTS workspace.warranty.bronze_repair_events (
  value STRING, _source_file STRING, _loaded_at TIMESTAMP
);
COPY INTO workspace.warranty.bronze_repair_events
FROM (
  SELECT value, _metadata.file_path AS _source_file, current_timestamp() AS _loaded_at
  FROM '/Volumes/workspace/warranty/raw/landing/repair_events/'
)
FILEFORMAT = TEXT;

-- ---------------------------------------------------------------- case_closures
CREATE TABLE IF NOT EXISTS workspace.warranty.bronze_case_closures (
  value STRING, _source_file STRING, _loaded_at TIMESTAMP
);
COPY INTO workspace.warranty.bronze_case_closures
FROM (
  SELECT value, _metadata.file_path AS _source_file, current_timestamp() AS _loaded_at
  FROM '/Volumes/workspace/warranty/raw/landing/case_closures/'
)
FILEFORMAT = TEXT;

-- ---------------------------------------------------------------- verification
-- 1) row count per bronze table
SELECT 'customers' AS tbl, COUNT(*) AS rows FROM workspace.warranty.bronze_customers
UNION ALL SELECT 'product_assets', COUNT(*) FROM workspace.warranty.bronze_product_assets
UNION ALL SELECT 'warranty_entitlements', COUNT(*) FROM workspace.warranty.bronze_warranty_entitlements
UNION ALL SELECT 'customer_contacts', COUNT(*) FROM workspace.warranty.bronze_customer_contacts
UNION ALL SELECT 'service_requests', COUNT(*) FROM workspace.warranty.bronze_service_requests
UNION ALL SELECT 'repair_events', COUNT(*) FROM workspace.warranty.bronze_repair_events
UNION ALL SELECT 'case_closures', COUNT(*) FROM workspace.warranty.bronze_case_closures;

-- 2) prove the raw JSON string can be read field by field (this is what dbt staging will do)
SELECT value:sr_id::string AS sr_id, value:status::string AS status, value:priority::string AS priority,
       value:created_ts::timestamp AS created_ts, _source_file
FROM workspace.warranty.bronze_service_requests
LIMIT 10;
