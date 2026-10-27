# data_gen: micro-batch source simulator

Generates the same 7 JSON datasets as the Snowflake v1 project, but as a stream of small batches so the
pipeline (S3 -> Airflow -> Databricks -> dbt) behaves like a near real-time system. Python 3.9+, standard library only.

## Quick start

```bash
# backfill ~10 days of activity (60 batches x 4 simulated hours), instantly
python data_gen/generate_batch.py --batches 60 --start 2026-10-01T00:00:00Z

# then act like a live source: one batch per run (this is what the Airflow DAG calls)
python data_gen/generate_batch.py --batches 1
```

State is kept in `./state/state.json`, so each run continues the same simulation (open cases move forward,
IDs keep counting). `--reset` starts over. Same `--seed` + same state = identical output.

## Output layout

```
output/
  customers/dt=2026-10-01/customers_b000001_20261001T000000.json
  service_requests/dt=.../service_requests_<batch_id>.json
  ... one folder per table ...
  _manifests/<batch_id>.json      <- written LAST; treat as the "batch committed" marker
```

* Files are newline-delimited JSON (one object per line), which Spark / Auto Loader reads natively.
* In-progress files are dot-prefixed `.tmp` names; skip them when listing.
* S3 mapping: upload `output/<table>/dt=.../<file>` to `s3://<bucket>/raw/<table>/dt=.../<file>`.
* Each manifest lists rows per table, files, and exactly how many bad / duplicate / late rows were injected,
  so you can prove your dbt tests catch them. It is also the source for the load-audit table.

## Tables (all carry `event_ts`, `updated_at`, `batch_id`)

| Table | Key | Notable columns |
|---|---|---|
| customers | customer_id | region, country, city, customer_segment, email, phone |
| product_assets | asset_id | customer_id, product_model, product_family, serial_number, purchase_date |
| warranty_entitlements | entitlement_id | asset_id, warranty_plan, warranty_type (MAIL_IN/ONSITE), coverage_start/end, entitlement_status |
| customer_contacts | contact_id | channel, issue_category, duration_sec, agent_id, warranty_status_at_contact, troubleshooting_outcome, first_contact_resolved |
| service_requests | sr_id (many versions) | priority, service_type, status, created_ts, sla_due_ts, escalated_flag, assigned_team, billable_flag, closed_ts |
| repair_events | repair_event_id | sr_id, event_type, repair_center, technician_id, part_number, part_eta, labor_hours, repair_cost |
| case_closures | closure_id | sr_id, closed_ts, resolution_code, reopened_flag, csat_score |

Lifecycle per case: contact -> (troubleshooting) -> warranty check -> SR created -> repair events -> repair complete -> closure.
Mail-in: DEVICE_RECEIVED, DIAGNOSED, [PART_ORDERED, PART_RECEIVED], REPAIR_STARTED, REPAIR_COMPLETED, SHIPPED_BACK.
Onsite: TECH_DISPATCHED, DIAGNOSED, [PART_ORDERED, PART_RECEIVED, second TECH_DISPATCHED], REPAIR_COMPLETED.
SLA = hours from `created_ts` to `REPAIR_COMPLETED` (see `SLA_HOURS` in the script).

## Realism built in (and what to model for it)

| Behaviour | Default | What it exercises |
|---|---|---|
| `service_requests` is append-only status history | one row per status change | take the latest row per `sr_id` by `updated_at` (incremental merge) |
| Late-arriving event rows | 3% land 1-3 batches late | out-of-order `updated_at`, event time vs load time |
| Exact duplicate rows | 2% | dedupe in staging, unique tests |
| Bad rows | 1% | null keys, invalid channel (`FAX_SUPPORT`), coverage end < start, negative cost, orphan `asset_id`/`sr_id`, CSAT = 9, bad email, future purchase date |
| Reopened cases and repeat contacts | 5% reopened, ~6% repeat | `INT_REPEAT_SERVICE_REQUESTS` (same asset + issue) |
| SLA breaches rise over time | `--drift 0.0015` | trend lines on the SLA mart (about 12% breach in week 1, about 18% in week 2) |
| Entitlement expiry updates | automatic | `entitlement_status` goes stale, so derive warranty status from dates, not the column |

## Useful flags

`--contacts-per-batch 60` volume | `--tick-hours 4` simulated time per batch | `--clock real` use wall-clock windows |
`--late-rate`, `--dup-rate`, `--bad-rate` | `--seed-customers 600` size of the first batch | `--sleep-seconds N` pause between batches.

Volume check: 60 batches is about 13 MB and about 9k service-request rows, comfortable for Databricks Free Edition.

## Git hygiene

Add to `.gitignore`: `data_gen/output/` and `data_gen/state/`. Never commit AWS keys or Databricks tokens.
