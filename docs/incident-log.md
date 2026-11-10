# Incident log

One entry per drill: what I injected, how it was detected, root cause, fix, prevention.

| # | Date | Injected | Detected by | Root cause | Fix | Prevention |
|---|---|---|---|---|---|---|
| 1 | 2026-11-10 | Late-arriving data: generator run with --late-rate 0.4 (normally 0.03) | Manifest counters (161-201 rows deferred per batch) and bronze versus staging counts | Source delivering about 40% of rows late, out of order | None needed: latest-version dedupe in staging absorbed it (bronze 26,269 rows, staging = fct = distinct requests = 5,758) | Alert when late_rows_deferred exceeds a threshold; keep staging dedupe and the fact/staging row-count reconciliation test |
