# Zenoti PO / Transfer Railway service

This folder is a self-contained API-to-SQL job for purchase and transfer orders.
It includes its own runner, API/authentication helpers, center list,
database helper, requirements, and Dockerfile. It uses the verified 11-field
Orders Listing mapping and updates existing orders by `ref_no`. The same API
snapshot also refreshes both product-detail tables with the existing mappings.

| File | Purpose | SQL output |
| --- | --- | --- |
| `run_all.py` | Resolve one date window, fetch once, and run all three outputs | All three tables |
| `po_and_transfers_api.py` | Map and upsert one summary row per order; coordinate the shared transaction | Summary table |
| `purchase_order.py` | Map one product row per purchase order | Purchase-detail table |
| `transfer_order.py` | Map one product row per transfer order | Transfer-detail table |
| `detail_sql.py` | Replace product rows for fetched orders inside the shared transaction | Both detail tables |

## Railway configuration

Create a separate scheduled Railway service from this repository:

| Setting | Value |
| --- | --- |
| Root Directory | Leave blank (repository root) |
| Builder | Dockerfile |
| Dockerfile Path | `Zenoti/PO_Transfer/Dockerfile` |
| Start Command | `python -u run_all.py` (also the image default) |
| Restart Policy | Never, for this scheduled batch job |
| Cron Schedule | Set the daily time you want; Railway cron uses UTC |

The image installs ODBC Driver 18 on Debian 12 and needs no browser. Configure
API, SQL, scheduling, destination-table and Google Drive values directly in
Railway Variables. Values and variable names are intentionally kept out of this
README.

Every Railway run writes a concise `po_and_transfer_logs.txt` and uploads it to
the configured parent Drive folder, including failed runs. The upload is
best-effort and does not change the API/SQL exit status.

`run_all.py` loads the summary and both detail tables by default. A test service
can instead select isolated test destinations or run summary-only. Mixing a
test summary destination with production detail destinations is rejected before
fetching or writing.

The main `Zenoti/run_all.py` now runs the other reports. Its old PO CSV step
has been removed. After enabling this PO service, disable the old PO website
fetch and ingestion services in Railway.

## Automatic dates

With no date arguments, the service resolves dates once at startup:

```text
end_date   = yesterday in America/New_York
start_date = end_date - 30 days
```

Both ends are inclusive: exactly 31 completed calendar days. For example, if
the current New York date is November 12, 2026, the range is October 12 through
November 11. If UTC has crossed midnight while New York is still November 11,
the range is October 11 through November 10.

All four criteria are fetched: ordered, raised, delivered, and completed.
An order placed earlier can therefore be refreshed when it is delivered inside
the window. Unraised CREATED orders are included through the ordered criterion.
The API supplies current values for selected orders. Both detail tables use
those same selected responses and dates; there is no second API pull.

Date precedence is: command-line boundaries, service-specific fixed boundaries,
shared fixed boundaries, then the automatic rolling range.

Leave fixed date variables unset for the daily rolling job. `--timezone` and
`--lookback-days` override their PO-specific variables. Relative dates `today`
and `yesterday` use the selected calendar timezone. Order/delivery timestamps
and load timestamps retain New York time.

## Local commands

Run from the repository root. Configuration can come from `PO_Transfer/.env`,
the existing `Zenoti/.env`, or environment variables; environment variables
have priority.

```powershell
# Review dates and configured destinations without API calls or SQL connections.
.\.venv\Scripts\python.exe Zenoti\PO_Transfer\run_all.py --dry-run

# Backfill a fixed range using the configured destinations.
.\.venv\Scripts\python.exe Zenoti\PO_Transfer\run_all.py --start-date 2025-01-01 --end-date 2026-11-11

# Replay a saved snapshot into all three CSV outputs without SQL or API calls.
.\.venv\Scripts\python.exe Zenoti\PO_Transfer\run_all.py --from-json <snapshot_api.json> --no-load
```

The `Zenoti/purchase_order.py` and `Zenoti/transfer_order.py` paths are small
compatibility entry points to the loaders now in this folder. Use
`PO_Transfer/run_all.py` for the full scheduled service.

## Writes and failure reporting

The Railway runner loads the summary and both product-detail tables. Pass
`--summary-only` to load only the summary. Summary IDs survive updates, and new
summary IDs are allocated when a table uses a regular integer instead of
identity. For each fetched order, the detail tables replace its previous
product rows, including obsolete rows when an order no longer has products.
Orders absent from the pull are retained in all three tables.

The production summary table stores `ref_no`, `order_on` and `deliver_on` as
`varchar`; the current summary TEST table uses an integer reference and
`datetime2` dates. The loader supports both schemas, keeps text references as
text, and writes text dates as `YYYY-MM-DD HH:MM:SS`. No table type changes are
required.

All API windows must complete before SQL loading starts. The SQL transaction
checks the summary row count and every mapped summary value, plus both detail
row counts, before committing all three tables together. API/configuration
failures return exit code 1; SQL failures roll back all three loads and return 1.
The runner prints progress and failure details to Railway logs.

Local runs keep summary/detail CSVs, API JSON, a load report and request
checkpoints under `data/api_order_ingestion/<start>_to_<end>/`. Railway runs
delete those temporary files in a `finally` block after both success and
failure; only the uploaded Drive log is retained. Run backfills that require
`--resume` locally, where checkpoints are intentionally preserved.

The mapping was compared with the November 10 source CSV: all 161 references
were found, 160 matched every field, and PO4907 had its later delivery update.
The January 2025–November 11, 2026 TEST backfill committed 4,170 verified rows.
