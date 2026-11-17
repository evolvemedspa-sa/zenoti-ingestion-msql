# Zenoti Stock Inventory Ingestion

## How Dates Work

Stock inventory snapshots use **three separate date columns** that serve different purposes. Understanding the difference is critical for querying historical data correctly.

### The Three Date Columns

| Column | Type | Source | Purpose | Example |
|--------|------|--------|---------|---------|
| `date_inventory` | `date` | Filename | The business date of the snapshot — when the inventory count was taken | `2026-07-31` |
| `date_inserted` | `datetime` | Script runtime | When this batch of rows was loaded into the database | `2026-08-01 09:15:23.456` |
| `ingestion_timestamp` | `datetime` | Script runtime | Duplicate of `date_inserted` for compatibility | `2026-08-01 09:15:23.456` |

### `date_inventory`: The Snapshot Date

**This is the date that matters for business reporting.**

The Zenoti export filename carries the snapshot date, and the script extracts it:

```
Current Stock_2026-07-31.csv  →  date_inventory = 2026-07-31
current_stock_2026_07_29.csv  →  date_inventory = 2026-07-29
Stock20260728.csv             →  date_inventory = 2026-07-28
```

Accepted filename patterns (first match wins):
- `YYYY-MM-DD` or `YYYY_MM_DD` → e.g. `2026-07-31`
- `YYYYMMDD` → e.g. `20260731`

A file named `Current Stock.csv` with no date in the name loads successfully, but every row gets `date_inventory = NULL`. This makes the snapshot invisible to any date-filtered queries, so **always include the date in the filename**.

### `date_inserted` and `ingestion_timestamp`: Load Timestamps

Both columns hold the exact moment the script inserted each row, stamped in **New York local time** (America/New_York timezone, which is EST/EDT depending on the season).

These timestamps answer "when did we load this?" and are useful for:
- Detecting double-loads (same `date_inventory`, different `date_inserted`)
- Auditing ingestion delays (difference between `date_inventory` and `date_inserted`)
- Debugging loads that partially failed

**All rows in one run get the same `date_inserted` value**, even when the script processes multiple files in one execution.

### Why Two Load Timestamps?

`ingestion_timestamp` was added for compatibility with other tables in the pipeline that use that column name. Both hold identical values. Queries can use either one.

## Querying By Date

### Get the most recent snapshot

```sql
SELECT TOP 1000 *
FROM stock_inventory
WHERE date_inventory = (SELECT MAX(date_inventory) FROM stock_inventory)
```

### Compare two specific snapshots

```sql
-- July 30 vs July 31
SELECT 
    s1.product_code,
    s1.product_name,
    s1.on_hand_quantity AS qty_july30,
    s2.on_hand_quantity AS qty_july31,
    s2.on_hand_quantity - s1.on_hand_quantity AS change
FROM 
    stock_inventory s1
INNER JOIN 
    stock_inventory s2 
    ON s1.product_code = s2.product_code
WHERE 
    s1.date_inventory = '2026-07-30'
    AND s2.date_inventory = '2026-07-31'
ORDER BY 
    ABS(s2.on_hand_quantity - s1.on_hand_quantity) DESC
```

### Detect double-loads

```sql
SELECT 
    date_inventory,
    COUNT(*) AS row_count,
    MIN(date_inserted) AS first_load,
    MAX(date_inserted) AS last_load
FROM stock_inventory
GROUP BY date_inventory
HAVING COUNT(DISTINCT date_inserted) > 1
ORDER BY date_inventory DESC
```

If `first_load` and `last_load` differ for the same `date_inventory`, the snapshot was loaded more than once. This usually happens after a failure and manual re-run without cleaning up the partial load first.

### Find snapshots loaded late

```sql
SELECT 
    date_inventory,
    MIN(date_inserted) AS loaded_at,
    DATEDIFF(day, date_inventory, MIN(date_inserted)) AS days_delayed
FROM stock_inventory
GROUP BY date_inventory
HAVING DATEDIFF(day, date_inventory, MIN(date_inserted)) > 1
ORDER BY days_delayed DESC
```

## Loading Behavior

### Append-Only

Every `stock_inventory.py` run **appends** a full snapshot to the table. Nothing is deleted or updated, so the table accumulates history:

```
date_inventory  | rows   | loaded_at
----------------|--------|------------------
2026-07-29      | 3,512  | 2026-07-30 08:10
2026-07-30      | 3,515  | 2026-07-31 08:05
2026-07-31      | 3,515  | 2026-08-01 09:15
```

This means:
- ✅ Historical comparisons work out of the box
- ✅ A failed load can be cleaned up by deleting rows with a specific `date_inventory` and re-running
- ⚠️ The table grows by ~3,500 rows per day
- ⚠️ Queries must filter on `date_inventory` to avoid scanning the entire history

### Processing Multiple Files

When the `.env` variable `CSV_FILE_STOCK_INVENTORY` points to a **directory** instead of a single file, the script processes all `.csv`, `.xlsx`, and `.xlsm` files in that directory in alphabetical order.

Each file's `date_inventory` is extracted from its own filename. This is how you can backfill multiple days in one run:

```
/stock_exports/
  current_stock_2026-07-28.csv  → date_inventory = 2026-07-28
  current_stock_2026-07-29.csv  → date_inventory = 2026-07-29
  current_stock_2026-07-30.csv  → date_inventory = 2026-07-30
```

Running `stock_inventory.py` once on that directory loads all three snapshots, each with its correct `date_inventory`, but all three share the same `date_inserted` timestamp.

### Google Drive Integration

When `.env` has:

```env
CSV_SOURCE=gdrive
GDRIVE_FOLDER_STOCK_INVENTORY=<folder_id>
GDRIVE_CREDENTIALS_JSON=<service_account_json>
```

The script downloads **all CSV files** from that Google Drive folder to a temp directory and processes them just like the local-directory case above. Each file must still have the date in its filename.

## Troubleshooting

### All `date_inventory` values are NULL

**Cause:** The filename does not contain a recognizable date pattern.

**Fix:** Rename the file to include the date, e.g. `Current Stock_2026-07-31.csv`, and reload.

### Duplicate rows for the same `date_inventory`

**Cause:** The script ran more than once on the same snapshot without deleting the previous load.

**Fix:** Delete the duplicates and reload:

```sql
-- Find the extra load
SELECT date_inventory, date_inserted, COUNT(*)
FROM stock_inventory
GROUP BY date_inventory, date_inserted
HAVING COUNT(*) > 0
ORDER BY date_inventory DESC

-- Delete one of the duplicate loads (pick the timestamp to remove)
DELETE FROM stock_inventory
WHERE date_inventory = '2026-07-31' AND date_inserted = '2026-08-01 12:00:00.000'
```

### `date_inventory` is one day behind the export date

This is expected. Zenoti generates the "Current Stock" export at the end of the business day, so a report exported on August 1 reflects inventory **as of July 31**. The filename should match the snapshot date (July 31), not the export date (August 1).

### `date_inserted` is in the wrong timezone

`date_inserted` is always stamped in **New York time** (America/New_York), regardless of where the script runs. This is intentional — it keeps all timestamps consistent even when the script moves to a server in a different timezone.

If you need UTC or another timezone for reporting, convert it in the query:

```sql
SELECT 
    date_inventory,
    date_inserted,
    date_inserted AT TIME ZONE 'Eastern Standard Time' AT TIME ZONE 'UTC' AS date_inserted_utc
FROM stock_inventory
```

## HyperConnect Sync

`Zenoti/zconnect/sync_hc.py` runs `agent_metrics`, `location_performance`,
`events`, and `call_logs`. Review ingestion runs directly inside
`sync_hc.py` and loads `TABLE_HC_EVENTS`. The other reports use
`hc_sql_helper/import_hc_report.py`. `feedback_metrics` is currently disabled
in the sync.
The `events` report runs `get_events.py`; use `--only events` to select it.

Run these commands from the repository root:

```powershell
# Pull and load all enabled reports.
python Zenoti\zconnect\sync_hc.py

# Pull and load reviews only; uses ZENOTI_API_KEY and skips browser login.
python Zenoti\zconnect\sync_hc.py --only events

# Widen the review backfill to the previous 14 complete days.
python Zenoti\zconnect\sync_hc.py --only events --lookback 14

# Backfill an explicit date range.
python Zenoti\zconnect\sync_hc.py --only events --start-date 2026-11-01 --end-date 2026-11-16

# Export reviews to CSV without loading SQL.
python Zenoti\zconnect\sync_hc.py --only events --no-ingest
```

With no explicit dates, the sync fetches the previous **seven complete days**,
ending yesterday (`--lookback 7 --t-minus 1`). For example, a run on
**November 17, 2026 fetches November 10 through November 16, inclusive**.
The date is taken from the machine running the script; the deployed container
uses UTC. This overlapping window catches late-arriving data and updates to
reviews loaded on previous runs. `--start-date`, `--end-date`, and
`--center-id` are forwarded to the exporter.

HyperConnect reports require a browser bearer token, captured once per run
or supplied with `--token`. Reviews use `ZENOTI_API_KEY` from
`Zenoti/.env`. A run with `--only events` needs no browser login.

### Review SQL Ingestion

The production review loader is built into `sync_hc.py`. Configure
`TABLE_HC_EVENTS` and the shared SQL connection settings in `Zenoti/.env`.
The destination must already have the **17 columns exported by `get_events.py`**
plus `date_inserted datetime2(7)`, with a primary key or unique index on
`review_key`. The loader validates the CSV, destination schema, and value
capacity before writing. The former timeline CSV uses a different schema.

Reviews are inserted or refreshed by `review_key` in one transaction. A
failure rolls back the load. Reviews outside the CSV are retained, and older
event versions cannot overwrite newer stored versions. Unicode text,
whitespace, multiline and blank comments, and JSON are preserved.

For a log such as:

```text
Ingested: 2 inserted, 3 refreshed, 1 older versions skipped.
```

| Count | Meaning |
| --- | --- |
| `inserted` | New `review_key` values were added, with a UTC `date_inserted`. |
| `refreshed` | Existing rows were updated using an equal or newer event timestamp. Identical reviews fetched again can count as refreshed. |
| `older versions skipped` | Incoming events were older than the stored review and were left out of the update. |

Refreshes preserve the original `date_inserted`. The seven-day overlap
therefore commonly refreshes previous records without creating duplicate
reviews, while also capturing changed ratings, comments, or replies.

During sync ingestion, CSVs are temporary hand-offs and are deleted after a
successful load. A failed review load keeps `Zenoti/zconnect/events.csv`
for inspection. `--no-ingest` leaves review exports in
`Zenoti/zconnect/output/`. `--stop-on-error` stops the sync at the first
failed pull or load.

## Individual Review Export

`Zenoti/zconnect/get_events.py` exports individual ratings, comments, reviewer
names, review dates, sources, replies, and full review metadata to a local CSV.
It uses `ZENOTI_API_KEY` and optional `ZENOTI_BASE_URL` from
`Zenoti/.env`.

The exporter reads
`GET /v1/zconnect/centers/{center_id}/events?filter=feedback`. Google reviews
come from event metadata. Submitted Zenoti feedback is followed through
`GET /v1/feedbacks?appointment_group_id=...` and exported with
`source=ZENOTI`. Native tags, invoice information, and separate rating
dimensions are preserved in `metadata_json`. The native `rating` is
`service_rating`; unanswered values (`-1`) are left blank.

```powershell
python Zenoti\zconnect\get_events.py --start-date 2026-01-01
python Zenoti\zconnect\get_events.py --center-id cac21ac7-7925-418f-ae6e-1f3e8d45ca2b --start-date 2026-11-01 --end-date 2026-11-16
python Zenoti\zconnect\get_events.py --start-date 2026-11-01 --end-date 2026-11-16 --csv reviews.csv
python Zenoti\zconnect\get_events.py --dry-run
```

Running `get_events.py` directly defaults to all accessible centers from
`2000-01-01` through today. The seven-day default belongs to `sync_hc.py`.
`ZENOTI_REVIEW_START_DATE` and `ZENOTI_REVIEW_END_DATE` can override the
standalone export defaults; general pipeline date settings are not used.

Default output is
`Zenoti/zconnect/output/reviews_<scope>_<start>_<end>.csv`; `--csv` selects
another path. Every page is fetched in inclusive 10-day windows. An optional
`--max-pages` cap aborts the export if it would truncate a window. The CSV is
published only after every center and window finishes successfully; a failed
export does not replace an existing CSV. Blank comments are retained, and
repeated review updates keep the latest event version.

Dates filter event activity, which can differ from the original review date.
Only reviews retained in Zenoti's event feed and details returned by the API
are available; this does not guarantee all historical reviews on Google or
other review sites. The former `--filter` and `--expand-metadata` timeline
options are removed. This CSV also differs from the invoice-based file expected
by `feedback_report.py`.

### API Responses and Troubleshooting

| Response | Exporter behavior |
| --- | --- |
| HTTP 204 from `GET /feedbacks` | Successful **No Content** response. The appointment group has no returned feedback details. Log and cache an empty lookup, create no review row for it, and continue exporting other reviews. |
| HTTP 429 | Back off and retry, honoring `Retry-After` when supplied. |
| Empty or non-JSON HTTP 200 | Retry the same GET request; fail the export if retries are exhausted. |
| Temporary GET failures, including HTTP 408 and 500/502/503/504 | Back off and retry. |

An empty native feedback lookup can appear as:

```text
HTTP 204 on /feedbacks: no feedback details (appointment_group_id=...)
```

This message needs no retry. HTTP 204 handling is specific to native
`/feedbacks` detail requests; it does not turn an unavailable event-feed page
into a successful empty page.

Retry settings come from `Zenoti/.env` or the process environment:

| Setting | Default | Purpose |
| --- | --- | --- |
| `ZENOTI_MAX_RETRIES` | `5` | Up to five retries after the initial request. |
| `ZENOTI_RETRY_BACKOFF` | `5` | Initial retry wait in seconds; doubles on subsequent retries. |
| `ZENOTI_REQUEST_DELAY` | `0.4` | Pause between successive API requests. |

Exhausted errors identify the center, date window, and endpoint. Event-feed
errors include the requested page and received event count; native feedback
errors include the event and appointment group. Non-JSON failures show the
HTTP status, content type, and response body on a separate line.

Restart an existing Python run after updating the code so it loads the current
request handling.

### Offline Checks

These commands exercise export, request handling, and ingestion without live
API requests or database writes:

```powershell
python Zenoti\zconnect\sync_hc.py --selftest
python Zenoti\zconnect\get_events.py --self-check
python -m unittest discover -s Zenoti\zconnect\tests -v
```

### Manual Review Loader

The separate testing loader lives at
`Zenoti/zconnect/hc_ingestion_manual/ingest_hc_events.py`. It reads
`Zenoti/.env` and defaults to the newest `reviews_*.csv` in
`Zenoti/zconnect/output/`. `--csv` selects a specific export. `--dry-run`
validates without a database connection; `--check-db` also checks the live
destination without writing. A successful manual load retains the CSV.

```powershell
python Zenoti\zconnect\hc_ingestion_manual\ingest_hc_events.py --dry-run
python Zenoti\zconnect\hc_ingestion_manual\ingest_hc_events.py --check-db
python Zenoti\zconnect\hc_ingestion_manual\ingest_hc_events.py --csv path\to\reviews.csv
python Zenoti\zconnect\hc_ingestion_manual\ingest_hc_events.py --self-check
```

### Manual Feedback Metrics

Previously downloaded feedback metrics CSVs can be loaded separately using
`TABLE_HC_FEEDBACK`. This loader replaces rows by `center_id` and `date`:

```powershell
python Zenoti\zconnect\hc_ingestion_manual\ingest_feedback_metrics.py "Zenoti\sample csv\zconnect_feedbacks_all_2025-01-01_2026-10-26.csv" --dry-run
python Zenoti\zconnect\hc_ingestion_manual\ingest_feedback_metrics.py "Zenoti\sample csv\zconnect_feedbacks_all_2025-01-01_2026-10-26.csv"
```

## Call Log ID Capacity

HyperConnect can return composite call IDs containing a call SID and a GUID,
which are 71 characters long. A `CALL_ID` column sized at 50 or 64 characters
rejects those rows before the loader writes anything. Keep the full ID: it is
the key used to replace each call on a later import.

The schema helper widens an existing `varchar` or `nvarchar` `CALL_ID` to 128
characters while preserving its type, nullability and indexes. It leaves
columns already at least 128 characters, including `MAX`, alone. Preview and
apply the fix from the repository root:

```powershell
python Zenoti\zconnect\hc_sql_helper\fix_hc_tables.py --report call_logs
python Zenoti\zconnect\hc_sql_helper\fix_hc_tables.py --report call_logs --apply
```

Then load the retained CSV on the machine where the failed sync ran, passing
its actual path:

```powershell
python Zenoti\zconnect\hc_sql_helper\import_hc_report.py --report call_logs --csv path\to\call_logs.csv
```

## Verification

After running `stock_inventory.py`, check the load with:

```bash
python dbquery/inventory_table_check.py          # newest snapshot
python dbquery/inventory_table_check.py 2026-07-31  # specific date
```

This script reports:
- How many snapshots are in the table and when each was loaded
- Whether key columns like `product_code`, `on_hand_quantity`, and `date_inventory` are populated
- Sample rows to eyeball

A successful load shows:

```
Most recent 1 snapshot(s) by date_inventory:
  2026-07-31      3,515 rows   loaded 2026-08-01 09:15:23.456

Checking snapshot 2026-07-31: 3,515 rows
  Source file(s):
    current_stock_2026-07-31.csv: 3,515 rows

  Populated values per key column:
    center_name                      3,515 / 3,515  (100.0%)
    product_code                     3,515 / 3,515  (100.0%)
    product_name                     3,515 / 3,515  (100.0%)
    on_hand_quantity                 3,515 / 3,515  (100.0%)
    date_inventory                   3,515 / 3,515  (100.0%)

Looks good - 3,515 rows loaded with all key columns populated.
```

If `date_inventory` shows `<-- ALL NULL`, the filename pattern was not recognized. Rename and reload.
