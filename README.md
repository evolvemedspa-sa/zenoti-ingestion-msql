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
