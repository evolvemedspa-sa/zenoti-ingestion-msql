# Orders API ingestion

The standalone Railway service is now in `Zenoti/PO_Transfer`. Its entry point
is `PO_Transfer/run_all.py`, and it has its own Dockerfile, API/database helpers,
requirements and center list. See [PO_Transfer/README.md](PO_Transfer/README.md)
for Railway settings, variables and date precedence.

The service runner defaults to the table in `TABLE_PO_AND_TRANSFERS`. Select
`--table-env TABLE_PO_AND_TRANSFER_TEST` for TEST loads. The old
`Zenoti/po_and_transfers_api.py` path remains a compatibility entry point with
its TEST default. Product-detail tables are not loaded here.

```powershell
.\.venv\Scripts\python.exe Zenoti\po_and_transfers_api.py --check-table
.\.venv\Scripts\python.exe Zenoti\po_and_transfers_api.py --start-date 2025-01-01 --end-date yesterday
```

The scheduled default is the last 31 completed New York days, ending yesterday.
Explicit dates override this range. All 20 configured centers and all four
criteria (ordered, raised, delivered and completed) are included by default.
An older order delivered inside the range is therefore included. The filters
select orders; the returned fields are their current Zenoti values.

## Field mapping

| Orders Listing field | SQL column | API interpretation |
| --- | --- | --- |
| REF # | ref_no | Numeric order number, without PO/TO prefix |
| Order on | order_on | ordered_date, with raised_date as a fallback |
| Deliver on | deliver_on | Latest partial delivery date |
| To | order_to | Destination center |
| From | order_from | Vendor/source center |
| Status | order_status | Current API status |
| Qty | order_qty | Sum of retail and consumable quantities |
| Value | order_value | Sum of quantity multiplied by unit price |
| Tax | order_tax | Sum of line-item tax |
| PO notes | order_ponotes | Unique nonempty partial notes, joined by newlines |
| Wait time | wait_time | Calendar days between order/delivery dates for DELIVERED purchase orders |

CREATED orders use ordered quantities, prices and tax. Other statuses use the
delivered fields. Delivered unit prices already incorporate the verified line
discounts. Order/delivery timestamps keep the CSV's minute precision.
`date_inserted` records this load in New York time, as in the API detail loaders.

## SQL behavior

Rows match on `ref_no`: existing orders are updated and new orders are inserted.
IDs are preserved on updates. If the TEST table's `id` is a regular integer
instead of an identity, new IDs are allocated under a transaction/table lock.
Duplicate references in the destination stop the load. No rows are pruned when
they are absent from a date-filtered pull.

All requests must succeed before the SQL load starts. The complete SQL batch
is committed only after its row count and all mapped values are verified.
SQL failures roll back the entire batch.

## Saved data and resuming

New outputs are under `Zenoti/PO_Transfer/data/api_order_ingestion/<start>_to_<end>/`
or the configured `PO_AND_TRANSFERS_OUTPUT_DIR`. The already-completed backfill
remains under `Zenoti/old/api_order_ingestion/2025-01-01_to_2026-11-11/`.

- Orders Listing CSV, for review.
- Raw API JSON, for replay.
- SQL load report, with insert/update counts and verification results.
- `_requests/`, with one checkpoint for each completed API window.

The historical range is split into windows of at most 31 days. API quota and
connectivity errors are retried. After an interrupted run, use the **same fixed
date range** and `--resume` to reuse completed windows:

```powershell
.\.venv\Scripts\python.exe Zenoti\po_and_transfers_api.py --start-date 2025-01-01 --end-date 2026-11-11 --resume
```

Omit `--resume` for a fresh API pull. To load an already saved complete snapshot
without API requests:

```powershell
.\.venv\Scripts\python.exe Zenoti\po_and_transfers_api.py --from-json Zenoti\old\api_order_ingestion\2025-01-01_to_2026-11-11\orders_2025-01-01_to_2026-11-11_api.json
```

Add `--no-load` to export only, or `--workers 1` to reduce concurrent API calls.

## Validation already performed

The November 10, 2026 source CSV has 161 orders. The verified API projection
finds all 161 references: 160 match all 11 fields, and PO4907 has its later
delivery/status update. CREATED order PO5008 retains its value of 1,338.00.

A rolled-back SQL integration check inserted those 161 CSV orders and then
upserted their API values. The second pass inserted zero additional rows,
preserved every ID, updated PO4907 from RAISED to DELIVERED, and retained all
32 populated PO notes. All mapped SQL values matched the staged input.

The January 1, 2025 through November 11, 2026 backfill completed all 3,520 API
windows and committed 4,170 TEST rows: 3,219 purchase orders and 951 transfers.
SQL verification found zero differences, and an independent read confirmed
the committed row count. The full pull also contains all 161 baseline CSV
references, with the same 160 complete matches and PO4907 delivery update.

PO2946 and PO3069 have November 9 deliveries, and PO4907 has its November 11
delivery. The returned API data contains no dated deliveries on November 6,
including partial deliveries with received quantities.
