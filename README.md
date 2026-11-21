# Zenoti Ingestion and Form Exports

This project loads report exports and Zenoti / HyperConnect API data into SQL Server or Azure SQL, and exports service forms and guest form answers to CSV. The workflow list below follows the current runners and their required helpers.

Open [Documentation.html](Documentation.html) for the standalone guide with workflow navigation, active-script search, copyable commands and print / PDF support.

## Current workflows

| Workflow | Entry point | Result |
| --- | --- | --- |
| Main ingestion | [`Zenoti/run_all.py`](Zenoti/run_all.py) | 15 ordered report and SQL steps |
| HyperConnect | [`Zenoti/zconnect/sync_hc.py`](Zenoti/zconnect/sync_hc.py) | Three enabled communication reports and SQL loads |
| PO / Transfer | [`Zenoti/PO_Transfer/run_all.py`](Zenoti/PO_Transfer/run_all.py) | Order summary plus purchase and transfer product details |
| Service forms | [`Zenoti/guest_form_retrieve/service_forms.py`](Zenoti/guest_form_retrieve/service_forms.py) | Forms Review CSV |
| Guest form answers | [`Zenoti/guest_form_retrieve/retrieve_guest_forms.py`](Zenoti/guest_form_retrieve/retrieve_guest_forms.py) | Guest answers selected through appointment dates |

## Local setup

Use Python 3.11 or newer. Commands below run from the repository root in PowerShell. Install Microsoft ODBC Driver 18 for SQL workflows and Google Chrome for local browser login.

```powershell
python -m venv .venv
& .\.venv\Scripts\python.exe -m pip install -r Zenoti\requirements.txt
```

Install the extra requirements for the workflows you run:

```powershell
& .\.venv\Scripts\python.exe -m pip install -r Zenoti\zconnect\requirements.txt
& .\.venv\Scripts\python.exe -m pip install -r Zenoti\PO_Transfer\requirements.txt
& .\.venv\Scripts\python.exe -m pip install -r Zenoti\guest_form_retrieve\requirements.txt
```

Configure `Zenoti/.env`; process environment variables take precedence. The PO service reads `Zenoti/PO_Transfer/.env` when present, otherwise the shared file. On Railway, supply settings through Variables.

- SQL: `SERVER`, `DATABASE`, `DB_USER`, `DB_PASSWORD` and the configured destination tables.
- Main CSV sources: `CSV_SOURCE=local` with report-specific `CSV_FILE_*`, or `CSV_SOURCE=gdrive` with `GDRIVE_FOLDER_*` and Drive credentials. Cost of goods uses the `COG` suffix.
- API workflows: `ZENOTI_API_KEY`. Both form scripts also load the shared API-key client.
- Browser login: `MINER_USER` and `MINER_PASSWORD`; Playwright must be installed in the interpreter that runs the command.
- Log upload: `GDRIVE_PARENT_FOLDER` and configured Drive credentials.

## Main SQL ingestion

```powershell
& .\.venv\Scripts\python.exe Zenoti\run_all.py
```

`run_all.py` sets each child working directory to `Zenoti/`, runs the following list in order, and stops when a child exits with a nonzero status:

| Order | Script | Purpose |
| --- | --- | --- |
| 1 | [`Zenoti/sql_helper.py`](Zenoti/sql_helper.py) | Clear recognized CSV date ranges for configured report tables before imports. |
| 2 | [`Zenoti/appointments.py`](Zenoti/appointments.py) | Load appointment report exports into SQL. |
| 3 | [`Zenoti/block_out_update.py`](Zenoti/block_out_update.py) | Synchronize attendance and block-out records using schedule identifiers. |
| 4 | [`Zenoti/cost_of_goods.py`](Zenoti/cost_of_goods.py) | Load cost-of-goods report transactions. |
| 5 | [`Zenoti/cash.py`](Zenoti/cash.py) | Load cash and payment exports used by downstream reporting. |
| 6 | [`Zenoti/memberships.py`](Zenoti/memberships.py) | Load membership sale exports. |
| 7 | [`Zenoti/employee_sales.py`](Zenoti/employee_sales.py) | Load employee sales exports with SQL type-aware binding. |
| 8 | [`Zenoti/feedback_report.py`](Zenoti/feedback_report.py) | Validate detailed feedback CSVs and replace fetched center/invoice pairs. |
| 9 | [`Zenoti/stock_ledger.py`](Zenoti/stock_ledger.py) | Load stock movement and ledger transactions. |
| 10 | [`Zenoti/business_kpi_v3.py`](Zenoti/business_kpi_v3.py) | Refresh KPI snapshots by their filename-derived business date. |
| 11 | [`Zenoti/fb_ads.py`](Zenoti/fb_ads.py) | Load Facebook advertising metrics using report business keys. |
| 12 | [`Zenoti/google_ads.py`](Zenoti/google_ads.py) | Load Google advertising metrics using report business keys. |
| 13 | [`Zenoti/update_zenoti_cohort.py`](Zenoti/update_zenoti_cohort.py) | Refresh the customer-spend source legs after the cash load. |
| 14 | [`Zenoti/stock_inventory.py`](Zenoti/stock_inventory.py) | Append stock snapshots and derive the inventory date from each filename. |
| 15 | [`Zenoti/get_guest_birthday_v3.py`](Zenoti/get_guest_birthday_v3.py) | Fetch guests selected by creation date, confirm missing birthdays, and refresh fetched SQL guest keys. |

`sql_helper.py` commits cleanup of recognized CSV windows before the report imports. Subsequent steps commit independently, so the runner is not one transaction. Earlier changes remain applied after a later failure. Some loaders skip missing input; review their output even when the runner completes.

The main log is `Zenoti/logs/sql_ingestion_logs_<timestamp>.txt`, with a best-effort Drive upload when configured. Detailed feedback replaces fetched `Center Name + Invoice No` pairs. Business KPI snapshots replace their parsed business date.

## HyperConnect

```powershell
& .\.venv\Scripts\python.exe Zenoti\zconnect\sync_hc.py
& .\.venv\Scripts\python.exe Zenoti\zconnect\sync_hc.py --lookback 1
& .\.venv\Scripts\python.exe Zenoti\zconnect\sync_hc.py --no-ingest
```

The enabled reports are:

| Report | Grain | SQL business key |
| --- | --- | --- |
| [`Zenoti/zconnect/agent_metrics.py`](Zenoti/zconnect/agent_metrics.py) | Agent / center / day | `agent_id + center_id + start_date + end_date + date` |
| [`Zenoti/zconnect/location_performance.py`](Zenoti/zconnect/location_performance.py) | Center / day | `center_id + start_date + end_date` |
| [`Zenoti/zconnect/call_logs.py`](Zenoti/zconnect/call_logs.py) | Individual call | `call_id` |

The runner captures one bearer token and passes `HC_TOKEN` to its children in memory. The default range is seven completed days ending yesterday; `--lookback 1` selects yesterday. Explicit `--start-date` / `--end-date` flags override the rolling range. The default Railway runner clock is UTC.

SQL loads delete and insert only the incoming business keys in one transaction. Keys absent from the pull remain. Successful loads remove their temporary CSV; failed loads keep the CSV in `Zenoti/zconnect/`. `--no-ingest` keeps all report CSVs. Use `--only`, `--skip`, `--stop-on-error` or `--hc-extras` for the enabled reports.

## Purchase and transfer orders

```powershell
& .\.venv\Scripts\python.exe Zenoti\PO_Transfer\run_all.py --dry-run
& .\.venv\Scripts\python.exe Zenoti\PO_Transfer\run_all.py
```

The runner fetches one shared API snapshot and loads three outputs:

| Output | Used script | Behavior |
| --- | --- | --- |
| Order summary | [`Zenoti/PO_Transfer/po_and_transfers_api.py`](Zenoti/PO_Transfer/po_and_transfers_api.py) | Upsert by ref_no; preserve existing summary IDs |
| Purchase product details | [`Zenoti/PO_Transfer/purchase_order.py`](Zenoti/PO_Transfer/purchase_order.py) | Replace products for fetched purchase orders |
| Transfer product details | [`Zenoti/PO_Transfer/transfer_order.py`](Zenoti/PO_Transfer/transfer_order.py) | Replace products for fetched transfer orders |

The default window is 31 completed days ending yesterday in `America/New_York`. Ordered, raised, delivered and completed criteria are fetched. All API windows complete before the summary and both details are verified and committed in one SQL transaction. A SQL failure rolls them back together; orders outside the pull remain.

Local snapshots, CSVs and checkpoints are kept under `Zenoti/PO_Transfer/data/api_order_ingestion/<start>_to_<end>/`. Railway removes temporary data after success or failure and uploads a concise service log. Use `--resume` for local backfills with retained checkpoints. `--dry-run` avoids API/SQL work but still creates the runner log and can upload it.

See [PO / Transfer README](Zenoti/PO_Transfer/README.md) for its service configuration, destination bindings and schema behavior.

## Form exports

```powershell
& .\.venv\Scripts\python.exe Zenoti\guest_form_retrieve\service_forms.py --start-date 2026-11-05 --end-date 2026-11-06
& .\.venv\Scripts\python.exe Zenoti\guest_form_retrieve\retrieve_guest_forms.py --start-date 2026-11-05 --end-date 2026-11-06
```

- `service_forms.py` exports the regional Forms Review report with 17 columns and one CSV row per API record. Default `--auth login` captures a fresh browser token; `--auth web` uses a supplied token and `--auth auto` tries a supplied token before logging in. It defaults to all centers and review/submission states.
- `retrieve_guest_forms.py` fetches appointments once per center / date window without `page` or `size`, then fetches each unique guest's answers once. Signatures are excluded, identified duplicate appointments are removed, and missing-ID appointments are retained.
- Both commands accept inclusive input dates. Guest appointment requests advance the final bound by one day and use windows of at most seven days. Guest dates select appointments, not form submission dates or a particular service-form instance.
- Guest requests log each attempt immediately. `ZENOTI_CONNECT_TIMEOUT` and `ZENOTI_READ_TIMEOUT` must be positive and default to 10 / 30 seconds per attempt.
- CSVs are written to the working directory. Default names are `zenoti_service_forms_<start>_<end>.csv` and `zenoti_guest_forms_<start>_<end>.csv`. Guest failures are recorded in an errors CSV, including runs with no recovered appointments. Service forms supports `--csv`.
- These two commands export CSVs; neither loads SQL.

See [Form export README](Zenoti/guest_form_retrieve/README.md) for optional authentication, host, request-body and deployment controls.

## Dates and stock inventory

| Workflow | Date selection | Notes |
| --- | --- | --- |
| Main CSV reports | Source filenames and report columns. | Cleanup recognizes YYYY-MM-DD_to_YYYY-MM-DD for configured report prefixes. |
| HyperConnect runner | Seven completed days ending yesterday. | --lookback 1 selects yesterday. The default Railway runner clock is UTC; call timestamps use UTC days. |
| PO / Transfer runner | 31 completed days ending yesterday in America/New_York. | CLI boundaries override fixed service/shared dates, then the rolling range. |
| Guest birthday step | Guests created in the previous two calendar days, ending yesterday. | Uses the process clock. --from / --to select creation dates, not birthday dates. |
| Service forms | Seven days ago through today, unless CLI/shared dates are supplied. | Both bounds inclusive; report requests send 00:00:00 through 23:59:59. |
| Guest form answers | Same inclusive input defaults as service forms. | Appointments use an exclusive end date, advanced one day. Windows contain at most seven days. |

Stock inventory keeps snapshot dates separate from load timestamps:

| Column | Meaning |
| --- | --- |
| `date_inventory` | Snapshot business date parsed from the filename. |
| `date_inserted` | Stock load time in America/New_York. |
| `ingestion_timestamp` | The same stock load time, where the target table has this column. |

Snapshot filenames can contain `YYYY-MM-DD`, `YYYY_MM_DD` or `YYYYMMDD`, for example `Current Stock_2026-07-31.csv`. Missing filename dates produce null `date_inventory` values. CSV, XLSX and XLSM inputs are supported; spreadsheets also need a compatible pandas Excel engine. A configured directory can supply multiple files.

`stock_inventory.py` appends a full snapshot on every run. Repeating a snapshot can create duplicates. All files in one run share the New York load timestamp; identify batches by snapshot date and insertion time.

## Required helpers

| Script | Purpose |
| --- | --- |
| [`Zenoti/zconnect/hc_sql_helper/import_hc_report.py`](Zenoti/zconnect/hc_sql_helper/import_hc_report.py) | Match CSV headers to live SQL column types and replace the business keys present in each incoming report. |
| [`Zenoti/zconnect/hc_sql_helper/csv_table.py`](Zenoti/zconnect/hc_sql_helper/csv_table.py) | Provide CSV normalization, coercion, column mapping and validation for the report importer. |
| [`Zenoti/zconnect/_client.py`](Zenoti/zconnect/_client.py) | Provide API-key and HyperConnect clients, retries, center resolution and CSV utilities. |
| [`Zenoti/zconnect/login_headers.py`](Zenoti/zconnect/login_headers.py) | Capture an admin bearer token through Playwright; runners use the token in memory. |
| [`Zenoti/PO_Transfer/order_listing_api.py`](Zenoti/PO_Transfer/order_listing_api.py) | Fetch and normalize the Orders Listing snapshot used by summary and detail loaders. |
| [`Zenoti/PO_Transfer/api_client.py`](Zenoti/PO_Transfer/api_client.py) | Provide inventory API authentication, request pacing, date splitting and center configuration. |
| [`Zenoti/PO_Transfer/detail_sql.py`](Zenoti/PO_Transfer/detail_sql.py) | Replace product rows for fetched orders inside the shared transaction. |
| [`Zenoti/PO_Transfer/config.py`](Zenoti/PO_Transfer/config.py) | Load the service environment, falling back to Zenoti/.env for local runs. |
| [`Zenoti/PO_Transfer/db_helper.py`](Zenoti/PO_Transfer/db_helper.py) | Provide SQL connection settings, driver selection and connection retries for the order service. |
| [`Zenoti/PO_Transfer/service_log.py`](Zenoti/PO_Transfer/service_log.py) | Capture service output and upload its concise run log to Google Drive. |
| [`Zenoti/PO_Transfer/gdrive_helper.py`](Zenoti/PO_Transfer/gdrive_helper.py) | Upload order-service logs using configured service-account or OAuth credentials. |
| [`Zenoti/guest_form_retrieve/_shared.py`](Zenoti/guest_form_retrieve/_shared.py) | Load the shared environment and import existing ZConnect helpers by file path. |
| [`Zenoti/db_helper.py`](Zenoti/db_helper.py) | Provide SQL connection strings, ODBC driver selection and transient connection retries. |
| [`Zenoti/gdrive_helper.py`](Zenoti/gdrive_helper.py) | List and download report CSVs from Google Drive and upload service logs. |
| [`Zenoti/service_log.py`](Zenoti/service_log.py) | Tee service output to a log file and upload it to the configured Drive folder. |

## Railway deployment

| Workflow | Root directory | Dockerfile path | Container command |
| --- | --- | --- | --- |
| `Main SQL ingestion` | `Zenoti` | `Dockerfile` | `python run_all.py` |
| `HyperConnect` | `Zenoti` | `zconnect/Dockerfile` | `python zconnect/sync_hc.py` |
| `PO / Transfer` | `Repository root (leave blank)` | `Zenoti/PO_Transfer/Dockerfile` | `python -u run_all.py` |
| `Form exports` | `Zenoti` | `guest_form_retrieve/Dockerfile` | `python guest_form_retrieve/service_forms.py` |

The SQL images install ODBC Driver 18. HyperConnect and form images install Chromium; local browser login uses Chrome. The form image defaults to the service report; use `python guest_form_retrieve/retrieve_guest_forms.py` as its start command for guest answers. Collect exported CSVs or use a persistent volume when they must survive an ephemeral container run. Railway cron schedules use UTC.

## Verification and troubleshooting

Form regression checks run with mocked API and browser calls:

```powershell
& .\.venv\Scripts\python.exe -m unittest discover -s Zenoti/guest_form_retrieve/tests -v
```

- Missing Playwright: install requirements into the interpreter path shown by the error.
- SQL connection errors: check Driver 18, connection settings and server reachability.
- Repeated 401 / 403: check fresh bearer login, report permission and regional host; API-key workflows need a valid API key.
- CSV locked by Excel: close it. Guest forms fall back to a timestamped path; service forms report the lock.
- Unexpected SKIP or empty output: check the logged dates, input files, filename patterns and configured Drive folders.
- Missing stock snapshot dates: use a recognized date in the filename.

Update this README and `Documentation.html` when an entry point or active run list changes.
