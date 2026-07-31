import subprocess
import sys
import os
from datetime import datetime

from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path)

SCRIPTS = [
    "sql_helper.py",
    "appointments.py",
    "block_out_update.py",
    "cost_of_goods.py",
    "cash.py",
    "memberships.py",
    "stock_ledger.py",
    # "stock_inventory.py",  # disabled
    "business_kpi_v2.py",
    "fb_ads.py",
    "google_ads.py",
]

script_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(script_dir, "logs")
os.makedirs(log_dir, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"sql_ingestion_logs_{timestamp}.txt"
log_path = os.path.join(log_dir, log_filename)

start_time = datetime.now()
all_passed = True
failed_script = None


def log(msg):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def log_raw(text):
    print(text, end="", flush=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(text)


log(f"Zenoti SQL Ingestion Pipeline — {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

for script in SCRIPTS:
    path = os.path.join(script_dir, script)
    log(f"[START] {script}")

    script_start = datetime.now()

    result = subprocess.run(
        [sys.executable, path],
        cwd=script_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    if result.stdout:
        log_raw(result.stdout)
        if not result.stdout.endswith("\n"):
            log_raw("\n")

    elapsed = (datetime.now() - script_start).total_seconds()

    if result.returncode != 0:
        log(f"[FAILED] {script} (exit code {result.returncode}, {elapsed:.1f}s)")
        all_passed = False
        failed_script = script
        break
    else:
        log(f"[DONE] {script} ({elapsed:.1f}s)")

total_elapsed = (datetime.now() - start_time).total_seconds()

if all_passed:
    log(f"Pipeline complete. Total time: {total_elapsed:.1f}s")
else:
    log(f"Pipeline stopped at {failed_script}. Total time: {total_elapsed:.1f}s")

# Upload log to Google Drive
parent_folder = os.getenv("GDRIVE_PARENT_FOLDER")
credentials_json = os.getenv("GDRIVE_CREDENTIALS_JSON")
credentials_file = os.getenv("GDRIVE_CREDENTIALS_FILE", "service_account.json")

if parent_folder and (credentials_json or credentials_file):
    try:
        from gdrive_helper import upload_file_to_gdrive

        log(f"Uploading log to Google Drive...")
        upload_file_to_gdrive(
            log_path,
            parent_folder,
            credentials_json=credentials_json,
            credentials_file=credentials_file,
        )
        log(f"Log uploaded: {log_filename}")
    except Exception as e:
        log(f"Log upload failed: {e}")
else:
    log("GDRIVE_PARENT_FOLDER not set, skipping log upload.")

if not all_passed:
    sys.exit(1)
