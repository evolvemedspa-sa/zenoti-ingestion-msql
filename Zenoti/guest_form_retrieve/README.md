# Guest form retrieval

Place this folder directly inside `Zenoti/`, next to the existing `zconnect/`.
The scripts load the shared `Zenoti/.env`; deployed environment variables take
precedence. No credentials or copies of the ZConnect helpers belong in this
folder.

## Service forms

The default run calls the existing `zconnect/login_headers.py` helper to capture
a fresh admin session token, then uses it as `HC_TOKEN` for the regional Forms
Review endpoint. The token stays in memory, matching the ZConnect sync's login
flow. It is not written to a token cache or `.env`.

From `Zenoti/`:

```powershell
python guest_form_retrieve/service_forms.py --start-date 2026-11-05 --end-date 2026-11-06
```

To reuse `HC_TOKEN` already supplied through the shared environment:

```powershell
python guest_form_retrieve/service_forms.py --auth web --start-date 2026-11-05 --end-date 2026-11-06
```

`--auth auto` tries the supplied token first and captures a fresh token if the
report rejects it. `--auth login` (the default) always captures a fresh token.
The login helper uses the existing project credential names, including
`MINER_USER` and `MINER_PASSWORD`. Its local browser is Google Chrome; on
Railway it uses the bundled Playwright Chromium browser.

The export requests all centers and all review/submission states by default,
preserves one CSV row per API record, and writes diagnostics to standard output.
It creates no response JSON files. Both date bounds are inclusive.

## Guest-level form answers

```powershell
python guest_form_retrieve/retrieve_guest_forms.py --start-date 2026-11-05 --end-date 2026-11-06
```

This script uses `ZENOTI_API_KEY` from the same shared environment. It selects
guests through appointments in the inclusive date range, then fetches their
guest-level form answers. It does not associate those answers with a particular
service form instance or filter by form submission dates.

Appointments are fetched once per center and date window, without `page` or
`size` parameters. Centers still use pagination. Guest API requests log each
attempt before sending it, then log the response status or error and elapsed
time. Logs flush immediately. `ZENOTI_CONNECT_TIMEOUT` and
`ZENOTI_READ_TIMEOUT` set positive timeouts in seconds, defaulting to 10 and 30.

Without date flags, both scripts use `ZENOTI_START_DATE` and `ZENOTI_END_DATE`,
falling back to seven days ago through today. CSV files are written to the
working directory; the service report also accepts `--csv`.

## Dependencies and Railway

```powershell
python -m pip install -r guest_form_retrieve/requirements.txt
```

Install dependencies into the same Python environment that runs the script.
For VS Code using the repository's `.venv`, run these from the repository root:

```powershell
& .\.venv\Scripts\python.exe -m pip install -r Zenoti\guest_form_retrieve\requirements.txt
& .\.venv\Scripts\python.exe Zenoti\guest_form_retrieve\service_forms.py
```

Missing browser dependencies are reported separately from missing credentials,
with the Python interpreter path included in the error.

For a dedicated Railway service, use root directory `Zenoti` and Dockerfile path
`guest_form_retrieve/Dockerfile`. Supply the existing login credentials and
`ZENOTI_API_KEY` through Railway variables. The image installs Chromium and
copies only the scripts and shared helpers; it does not copy `.env` files.
The default container command runs the service form export. The guest form
export can be run with a separate start command using the same image.

Regression checks run without contacting Zenoti or opening a browser. From the
repository root:

```powershell
& .\.venv\Scripts\python.exe -m unittest discover -s Zenoti/guest_form_retrieve/tests -v
```
