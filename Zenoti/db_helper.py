"""Shared database helpers for the Zenoti backup ingest scripts.

Centralizes ODBC connection handling so every script connects the same way and
stays resilient to ODBC driver-version differences across machines. Callers are
expected to have already loaded the .env (every ingest script calls
load_dotenv() at startup) before invoking these functions.
"""
import os
import time
import pyodbc

# Preference order used when ODBC_DRIVER is not set explicitly in the .env.
# Newer drivers first; "SQL Server" is the legacy built-in last-resort fallback.
_PREFERRED_DRIVERS = (
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "SQL Server",
)


def pick_odbc_driver():
    """Return ODBC_DRIVER from the env if set, else the newest installed SQL Server driver."""
    installed = pyodbc.drivers()
    preferred = os.getenv("ODBC_DRIVER")
    if preferred:
        if preferred not in installed:
            raise RuntimeError(
                f"ODBC_DRIVER='{preferred}' is not installed. Installed drivers: {installed}"
            )
        return preferred
    for candidate in _PREFERRED_DRIVERS:
        if candidate in installed:
            return candidate
    raise RuntimeError(f"No SQL Server ODBC driver found. Installed drivers: {installed}")


def build_conn_str():
    """Build a pyodbc connection string from environment variables.

    Reads SERVER, DATABASE, DB_USER, DB_PASSWORD plus the optional ODBC_DRIVER,
    ENCRYPT and TRUST_SERVER_CERTIFICATE settings. Encrypt defaults to "yes"
    (required by Azure SQL); set TRUST_SERVER_CERTIFICATE=yes for local servers
    that use a self-signed certificate.
    """
    server = os.getenv("SERVER")
    database = os.getenv("DATABASE")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    missing = [name for name, value in {
        "SERVER": server,
        "DATABASE": database,
        "DB_USER": user,
        "DB_PASSWORD": password,
    }.items() if not value]
    if missing:
        raise ValueError(f"Missing environment variables in .env file: {', '.join(missing)}")

    return (
        f"DRIVER={{{pick_odbc_driver()}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"Encrypt={os.getenv('ENCRYPT', 'yes')};"
        f"TrustServerCertificate={os.getenv('TRUST_SERVER_CERTIFICATE', 'no')};"
    )


# SQLSTATEs that mean "transient connectivity/timeout" -- worth retrying.
# Deliberately EXCLUDES auth (28000 login failed) and 08004 (server actively
# rejected, e.g. IP firewall) so those fail fast instead of being retried.
_TRANSIENT_SQLSTATES = {"08001", "08S01", "HYT00", "HYT01"}
# Azure SQL transient error numbers (serverless resume, throttling, failover).
_TRANSIENT_AZURE_CODES = ("40613", "40197", "40501", "49918", "49919",
                          "49920", "4060", "10928", "10929", "40143", "233")


def _is_transient_connect_error(err):
    """True if a pyodbc error looks like a transient connectivity/timeout issue."""
    sqlstate = err.args[0] if err.args else ""
    if sqlstate in _TRANSIENT_SQLSTATES:
        return True
    text = str(err)
    return any(code in text for code in _TRANSIENT_AZURE_CODES)


def get_connection(timeout=None):
    """Open a pyodbc connection, retrying transient connect failures.

    Transient connectivity/timeout errors (e.g. SQLSTATE 08001 "server not
    found / login timeout expired") are retried with exponential backoff so a
    brief network blip does not abort a long ingest run. Non-transient errors
    (bad password, firewall rejection) raise immediately. Tunable via .env:
    DB_LOGIN_TIMEOUT (default 30s/attempt), DB_CONNECT_RETRIES (default 3),
    DB_CONNECT_BACKOFF (default 2s base).
    """
    conn_str = build_conn_str()
    login_timeout = timeout if timeout is not None else int(os.getenv("DB_LOGIN_TIMEOUT", "30"))
    retries = int(os.getenv("DB_CONNECT_RETRIES", "3"))
    backoff = float(os.getenv("DB_CONNECT_BACKOFF", "2"))
    attempt = 0
    while True:
        try:
            return pyodbc.connect(conn_str, timeout=login_timeout)
        except pyodbc.Error as err:
            attempt += 1
            if attempt > retries or not _is_transient_connect_error(err):
                raise
            wait = backoff * (2 ** (attempt - 1))
            sqlstate = err.args[0] if err.args else "?"
            print(f"DB connect failed (SQLSTATE {sqlstate}); "
                  f"retry {attempt}/{retries} in {wait:.0f}s...", flush=True)
            time.sleep(wait)
