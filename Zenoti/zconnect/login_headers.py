"""Standalone Zenoti probe: log in, capture the dashboard's Bearer token, and
write it straight into .env as HC_TOKEN.

Self-contained - imports nothing from miner.py. Uses a fresh cookie-free
browser context on purpose, so the login exchange itself is captured instead
of being skipped by cookies.json.

Only XHR/fetch calls carrying `authorization: Bearer AN:evolvemedspa|...` are
considered. The first one found supplies the token; the "Bearer " prefix is
stripped (_client.py adds it back) and HC_TOKEN=<token> is written into the
.env that _client.py reads, replacing any previous HC_TOKEN line. No JSON file
is produced.

    python login_headers.py
    python login_headers.py --headed            # watch the browser
    python login_headers.py --show              # also print the full token
    python login_headers.py --no-env            # print only, leave .env alone
    python login_headers.py --match ""          # consider every call, not just zconnect
    python login_headers.py --selftest          # parser self-check

Credentials come from the first env name set out of PROBE_USER / MINER_USER /
ZENOTI_USERNAME / ZENOTI_USER / USERNAME (and the matching password names), or
from --user/--password. The .env is looked for next to the script, one level
up, and in the current directory, under either `.env` or `env`.
"""
import argparse
import os
import random
import time

from playwright.sync_api import sync_playwright

# Works from any project layout: the script may sit at the repo root or in a
# scripts/ subfolder, and the dotenv file may be named `.env` or `env`.
_HERE = os.path.dirname(os.path.abspath(__file__))
_ENV_CANDIDATES = (
    os.path.join(_HERE, ".env"),
    os.path.join(_HERE, "env"),
    os.path.join(os.path.dirname(_HERE), ".env"),
    os.path.join(os.getcwd(), ".env"),
)

try:
    from dotenv import load_dotenv

    seen = set()
    for _candidate in _ENV_CANDIDATES:
        # cwd and the parent folder are often the same directory; loading one
        # file twice would just print the same banner twice.
        key = os.path.normcase(os.path.abspath(_candidate))
        if key in seen or not os.path.exists(_candidate):
            continue
        seen.add(key)
        load_dotenv(_candidate)
        print(f"Loaded env: {_candidate}")
except ImportError:
    pass

# Credential names differ per repo; take the first that is set.
USER_ENV_NAMES = ("PROBE_USER", "MINER_USER", "ZENOTI_USERNAME", "ZENOTI_USER", "USERNAME")
PASSWORD_ENV_NAMES = ("PROBE_PASSWORD", "MINER_PASSWORD", "ZENOTI_PASSWORD", "PASSWORD")

ADMIN_URL = os.getenv("PROBE_URL", "https://evolvemedspa.zenoti.com/Admin/Admin.aspx")
DASH_URL = os.getenv(
    "PROBE_DASH_URL", "https://evolvemedspa.zenoti.com/Admin/Reports/ReportsDashboard.aspx"
)

# Resolved in main() from --user/--password, then the env names above.
USERNAME = None
PASSWORD = None

LOGIN_MAX_ATTEMPTS = 3
LOGIN_RETRY_BACKOFF = 5

# The ZConnect console's own API calls (apiamrs12.cu.zenoti.com/v1/zconnect/...).
# Everything the Admin dashboard fires is noise for this probe, so the run is
# scoped to these by default; --match overrides, --match "" keeps everything.
DEFAULT_MATCH = "zconnect"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

USERNAME_SEL = (
    "input#Username, input[name='Username'], input[name='username'], input[type='email']"
)


def _safe(fn):
    """Playwright raises for fields the driver does not expose; a missing
    field must not kill the capture."""
    try:
        return fn()
    except Exception:
        return None


def record_failure(req):
    """A request that never produced a response - connection reset, aborted,
    blocked. Without this the capture silently shows fewer requests than the
    browser actually made."""
    return {
        "status": None,
        "method": req.method,
        "url": req.url,
        "resource_type": _safe(lambda: req.resource_type),
        "failure": _safe(lambda: req.failure),
        "request_headers": _safe(req.all_headers) or dict(req.headers),
    }


def login(page):
    """Fill and submit the Zenoti login form. Retries because the login form is
    slow to settle and the submit button starts disabled behind a captcha."""
    last_error = None

    for attempt in range(1, LOGIN_MAX_ATTEMPTS + 1):
        print(f"Login attempt {attempt}/{LOGIN_MAX_ATTEMPTS}...")
        try:
            page.goto(ADMIN_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_selector(USERNAME_SEL, state="visible", timeout=30000)

            page.locator(USERNAME_SEL).first.click()
            page.locator(USERNAME_SEL).first.fill("")
            page.locator(USERNAME_SEL).first.press_sequentially(USERNAME, delay=50)
            time.sleep(random.uniform(0.5, 1.5))

            page.locator("#Password").click()
            page.locator("#Password").fill("")
            page.locator("#Password").press_sequentially(PASSWORD, delay=50)
            time.sleep(random.uniform(0.5, 1.5))
            time.sleep(2)

            try:
                page.locator("#btnLogin").click(timeout=10000)
            except Exception:
                print("  Button disabled (captcha pending). Forcing submit via JS...")
                page.evaluate("document.getElementById('btnLogin').removeAttribute('disabled')")
                page.evaluate("document.getElementById('btnLogin').click()")
            print("  Submitted.")

            page.wait_for_url("**/Admin/**", timeout=30000)
            print("  Login successful.")
            return
        except Exception as e:
            last_error = e
            print(f"  Attempt {attempt} failed: {e}")
            if attempt == LOGIN_MAX_ATTEMPTS:
                break
            backoff = LOGIN_RETRY_BACKOFF * attempt
            print(f"  Retrying in {backoff}s...")
            time.sleep(backoff)
            if "/Admin/" in page.url:
                print("  Already authenticated after reload.")
                return

    raise Exception(f"Login failed after {LOGIN_MAX_ATTEMPTS} attempts: {last_error}")


AUTH_HEADER = "authorization"


def _bearer_value(call):
    """The `Bearer AN:...` value if this call carries one, else None. The header
    name is matched case-insensitively; the scheme is matched as written, since
    that is what the API expects to receive back."""
    for key, value in call["request_headers"].items():
        if key.lower() == AUTH_HEADER and value.startswith("Bearer "):
            return value
    return None


BEARER_PREFIX = "Bearer "
TOKEN_PREFIX = "AN:"  # Zenoti admin tokens look like "AN:<account>|..."


def token_from_calls(calls):
    """The bare token (no "Bearer ") from the first call that carries one."""
    for call in calls:
        value = _bearer_value(call)
        if value:
            token = value[len(BEARER_PREFIX):].strip()
            if token.startswith(TOKEN_PREFIX):
                return token
    return None


def env_target():
    """The dotenv file _client.py reads: .env next to this script, or `env` if
    that is the one that exists. Created as .env when neither does."""
    plain = os.path.join(_HERE, ".env")
    alt = os.path.join(_HERE, "env")
    return alt if (os.path.exists(alt) and not os.path.exists(plain)) else plain


def upsert_env(path, name, value):
    """Set NAME=value in a dotenv file, replacing an existing line in place and
    appending otherwise. Every other line is left byte-for-byte alone."""
    lines = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as handle:
            lines = handle.read().splitlines()
    entry = f"{name}={value}"
    for index, line in enumerate(lines):
        if line.lstrip().startswith(f"{name}=") or line.lstrip().startswith(f"export {name}="):
            lines[index] = entry
            break
    else:
        lines.append(entry)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def record_request(req):
    """An XHR/fetch the dashboard fired - this is where the Bearer token lives.
    Same shape whether it is written to the .txt or the .json."""
    return {
        "method": req.method,
        "url": req.url,
        "resource_type": _safe(lambda: req.resource_type),
        "request_headers": _safe(req.all_headers) or dict(req.headers),
        "post_data": _safe(lambda: req.post_data),
        "response": None,
    }


def _selftest():
    class FakeReq:
        method = "POST"
        resource_type = "document"
        url = "https://x/Login/Login.aspx"
        failure = "net::ERR_CONNECTION_RESET"
        post_data = '{"a":1}'
        headers = {"Referer": "https://x/Login/Login.aspx"}

        def all_headers(self):
            return self.headers

    f = record_failure(FakeReq())
    assert f["status"] is None and f["url"].endswith("Login.aspx")
    assert f["failure"] == "net::ERR_CONNECTION_RESET"

    r = record_request(FakeReq())
    assert r["request_headers"]["Referer"].endswith("Login.aspx")
    assert r["response"] is None and r["post_data"] == '{"a":1}'
    assert _bearer_value(r) is None

    # A non-Bearer authorization (Basic, or a bare token) must not pass.
    basic = record_request(FakeReq())
    basic["request_headers"]["Authorization"] = "Basic abc"
    assert _bearer_value(basic) is None

    bearer = record_request(FakeReq())
    bearer["request_headers"]["Authorization"] = "Bearer AN:x|y"
    assert _bearer_value(bearer) == "Bearer AN:x|y"
    # The token is returned bare, without the "Bearer " prefix.
    assert token_from_calls([r, basic, bearer]) == "AN:x|y"
    assert token_from_calls([{"request_headers": {"Authorization": "Basic abc"}}]) is None

    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        env = os.path.join(tmp, ".env")
        with open(env, "w", encoding="utf-8") as handle:
            handle.write("A=1\nHC_TOKEN=old\nB=2\n")
        upsert_env(env, "HC_TOKEN", "new")
        assert open(env, encoding="utf-8").read() == "A=1\nHC_TOKEN=new\nB=2\n"
        upsert_env(env, "OTHER", "x")
        assert open(env, encoding="utf-8").read().endswith("B=2\nOTHER=x\n")
    print("selftest ok")


def capture_token(headed=False, match=DEFAULT_MATCH, user=None, password=None, url=None):
    """Log in through a fresh browser and return the bare token (the part after
    "Bearer ", starting AN:evolvemedspa|...). Raises SystemExit if no
    credentials are available or no Bearer call was seen."""
    global USERNAME, PASSWORD, ADMIN_URL

    if url:
        ADMIN_URL = url
    USERNAME = user or next((os.getenv(n) for n in USER_ENV_NAMES if os.getenv(n)), None)
    PASSWORD = password or next((os.getenv(n) for n in PASSWORD_ENV_NAMES if os.getenv(n)), None)

    if not USERNAME or not PASSWORD:
        raise SystemExit(
            "No credentials. Pass --user/--password, or set one of these in the .env: "
            f"{', '.join(USER_ENV_NAMES)} / {', '.join(PASSWORD_ENV_NAMES)}"
        )

    print(f"Target: {ADMIN_URL}\nUser: {USERNAME}")

    calls = []
    calls_by_request = {}
    seen_calls = set()
    failures = []

    with sync_playwright() as pw:
        launch = {
            "headless": not headed,
            "args": [
                "--start-maximized",
                "--disable-blink-features=AutomationControlled",
                "--ignore-certificate-errors",
            ],
        }
        if os.getenv("RAILWAY_ENVIRONMENT") is None:
            launch["channel"] = "chrome"
        browser = pw.chromium.launch(**launch)

        # No storage_state: fresh session, so the login responses are visible.
        context = browser.new_context(no_viewport=True, user_agent=UA)
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            "window.chrome = {runtime: {}};"
        )
        page = context.new_page()

        def on_request(req):
            """The dashboard's own API calls - where the Bearer token lives."""
            if _safe(lambda: req.resource_type) not in ("xhr", "fetch"):
                return
            if match and match.lower() not in req.url.lower():
                return
            call = record_request(req)
            # Only calls carrying `authorization: Bearer ...` matter here.
            if not _bearer_value(call):
                return
            # The dashboard polls the same endpoint repeatedly; one block each.
            key = (call["method"], call["url"])
            if key in seen_calls:
                return
            seen_calls.add(key)
            calls.append(call)
            calls_by_request[req] = call

        def on_response(resp):
            call = calls_by_request.get(resp.request)
            if call is not None:
                call["response"] = {
                    "status": resp.status,
                    "response_headers": _safe(resp.all_headers) or dict(resp.headers),
                }

        def on_request_failed(req):
            if not match or match.lower() in req.url.lower():
                failures.append(record_failure(req))

        page.on("request", on_request)
        page.on("response", on_response)
        page.on("requestfailed", on_request_failed)

        login(page)
        # Nothing is printed between here and the token. The dashboard's own API
        # calls are what carry the Bearer header, so the page has to load before
        # anything can be captured, and the URLs that come back name centers and
        # employees -- they belong in the trace, not the log.
        _safe(lambda: page.goto(DASH_URL, wait_until="networkidle", timeout=120000))
        page.wait_for_timeout(5000)

        context.close()
        browser.close()

    token = token_from_calls(calls)
    if not token:
        # The counts are here rather than on the happy path: they are what says
        # whether the dashboard never loaded at all or the match is too narrow.
        raise SystemExit(
            f"No 'Bearer {TOKEN_PREFIX}...' token captured from {len(calls)} matching "
            f"call(s); {len(failures)} request(s) failed. Try --headed, or --match \"\" "
            "to look beyond zconnect calls."
        )
    return token


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--match",
        default=DEFAULT_MATCH,
        help=f"keep only calls whose URL contains this text "
        f"(default: {DEFAULT_MATCH!r}; pass \"\" to keep every call)",
    )
    ap.add_argument("--headed", action="store_true", help="show the browser window")
    ap.add_argument("--selftest", action="store_true", help="run the parser self-check and exit")
    ap.add_argument("--url", help="login page to start from (default: the Zenoti admin login)")
    ap.add_argument("--user", help="username, if it is not in the .env")
    ap.add_argument("--password", help="password, if it is not in the .env")
    ap.add_argument("--show", action="store_true", help="print the full token instead of a masked preview")
    ap.add_argument("--no-env", action="store_true", help="do not write HC_TOKEN into .env; just print it")
    args = ap.parse_args()

    if args.selftest:
        _selftest()
        return

    token = capture_token(
        headed=args.headed, match=args.match, user=args.user,
        password=args.password, url=args.url,
    )
    print(f"\nToken: {token if args.show else token[:24] + '...'}")
    if args.no_env:
        print("--no-env: .env left untouched")
    else:
        target = env_target()
        upsert_env(target, "HC_TOKEN", token)
        print(f"HC_TOKEN written to {target}")


if __name__ == "__main__":
    main()