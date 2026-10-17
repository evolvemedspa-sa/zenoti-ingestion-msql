"""Zenoti access token, minted from the account's app credentials.

POST /v1/tokens  ->  access_token + refresh_token

The apikey in .env authenticates the ZConnect routes but cannot be rotated or
refreshed. This exchanges the account's own app credentials for a bearer token
instead, so the bearer stops being something a human pastes in every day.

Credentials, all from .env:

    ZENOTI_ACCOUNT_NAME    account slug -- "evolvemedspa", the part before
                           .zenoti.com in the admin hostname
    ZENOTI_ADMIN_USER      employee login
    ZENOTI_ADMIN_PASSWORD  that employee's password
    ZENOTI_APP_ID          application GUID
    ZENOTI_APP_SECRET      application secret
    ZENOTI_DEVICE_ID       optional; generated once and then kept stable

app_id / app_secret come from Zenoti (Settings > Applications). The token lasts
24 hours, so it is cached in token_cache.json and only re-minted when the cached
one is within an hour of expiring -- repeated runs cost one call a day.

    python access_token.py             reuse the cached token, minting it if stale
    python access_token.py --show      print the token alone, for piping
    python access_token.py --force     mint a new one regardless
    python access_token.py --check     prove it works against /v1/centers
    python access_token.py --selftest  offline checks, no network

Other scripts can `import access_token` and call `access_token.access_token()`.

This file must NOT be called token.py: it would shadow the stdlib `token`
module, which `linecache` imports through `tokenize` on every interpreter start.

token_cache.json holds a live credential -- treat it exactly like .env.
"""
import argparse
import json
import os
import sys
import time
import uuid

import requests

import _client as zc

TOKEN_PATH = "/tokens"

# The documented lifetime is 24 hours. Refresh an hour early so a long run can
# never have its token expire underneath it mid-sweep.
TOKEN_TTL_SECONDS = 23 * 3600

CACHE_NAME = "token_cache.json"

REQUIRED = (
    "ZENOTI_ACCOUNT_NAME",
    "ZENOTI_ADMIN_USER",
    "ZENOTI_ADMIN_PASSWORD",
    "ZENOTI_APP_ID",
    "ZENOTI_APP_SECRET",
)


def credentials(device_id):
    """The /v1/tokens request body, or a ZConnectError naming what is missing."""
    missing = [name for name in REQUIRED if not os.getenv(name)]
    if missing:
        raise zc.ZConnectError(
            "missing from .env: "
            + ", ".join(missing)
            + " -- needs the account slug, an employee login, and the app id "
              "and secret from Zenoti > Settings > Applications"
        )
    return {
        "account_name": os.getenv("ZENOTI_ACCOUNT_NAME"),
        "user_name": os.getenv("ZENOTI_ADMIN_USER"),
        "password": os.getenv("ZENOTI_ADMIN_PASSWORD"),
        "grant_type": "password",
        "app_id": os.getenv("ZENOTI_APP_ID"),
        "app_secret": os.getenv("ZENOTI_APP_SECRET"),
        "device_id": device_id,
    }


def _post(path, body):
    """POST to a /v1/tokens route.

    Unlike every other route here, these answer a plain-text error body --
    "Invalid account" with a 401 -- rather than the {"error": {...}} object
    check_error knows how to translate. So a rejection arrives as a bare
    HTTPError and has to be turned into something readable here.
    """
    try:
        return zc.request("POST", path, json=body)
    except requests.exceptions.HTTPError as exc:
        raise zc.ZConnectError(
            f"{exc.response.status_code} from {path}: {exc.response.text.strip()[:200]}"
        )


def _record(payload, device_id):
    """The cache entry for one token response.

    The token sits under `Credentials`; a bare object is accepted too, since the
    wrapper is the one part of the shape not worth being strict about. Expiry is
    NOT taken from the response -- the format is undocumented, and timing it from
    the moment we minted is both simpler and safe.
    """
    if not isinstance(payload, dict):
        raise zc.ZConnectError(f"unexpected token response: {str(payload)[:200]}")
    creds = payload.get("Credentials") or payload
    token = creds.get("access_token")
    if not token:
        raise zc.ZConnectError(f"no access_token in the token response: {str(payload)[:200]}")
    return {
        "access_token": token,
        "refresh_token": creds.get("refresh_token"),
        "token_type": creds.get("token_type") or "bearer",
        "device_id": device_id,
        "obtained_at": int(time.time()),
    }


def _is_fresh(cached):
    """True while a cached token is still worth reusing."""
    obtained = cached.get("obtained_at")
    return isinstance(obtained, int) and (time.time() - obtained) < TOKEN_TTL_SECONDS


def _cache_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), CACHE_NAME)


def _load_cache():
    try:
        with open(_cache_path(), encoding="utf-8") as handle:
            cached = json.load(handle)
    except (OSError, ValueError):
        return {}
    return cached if isinstance(cached, dict) else {}


def _save_cache(record):
    with open(_cache_path(), "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=2)


def access_token(force=False):
    """A usable access token, minting one if the cache is empty or stale."""
    cached = {} if force else _load_cache()
    if _is_fresh(cached):
        return cached["access_token"]

    # Keep the device id stable across mints -- it is part of what the token is
    # bound to, so a fresh one every run is asking for trouble.
    device_id = cached.get("device_id") or os.getenv("ZENOTI_DEVICE_ID") or str(uuid.uuid4())
    record = _record(_post(TOKEN_PATH, credentials(device_id)), device_id)
    _save_cache(record)
    return record["access_token"]


def _selftest():
    """The freshness decision and the credential check -- a wrong guess in either
    is silent, so they are pinned down here."""
    now = int(time.time())
    assert _is_fresh({"obtained_at": now}), "a just-minted token must be reused"
    assert not _is_fresh({"obtained_at": now - TOKEN_TTL_SECONDS - 1}), "an expired token must not be reused"
    assert not _is_fresh({}), "an empty cache is not fresh"
    assert not _is_fresh({"obtained_at": "not an int"}), "a malformed timestamp is not fresh"

    record = _record({"Credentials": {"access_token": "t", "refresh_token": "r"}}, "dev")
    assert record["access_token"] == "t" and record["refresh_token"] == "r"
    assert abs(record["obtained_at"] - now) <= 2
    assert record["token_type"] == "bearer", "a missing token_type defaults to bearer"
    # The unwrapped shape, and a missing refresh_token, both have to work.
    assert _record({"access_token": "t"}, "dev")["access_token"] == "t"
    assert _record({"Credentials": {"access_token": "t"}}, "dev")["refresh_token"] is None

    for bad in ({}, {"Credentials": {}}, {"Credentials": {"access_token": None}}, "not a dict", None):
        try:
            _record(bad, "dev")
        except zc.ZConnectError:
            pass
        else:
            raise AssertionError(f"accepted a token response with no token: {bad!r}")

    saved = {name: os.environ.get(name) for name in REQUIRED}
    try:
        for name in REQUIRED:
            os.environ.pop(name, None)
        try:
            credentials("dev")
        except zc.ZConnectError as exc:
            assert "ZENOTI_ACCOUNT_NAME" in str(exc), exc
        else:
            raise AssertionError("credentials() accepted an empty environment")

        for name in REQUIRED:
            os.environ[name] = "x"
        body = credentials("dev")
        assert body["grant_type"] == "password", body
        assert body["account_name"] == "x" and body["device_id"] == "dev", body
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    print("token self-check ok")


def main():
    parser = argparse.ArgumentParser(
        description="Mint a Zenoti access token from the app credentials in .env."
    )
    parser.add_argument("--show", action="store_true", help="print only the token, for piping")
    parser.add_argument("--force", action="store_true", help="mint a new token even if the cached one is valid")
    parser.add_argument("--check", action="store_true", help="prove the token works against /v1/centers")
    parser.add_argument("--selftest", action="store_true", help="run the offline self-check and exit")
    args = parser.parse_args()

    if args.selftest:
        _selftest()
        return

    token = access_token(force=args.force)
    if args.show:
        print(token)
        return

    remaining = TOKEN_TTL_SECONDS - (time.time() - _load_cache().get("obtained_at", 0))
    print(f"token {token[:12]}... -- usable for about {remaining / 3600:.1f}h more")
    print(f"cached in {CACHE_NAME}, which holds a live credential: treat it like .env")

    if args.check:
        # A per-request header beats the session's apikey, so this really does
        # test the bearer rather than the key.
        try:
            payload = zc.request("GET", "/centers", headers={"Authorization": f"bearer {token}"})
        except requests.exceptions.HTTPError as exc:
            raise SystemExit(
                f"token rejected: {exc.response.status_code} {exc.response.text.strip()[:200]}"
            )
        centers = payload.get("centers") or payload.get("requested_centers") or []
        print(f"token accepted -- {len(centers)} centers visible")


if __name__ == "__main__":
    try:
        main()
    except zc.ZConnectError as exc:
        sys.exit(f"error: {exc}")
