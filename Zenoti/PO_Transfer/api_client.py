"""Shared inventory API/authentication helpers for the standalone PO service."""

import argparse
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

if __package__:
    from .config import load_environment
else:
    from config import load_environment

load_environment()
NY_TZ = ZoneInfo('America/New_York')


def parse_ymd(text, flag):
    try:
        return datetime.strptime(str(text).strip(), '%Y-%m-%d').date()
    except ValueError as error:
        raise argparse.ArgumentTypeError(f'{flag} must look like YYYY-MM-DD, got {text!r}') from error


def split_range(start, end, max_days=31):
    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=max_days - 1), end)
        yield cursor, window_end
        cursor = window_end + timedelta(days=1)


def split_table_name(table):
    if '.' in table:
        return tuple(part.strip(' []') for part in table.split('.', 1))
    return 'dbo', table.strip(' []')


def load_centers_file():
    path = Path(os.getenv('ZENOTI_CENTERS_FILE') or Path(__file__).with_name('centers.json'))
    if not path.is_file():
        raise ValueError(f'Centers file not found: {path}')
    payload = json.loads(path.read_text(encoding='utf-8'))
    if isinstance(payload, dict):
        payload = payload.get('centers') or payload.get('list') or []
    if not isinstance(payload, list):
        raise ValueError(f'{path} must contain a list of centers.')
    centers, seen = [], set()
    for entry in payload:
        if isinstance(entry, str):
            center_id, name = entry.strip(), ''
        elif isinstance(entry, dict):
            center_id = str(entry.get('center_id') or entry.get('id') or '').strip()
            name = str(entry.get('center_name') or entry.get('name') or '').strip()
        else:
            continue
        if center_id and center_id not in seen:
            seen.add(center_id)
            centers.append((center_id, name))
    return centers


def resolve_centers(values, all_centers=False):
    explicit = list(dict.fromkeys(part.strip() for value in values
                                 for part in str(value).split(',') if part.strip()))
    if explicit:
        names = dict(load_centers_file())
        return [(center_id, names.get(center_id, '')) for center_id in explicit]
    centers = load_centers_file()
    if centers:
        return centers
    default = (os.getenv('ZENOTI_CENTER_ID') or '').strip()
    if default and not all_centers:
        return [(default, '')]
    raise ValueError('No Zenoti centers are configured.')


def build_session():
    key = (os.getenv('ZENOTI_API_KEY') or '').strip()
    if not key:
        raise ValueError('ZENOTI_API_KEY is not set.')
    authorization = key if key.lower().startswith('apikey ') else f'apikey {key}'
    session = requests.Session()
    session.headers.update({'Authorization': authorization, 'Content-Type': 'application/json',
                            'Accept': 'application/json'})
    return session


class InventoryEndpoint:
    PAGE_SIZE = 100
    MAX_PAGES = 200
    REQUEST_TIMEOUT = 120
    MAX_RETRIES = 3
    RETRY_BACKOFF_SECONDS = 5
    NY_TZ = NY_TZ
    parse_ymd = staticmethod(parse_ymd)
    split_range = staticmethod(split_range)
    split_table_name = staticmethod(split_table_name)
    resolve_centers = staticmethod(resolve_centers)
    build_session = staticmethod(build_session)

    def __init__(self, resource, list_keys):
        self.resource = resource
        self.list_keys = list_keys

    @property
    def ENDPOINT(self):
        base = (os.getenv('ZENOTI_API_BASE') or 'https://api.zenoti.com/v1').rstrip('/')
        return f'{base}/inventory/{self.resource}'

    def extract_rows(self, payload):
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return []
        for key in self.list_keys:
            value = payload.get(key)
            if isinstance(value, list):
                return value
        for value in payload.values():
            if isinstance(value, list) and (not value or isinstance(value[0], dict)):
                return value
        return []

    def request_with_retry(self, session, params):
        last_error = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                response = session.get(self.ENDPOINT, params=params, timeout=self.REQUEST_TIMEOUT)
            except requests.RequestException as error:
                last_error = f'{type(error).__name__}: {error}'
                if attempt == self.MAX_RETRIES:
                    break
                delay = self.RETRY_BACKOFF_SECONDS * attempt
                print(f'  {last_error} - retrying in {delay}s', flush=True)
                time.sleep(delay)
                continue
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as error:
                    raise ValueError(f'Zenoti returned non-JSON for {params}.') from error
            if response.status_code == 429 or response.status_code >= 500:
                last_error = f'HTTP {response.status_code}: {response.text[:300]}'
                if attempt == self.MAX_RETRIES:
                    break
                delay = self.RETRY_BACKOFF_SECONDS * attempt
                print(f'  {last_error} - retrying in {delay}s', flush=True)
                time.sleep(delay)
                continue
            raise ValueError(f'Zenoti rejected the request (HTTP {response.status_code}). '
                             f'Params: {params}; Response: {response.text[:1000]}')
        raise ValueError(f'Gave up after {self.MAX_RETRIES} attempts. Last error - {last_error}')


PURCHASE = InventoryEndpoint('purchase_orders', (
    'orders', 'purchase_orders', 'purchaseOrders', 'purchase_order', 'list', 'items', 'data',
))
TRANSFER = InventoryEndpoint('transfer_orders', (
    'orders', 'transfer_orders', 'transferOrders', 'transfer_order', 'list', 'items', 'data',
))
