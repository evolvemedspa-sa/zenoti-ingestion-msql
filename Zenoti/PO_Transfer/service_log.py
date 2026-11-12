"""Capture one Railway run and upload its concise log to Google Drive."""

import json
import os
import sys
import tempfile
from pathlib import Path

import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account


LOG_FILENAME = 'po_and_transfer_logs.txt'
DRIVE_UPLOAD_API = 'https://www.googleapis.com/upload/drive/v3/files'
DRIVE_SCOPE = 'https://www.googleapis.com/auth/drive'


class _Tee:
    def __init__(self, stream, handle):
        self._stream = stream
        self._handle = handle

    def write(self, data):
        written = self._stream.write(data)
        self._handle.write(data)
        return written

    def flush(self):
        self._stream.flush()
        self._handle.flush()

    def __getattr__(self, name):
        return getattr(self._stream, name)


class LogSession:
    def __init__(self, path, handle, stdout, stderr):
        self.path = Path(path)
        self._handle = handle
        self._stdout = stdout
        self._stderr = stderr

    def upload(self):
        """Upload to GDRIVE_PARENT_FOLDER without changing the job result."""
        folder_id = os.getenv('GDRIVE_PARENT_FOLDER')
        credentials_json = os.getenv('GDRIVE_CREDENTIALS_JSON')
        credentials_file = os.getenv('GDRIVE_CREDENTIALS_FILE')
        if not folder_id:
            print('  Log      Drive upload skipped (GDRIVE_PARENT_FOLDER is not set).', flush=True)
            return False
        if not credentials_json and not credentials_file:
            print('  Log      Drive upload skipped (credentials are not set).', flush=True)
            return False

        try:
            self._handle.flush()
            credentials = _credentials(credentials_json, credentials_file)
            credentials.refresh(Request())
            metadata = {'name': LOG_FILENAME, 'parents': [folder_id]}
            boundary = 'po_transfer_log_upload'
            body = (
                f'--{boundary}\r\n'
                'Content-Type: application/json; charset=UTF-8\r\n\r\n'
                f'{json.dumps(metadata)}\r\n'
                f'--{boundary}\r\n'
                'Content-Type: text/plain; charset=UTF-8\r\n\r\n'
            ).encode('utf-8') + self.path.read_bytes() + f'\r\n--{boundary}--'.encode('utf-8')
            response = requests.post(
                DRIVE_UPLOAD_API,
                params={'uploadType': 'multipart', 'fields': 'id,name'},
                headers={
                    'Authorization': f'Bearer {credentials.token}',
                    'Content-Type': f'multipart/related; boundary={boundary}',
                },
                data=body,
                timeout=60,
            )
            response.raise_for_status()
            print(f'  Log      uploaded to Drive: {LOG_FILENAME}', flush=True)
            return True
        except Exception as error:  # best effort: SQL/API outcome remains authoritative
            print(f'  Log      Drive upload failed: {error}', flush=True)
            return False

    def close(self):
        """Restore console streams and remove the local temporary log."""
        sys.stdout.flush()
        sys.stderr.flush()
        sys.stdout = self._stdout
        sys.stderr = self._stderr
        self._handle.close()
        try:
            self.path.unlink(missing_ok=True)
            self.path.parent.rmdir()
        except OSError:
            pass


def _credentials(credentials_json, credentials_file):
    if credentials_json:
        info = json.loads(credentials_json)
        return service_account.Credentials.from_service_account_info(
            info, scopes=[DRIVE_SCOPE])
    path = Path(credentials_file)
    if not path.is_absolute():
        path = Path(__file__).resolve().parent / path
    return service_account.Credentials.from_service_account_file(
        path, scopes=[DRIVE_SCOPE])


def start():
    """Tee stdout and stderr to a temporary po_and_transfer_logs.txt."""
    folder = Path(tempfile.mkdtemp(prefix='po_transfer_log_'))
    path = folder / LOG_FILENAME
    handle = path.open('w', encoding='utf-8')
    stdout, stderr = sys.stdout, sys.stderr
    session = LogSession(path, handle, stdout, stderr)
    sys.stdout = _Tee(stdout, handle)
    sys.stderr = _Tee(stderr, handle)
    return session
