"""Capture one Railway run and upload its concise log to Google Drive."""

import os
import sys
import tempfile
from pathlib import Path

LOG_FILENAME = 'po_and_transfer_logs.txt'

if __package__:
    from .gdrive_helper import upload_file_to_gdrive
else:
    from gdrive_helper import upload_file_to_gdrive


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
        oauth_token = os.getenv('GOOGLE_TOKEN_JSON')
        if not folder_id:
            print('  Log      Drive upload skipped (GDRIVE_PARENT_FOLDER is not set).', flush=True)
            return False
        if not credentials_json and not credentials_file and not oauth_token:
            print('  Log      Drive upload skipped (credentials are not set).', flush=True)
            return False

        try:
            self._handle.flush()
            upload_file_to_gdrive(
                str(self.path), folder_id,
                credentials_json=credentials_json,
                credentials_file=credentials_file,
            )
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
