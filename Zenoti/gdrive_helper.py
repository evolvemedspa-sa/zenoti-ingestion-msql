import json
import os
import tempfile
import requests
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SCOPES = ["https://www.googleapis.com/auth/drive"]
DRIVE_API = "https://www.googleapis.com/drive/v3/files"


def _get_authed_session(credentials_json=None, credentials_file=None):
    if credentials_json:
        try:
            info = json.loads(credentials_json)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"GDRIVE_CREDENTIALS_JSON is not valid JSON: {e}. "
                f"First 50 chars: {credentials_json[:50]!r}"
            )
        required = {"client_email", "token_uri", "private_key", "type"}
        missing = required - set(info.keys())
        if missing:
            raise ValueError(
                f"GDRIVE_CREDENTIALS_JSON missing fields: {missing}. "
                f"Keys found: {list(info.keys())}"
            )
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=SCOPES
        )
    elif credentials_file:
        creds_path = credentials_file
        if not os.path.isabs(creds_path):
            creds_path = os.path.join(os.path.dirname(__file__), creds_path)

        if not os.path.exists(creds_path):
            raise FileNotFoundError(
                f"Service account credentials file not found: {creds_path}"
            )

        creds = service_account.Credentials.from_service_account_file(
            creds_path, scopes=SCOPES
        )
    else:
        raise ValueError(
            "Provide GDRIVE_CREDENTIALS_JSON (JSON string) or GDRIVE_CREDENTIALS_FILE (file path)"
        )

    creds.refresh(Request())
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {creds.token}"
    return session


UPLOAD_API = "https://www.googleapis.com/upload/drive/v3/files"


def get_drive_service():
    """Get Drive service using OAuth user credentials (personal account fallback)."""
    token_json = os.getenv("GOOGLE_TOKEN_JSON")
    if not token_json:
        print("GOOGLE_TOKEN_JSON not set. Skipping fallback upload.")
        return None

    creds_info = json.loads(token_json)
    creds = Credentials.from_authorized_user_info(creds_info)

    if creds.expired and creds.refresh_token:
        print("Refreshing Google OAuth token...")
        creds.refresh(Request())
        print("Token refreshed.")

    return build("drive", "v3", credentials=creds)


def _upload_via_oauth(local_path, folder_id):
    """Upload using personal OAuth credentials via googleapiclient."""
    service = get_drive_service()
    if not service:
        return None

    filename = os.path.basename(local_path)
    metadata = {"name": filename, "parents": [folder_id]}
    media = MediaFileUpload(local_path, resumable=True)
    result = service.files().create(body=metadata, media_body=media, fields="id,name").execute()
    print(f"Uploaded to Drive (OAuth fallback): {result['name']} (id: {result['id']})")
    return result["id"]


def upload_file_to_gdrive(local_path, folder_id, credentials_json=None, credentials_file=None):
    """Upload a file to a Google Drive folder. Returns the file ID."""
    if not folder_id:
        raise ValueError("Google Drive folder ID is required for upload.")

    session = _get_authed_session(
        credentials_json=credentials_json, credentials_file=credentials_file
    )

    filename = os.path.basename(local_path)
    metadata = {"name": filename, "parents": [folder_id]}

    boundary = "zenoti_upload_boundary"
    meta_json = json.dumps(metadata)

    with open(local_path, "rb") as f:
        file_content = f.read()

    body = (
        f"--{boundary}\r\n"
        f"Content-Type: application/json; charset=UTF-8\r\n\r\n"
        f"{meta_json}\r\n"
        f"--{boundary}\r\n"
        f"Content-Type: text/plain\r\n\r\n"
    ).encode("utf-8") + file_content + f"\r\n--{boundary}--".encode("utf-8")

    resp = session.post(
        UPLOAD_API,
        params={"uploadType": "multipart", "fields": "id,name"},
        headers={"Content-Type": f"multipart/related; boundary={boundary}"},
        data=body,
    )

    if resp.status_code == 403 or (resp.status_code != 200 and "storageQuota" in resp.text):
        print(f"Service account upload failed (storage limit). Trying OAuth fallback...")
        fallback_id = _upload_via_oauth(local_path, folder_id)
        if fallback_id:
            return fallback_id
        print("OAuth fallback also failed.")

    if resp.status_code != 200:
        print(f"Upload failed ({resp.status_code}): {resp.text}")
    resp.raise_for_status()
    result = resp.json()
    print(f"Uploaded to Drive: {result['name']} (id: {result['id']})")
    return result["id"]


def list_csv_filenames(folder_id, credentials_json=None, credentials_file=None):
    """Return list of CSV filenames in a Google Drive folder (no download)."""
    if not folder_id:
        return []

    session = _get_authed_session(
        credentials_json=credentials_json, credentials_file=credentials_file
    )

    query = f"'{folder_id}' in parents and mimeType='text/csv' and trashed=false"
    resp = session.get(
        DRIVE_API,
        params={"q": query, "fields": "files(name)", "orderBy": "name"},
    )
    resp.raise_for_status()
    return [f["name"] for f in resp.json().get("files", [])]


def get_csv_from_gdrive(folder_id, credentials_json=None, credentials_file=None):
    """Download all CSV files from a Google Drive folder to a temp directory.

    Returns the temp directory path (or single file path if only one CSV).
    """
    if not folder_id:
        raise ValueError("Google Drive folder ID is not set. Check your .env file.")

    session = _get_authed_session(
        credentials_json=credentials_json, credentials_file=credentials_file
    )

    query = f"'{folder_id}' in parents and mimeType='text/csv' and trashed=false"
    resp = session.get(
        DRIVE_API,
        params={"q": query, "fields": "files(id,name)", "orderBy": "name"},
    )
    resp.raise_for_status()
    files = resp.json().get("files", [])

    if not files:
        print(f"SKIP: No CSV files found in Google Drive folder: {folder_id}")
        return None

    download_dir = tempfile.mkdtemp(prefix="zenoti_gdrive_")

    for f in files:
        dl_resp = session.get(
            f"{DRIVE_API}/{f['id']}", params={"alt": "media"}, stream=True
        )
        dl_resp.raise_for_status()
        local_path = os.path.join(download_dir, f["name"])
        with open(local_path, "wb") as fh:
            for chunk in dl_resp.iter_content(chunk_size=8192):
                fh.write(chunk)
    print(f"Downloaded {len(files)} file(s) from Drive")

    if len(files) == 1:
        return os.path.join(download_dir, files[0]["name"])

    return download_dir
