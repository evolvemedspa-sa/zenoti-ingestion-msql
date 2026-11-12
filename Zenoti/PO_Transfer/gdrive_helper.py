"""Google Drive upload helper with service-account and OAuth fallback."""

import json
import os

import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


SCOPES = ['https://www.googleapis.com/auth/drive']
UPLOAD_API = 'https://www.googleapis.com/upload/drive/v3/files'


def _get_authed_session(credentials_json=None, credentials_file=None):
    if credentials_json:
        try:
            info = json.loads(credentials_json)
        except json.JSONDecodeError as error:
            raise ValueError('GDRIVE_CREDENTIALS_JSON is not valid JSON.') from error
        required = {'client_email', 'token_uri', 'private_key', 'type'}
        missing = required - set(info)
        if missing:
            raise ValueError(
                f'GDRIVE_CREDENTIALS_JSON is missing field(s): {", ".join(sorted(missing))}')
        credentials = service_account.Credentials.from_service_account_info(
            info, scopes=SCOPES)
    elif credentials_file:
        path = credentials_file
        if not os.path.isabs(path):
            path = os.path.join(os.path.dirname(__file__), path)
        credentials = service_account.Credentials.from_service_account_file(
            path, scopes=SCOPES)
    else:
        raise ValueError('Google Drive credentials are not configured.')

    credentials.refresh(Request())
    session = requests.Session()
    session.headers['Authorization'] = f'Bearer {credentials.token}'
    return session


def get_drive_service():
    """Build the personal OAuth client used by the existing pipeline fallback."""
    token_json = os.getenv('GOOGLE_TOKEN_JSON')
    if not token_json:
        return None
    credentials = Credentials.from_authorized_user_info(json.loads(token_json))
    if credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
    return build('drive', 'v3', credentials=credentials, cache_discovery=False)


def _upload_via_oauth(local_path, folder_id):
    service = get_drive_service()
    if service is None:
        return None
    metadata = {'name': os.path.basename(local_path), 'parents': [folder_id]}
    media = MediaFileUpload(local_path, mimetype='text/plain', resumable=True)
    result = service.files().create(
        body=metadata, media_body=media, fields='id,name',
        supportsAllDrives=True,
    ).execute()
    return result['id']


def upload_file_to_gdrive(local_path, folder_id, credentials_json=None,
                          credentials_file=None):
    """Upload a file, falling back to the pipeline's personal OAuth token."""
    if not folder_id:
        raise ValueError('Google Drive parent folder is not configured.')

    if not credentials_json and not credentials_file:
        fallback_id = _upload_via_oauth(local_path, folder_id)
        if fallback_id:
            return fallback_id
        raise ValueError('Google Drive credentials are not configured.')

    session = _get_authed_session(credentials_json, credentials_file)
    boundary = 'zenoti_upload_boundary'
    metadata = {'name': os.path.basename(local_path), 'parents': [folder_id]}
    with open(local_path, 'rb') as handle:
        content = handle.read()
    body = (
        f'--{boundary}\r\n'
        'Content-Type: application/json; charset=UTF-8\r\n\r\n'
        f'{json.dumps(metadata)}\r\n'
        f'--{boundary}\r\n'
        'Content-Type: text/plain; charset=UTF-8\r\n\r\n'
    ).encode('utf-8') + content + f'\r\n--{boundary}--'.encode('utf-8')
    response = session.post(
        UPLOAD_API,
        params={
            'uploadType': 'multipart', 'fields': 'id,name',
            'supportsAllDrives': 'true',
        },
        headers={'Content-Type': f'multipart/related; boundary={boundary}'},
        data=body,
        timeout=60,
    )

    if response.status_code in (403, 404) or 'storageQuota' in response.text:
        fallback_id = _upload_via_oauth(local_path, folder_id)
        if fallback_id:
            return fallback_id
    if response.status_code == 404:
        raise ValueError(
            'parent Drive folder was not found by either configured Drive account')
    if response.status_code == 403:
        raise ValueError(
            'configured Drive accounts cannot write to the parent folder')
    response.raise_for_status()
    return response.json()['id']
