from pathlib import Path
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from app.core.config import get_settings


SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]


def get_gmail_service(*, allow_interactive: bool = True) -> Any:
    settings = get_settings()
    credentials_path = Path(settings.GMAIL_CREDENTIALS_PATH)
    token_path = Path(settings.GMAIL_TOKEN_PATH)
    creds: Credentials | None = None
    if token_path.exists() and token_path.stat().st_size > 0:
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not allow_interactive:
                raise RuntimeError("No Gmail token found — run /gmail/auth first")
            if not credentials_path.exists():
                raise FileNotFoundError(
                    f"Gmail OAuth credentials file not found: {credentials_path}"
                )
            # credentials.json is the OAuth client secret downloaded from Google
            # Cloud Console. We load it rather than generating it because Google
            # must issue the client ID/secret for the configured OAuth app.
            flow = InstalledAppFlow.from_client_secrets_file(
                str(credentials_path),
                SCOPES,
            )
            creds = flow.run_local_server(port=0)

        # token.json stores refresh/access tokens after the first consent flow so
        # normal poll cycles can authenticate without reopening the browser.
        token_path.write_text(creds.to_json(), encoding="utf-8")

    return build("gmail", "v1", credentials=creds)
