import base64
import binascii
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class PermanentJobError(ValueError):
    """A queue entry cannot become valid by retrying it."""


class InvoiceJobEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    idempotency_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    gmail_account: str = Field(min_length=1)
    message_id: str = Field(min_length=1)
    gmail_thread_id: str = ""
    mime_part_id: str = Field(min_length=1)
    filename: str = Field(min_length=1)
    file_bytes: str = Field(min_length=1, max_length=35_000_000)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    enqueued_at: datetime

    @model_validator(mode="after")
    def validate_content(self) -> "InvoiceJobEnvelope":
        if Path(self.filename).suffix.lower() not in {".pdf", ".png", ".jpg", ".jpeg"}:
            raise ValueError("filename has an unsupported attachment extension")
        if self.enqueued_at.tzinfo is None:
            raise ValueError("enqueued_at must include a timezone")
        try:
            decoded = base64.b64decode(self.file_bytes, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("file_bytes must be valid base64") from exc
        if hashlib.sha256(decoded).hexdigest() != self.content_sha256:
            raise ValueError("content_sha256 does not match file_bytes")
        return self

    def decoded_file_bytes(self) -> bytes:
        return base64.b64decode(self.file_bytes, validate=True)
