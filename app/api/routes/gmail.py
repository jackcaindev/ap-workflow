from pathlib import Path

from fastapi import APIRouter, Request

from app.core.config import get_settings
from app.services.gmail_poller import poll_inbox


router = APIRouter(prefix="/gmail", tags=["gmail"])


@router.get("/auth")
async def gmail_auth() -> dict[str, str]:
    settings = get_settings()
    token_path = Path(settings.GMAIL_TOKEN_PATH)
    if token_path.exists() and token_path.stat().st_size > 0:
        return {"status": "authenticated"}
    return {
        "status": "not_authenticated",
        "message": "Run the OAuth flow locally to generate token.json",
    }


@router.post("/poll")
async def gmail_poll(request: Request) -> dict[str, int]:
    processed_count = await poll_inbox(
        getattr(request.app.state, "runtime_health", None)
    )
    return {"count": processed_count}
