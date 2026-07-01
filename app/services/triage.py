import json
from typing import Any

from anthropic import AsyncAnthropic
from pydantic import ValidationError

from app.core.config import get_settings
from app.schemas.triage import TriageDecision
from app.services.extraction import CLAUDE_MODEL, ExtractionError, extract_response_text
from app.services.json_utils import strip_code_fences


TRIAGE_SYSTEM_PROMPT = """You triage freight accounts payable reconciliation exceptions before human review.
Return JSON only. Do not include markdown, code fences, explanations, or surrounding text.

Classify each exception into exactly one route:

- auto_resolve: high-confidence formatting or rounding noise only. Examples: carrier name
  variance that clearly refers to the same entity (e.g. ACME Freight vs ACME FRT LLC),
  amount differences under $5 or under 1% of the invoice total.

- escalate_standard: normal human review with no urgency signal. Use this for most exceptions
  when neither auto_resolve nor escalate_priority criteria apply.

- escalate_priority: material dollar variance (over $100 or over 5% of the agreed rate), or a
  missing/overdue proof of delivery beyond a reasonable grace period, or other signals that
  require prompt attention.

You are pre-labeling only. Every exception still goes to human review regardless of route.
Provide concise reasoning and a confidence score between 0 and 1.
"""

TRIAGE_USER_PROMPT = """Triage this reconciliation exception.

Return JSON with exactly these fields: route, reasoning, confidence.
route must be one of: auto_resolve, escalate_standard, escalate_priority.
"""


def validate_triage_json(response_text: str) -> TriageDecision:
    cleaned = strip_code_fences(response_text)
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ExtractionError(
            f"Claude returned non-JSON triage output: {response_text!r}"
        ) from exc

    try:
        return TriageDecision.model_validate(payload)
    except ValidationError as exc:
        raise ExtractionError("Claude JSON did not match the triage schema") from exc


async def triage_exception(
    *,
    exception_reason: str | None,
    extraction: dict[str, Any] | None,
    match_result: dict[str, Any] | None,
) -> TriageDecision:
    settings = get_settings()
    if not settings.ANTHROPIC_API_KEY:
        raise ExtractionError("ANTHROPIC_API_KEY is not configured")

    context = {
        "exception_reason": exception_reason,
        "extraction": extraction,
        "match_result": match_result,
    }

    client = AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)
    message = await client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=512,
        temperature=0,
        system=TRIAGE_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": f"{TRIAGE_USER_PROMPT}\n\nContext:\n{json.dumps(context, default=str)}",
                    }
                ],
            }
        ],
    )

    return validate_triage_json(extract_response_text(message.content))
