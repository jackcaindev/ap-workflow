from typing import Literal

from pydantic import BaseModel, Field


class TriageDecision(BaseModel):
    route: Literal["auto_resolve", "escalate_standard", "escalate_priority"]
    reasoning: str
    confidence: float = Field(ge=0, le=1)
