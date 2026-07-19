from typing import Literal

from pydantic import BaseModel


CheckOutcome = Literal["passed", "failed", "not_evaluated"]


class ReconciliationCheck(BaseModel):
    check_name: str
    outcome: CheckOutcome
    details: str
    reason_code: str | None = None
