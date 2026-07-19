from typing import Literal

from pydantic import BaseModel


class ResumeRequest(BaseModel):
    decision: Literal["approved", "rejected"]

