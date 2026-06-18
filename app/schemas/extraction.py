from typing import Literal

from pydantic import BaseModel, Field, field_validator


DocumentType = Literal[
    "invoice",
    "rate_confirmation",
    "bill_of_lading",
    "proof_of_delivery",
]


class LineItem(BaseModel):
    description: str
    quantity: float | None
    unit_price: float | None
    total: float


class InvoiceExtraction(BaseModel):
    invoice_number: str
    carrier_name: str
    load_number: str | None
    invoice_date: str
    total_amount: float
    line_items: list[LineItem]
    doc_type: Literal["invoice", "rate_confirmation"]
    confidence: float = Field(ge=0, le=1)

    @field_validator("total_amount")
    @classmethod
    def reject_negative_total_amount(cls, value: float) -> float:
        if value < 0:
            raise ValueError("total_amount cannot be negative")
        return value

    @field_validator("carrier_name")
    @classmethod
    def normalize_carrier_name(cls, value: str) -> str:
        # Normalize early so downstream matching/deduplication can compare
        # carrier names without repeating whitespace/case cleanup.
        return value.strip().upper()


class BOLExtraction(BaseModel):
    bol_number: str
    load_number: str | None
    carrier_name: str
    shipper_name: str
    consignee_name: str
    pickup_date: str
    commodity_description: str
    pieces: int | None
    weight_lbs: float | None
    doc_type: Literal["bill_of_lading"]

    @field_validator("carrier_name")
    @classmethod
    def normalize_carrier_name(cls, value: str) -> str:
        return value.strip().upper()


class PODExtraction(BaseModel):
    bol_number: str | None
    load_number: str | None
    carrier_name: str
    delivery_date: str
    delivery_time: str | None
    pieces_received: int | None
    condition: str
    receiver_name: str | None
    doc_type: Literal["proof_of_delivery"]

    @field_validator("carrier_name")
    @classmethod
    def normalize_carrier_name(cls, value: str) -> str:
        return value.strip().upper()


ExtractionResult = InvoiceExtraction | BOLExtraction | PODExtraction
