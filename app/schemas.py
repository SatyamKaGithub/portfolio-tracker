from pydantic import BaseModel, field_validator
from datetime import date as dt_date
from typing import Optional, Literal


class HoldingCreate(BaseModel):
    symbol: str
    quantity: float
    avg_price: float


class TransactionCreate(BaseModel):
    symbol: str
    quantity: float
    price: float
    type: Literal["BUY", "SELL"]
    date: Optional[dt_date] = None

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol is required")
        return normalized

    @field_validator("quantity", "price")
    @classmethod
    def validate_positive_numbers(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("quantity and price must be greater than zero")
        return value

    @field_validator("type", mode="before")
    @classmethod
    def normalize_type(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("date")
    @classmethod
    def validate_date(cls, value: Optional[dt_date]) -> Optional[dt_date]:
        if value and value > dt_date.today():
            raise ValueError("transaction date cannot be in the future")
        return value
