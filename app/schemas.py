from pydantic import BaseModel

class HoldingCreate(BaseModel):
    symbol: str
    quantity: float
    avg_price: float