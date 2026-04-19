from pydantic import BaseModel, Field
from enum import Enum
from typing import List, Optional

class Exchange(str, Enum):
    binance = "binance"
    bybit = "bybit"
    okx = "okx"
    kucoin = "kucoin"
    mexc = "mexc"
    bitget = "bitget"
    gate = "gate"
    htx = "htx"
    bingx = "bingx"


class FilterSchema(BaseModel):
    exchanges: List[Exchange]
    min_volume: int = Field(ge=100)
    max_volume: int = Field(le=5000)
    gap: int = Field(ge=50)
    
class FilterUpdateSchema(BaseModel):
    exchanges: Optional[List[Exchange]]
    min_volume: Optional[int] = Field(ge=100)
    max_volume: Optional[int] = Field(le=5000)
    gap: Optional[int] = Field(ge=50)
    
#ge = больше или равно le = меньше или равно