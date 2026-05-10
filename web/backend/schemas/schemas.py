from pydantic import BaseModel, Field, EmailStr, field_validator
from enum import Enum
from typing import List, Optional, Set

class Exchange(str, Enum):
    binance = "binance"
    bybit = "bybit"
    okx = "okx"
    kucoin = "kucoin"
    mexc = "mexc"
    bitget = "bitget"
    gateio = "gateio"
    htx = "htx"
    bingx = "bingx"


class FilterSchema(BaseModel):
    exchanges: List[Exchange]
    min_volume: int = Field(ge=100)
    max_volume: int = Field(le=5000)
    gap: int = Field(ge=50)
    spread: float = Field(ge=0.5)
    
class FilterUpdateSchema(BaseModel):
    exchanges: Optional[List[Exchange]] = None
    min_volume: Optional[int] = Field(default=None, ge=100)
    max_volume: Optional[int] = Field(default=None, le=5000)
    gap: Optional[int] = Field(default=None, ge=50)
    spread: Optional[float] = Field(default=None, ge=0.5)
    
class LoginRegisterSchema(BaseModel):
    email: EmailStr
    password: str



class BlacklistSchema(BaseModel):
    tokens: List[str]

    @field_validator('tokens')
    @classmethod
    def valid_data(cls, data):
        valid = list()
        
        for token in data:
            token = token.upper()

        
            # if not token.endswith('USDT'):
            #     raise ValueError(
            #         "Token must end with USDT"
            #     )
            
            if not token.endswith('USDT'):
                token+="USDT"
                valid.append(token)

            
            valid.append(token)

        
        return valid
    
    
    
class CandlesSchema(BaseModel):
    symbol: str
    exchange_long: str
    type_long: str
    exchange_short: str
    type_short: str
    volume: int
    
    @field_validator('symbol')
    @classmethod
    def upper_symbol(cls, symbol):
        valid = symbol.upper()
        
        if not valid.endswith('USDT'):
            valid+='USDT'
        return valid
        
    @field_validator("exchange_long", "exchange_short", "type_long", "type_short")
    @classmethod
    def lower_all(cls, data):
        return data.lower()    
    
    
class ChartSchema(BaseModel):
    symbol: str
#ge = больше или равно le = меньше или равно