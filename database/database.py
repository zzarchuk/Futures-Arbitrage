from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from fastapi import Depends
from typing import Annotated
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy import JSON, ForeignKey, select
import os
from dotenv import load_dotenv

load_dotenv()

db = os.getenv("DATABASE_URL")



engine = create_async_engine(db) # type: ignore

new_session = async_sessionmaker(engine, expire_on_commit=False)

async def get_session():
    async with new_session() as session:
        yield session
        
        
SessionDep = Annotated[AsyncSession, Depends(get_session)]

        
class Base(DeclarativeBase):
    pass



    

class Users(Base):
    __tablename__ = 'users'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(unique=True, index=True)
    password: Mapped[str]

    filter: Mapped[list['Filters']] = relationship(back_populates='user_filter')
    
    blacklist: Mapped['Blacklist'] = relationship(back_populates='user_blacklist', uselist=False)
    
    #candles: Mapped[list['Candles']] = relationship(back_populates='user_candles')



class Filters(Base):
    __tablename__ = 'filters'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    exchanges: Mapped[list[str]] = mapped_column(JSON)
    min_volume: Mapped[int]
    max_volume: Mapped[int]
    gap: Mapped[int]
    spread: Mapped[int]
    
    user_id: Mapped[int] = mapped_column(ForeignKey('users.id'))
    
    
    user_filter: Mapped['Users'] = relationship(back_populates='filter')


class Blacklist(Base):
    __tablename__ = 'blacklist'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    tokens: Mapped[list[str]] = mapped_column(JSON)
    
    user_id: Mapped[int] = mapped_column(ForeignKey('users.id'), unique=True)
    

    user_blacklist: Mapped['Users'] = relationship(back_populates='blacklist')


class Candles(Base):
    __tablename__ = 'candles'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    #user_id: Mapped[int] = mapped_column(ForeignKey('users.id'))
    
    exchange_short: Mapped[str]
    type_short: Mapped[str]
    exchange_long: Mapped[str]
    type_long: Mapped[str]
    token: Mapped[str]
    time: Mapped[int]
    
    
    open_spread: Mapped[float]
    
    close: Mapped[float]
    low: Mapped[float]
    high: Mapped[float]
    volume: Mapped[int]
    
    def __repr__(self):
        return (
            f"{self.token} ("
            f"exchange_short='{self.exchange_short}', "
            f"type_short='{self.type_short}', "
            f"exchange_long='{self.exchange_long}', "
            f"type_long='{self.type_long}', "
            f"token='{self.token}', "
            f"time={self.time}, "
            f"open_spread={self.open_spread}, "
            f"close={self.close}, "
            f"low={self.low}, "
            f"high={self.high}, "
            f"volume={self.volume}"
            f")"
        )
    
    #user_candles: Mapped['Users'] = relationship(back_populates='candles')




async def get_my_filters(user_id: int):
    async with new_session() as session:
        all_filters = await session.execute(select(Filters).where(Filters.user_id == user_id))
        filters = all_filters.scalars().all() 
        if not filters:
            return {'message': 'You dont have any filters yet'}
        return {'data': filters}



async def run_database():
    async with engine.begin() as conn:
        #await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    return {'succes': True}