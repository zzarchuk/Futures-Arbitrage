from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from fastapi import Depends
from typing import Annotated
from sqlalchemy import JSON, select



engine = create_async_engine('sqlite+aiosqlite:///data.db')

new_session = async_sessionmaker(engine, expire_on_commit=False)

async def get_session():
    async with new_session() as session:
        yield session
        
        
SessionDep = Annotated[AsyncSession, Depends(get_session)]

        
class Base(DeclarativeBase):
    pass


class Filters(Base):
    __tablename__ = 'filters'
    
    id: Mapped[int] = mapped_column(primary_key=True)
    exchanges: Mapped[list[str]] = mapped_column(JSON)
    min_volume: Mapped[int]
    max_volume: Mapped[int]
    gap: Mapped[int]






async def get_my_filters():
    async with new_session() as session:
        all_filters = await session.execute(select(Filters))
        filters = all_filters.scalars().all() 
        if not filters:
            return {'message': 'You dont have any filters yet'}
        return {'data': filters}



async def run_database():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    return {'succes': True}