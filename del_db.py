from database.database import Candles, new_session
from sqlalchemy import delete
import asyncio

async def main():
    async with new_session() as session:
        await session.execute(delete(Candles))
        await session.commit()


asyncio.run(main())

