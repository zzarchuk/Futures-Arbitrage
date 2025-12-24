import asyncio
import asyncpg
import random





class Database:
    def __init__(self):
        self.pool = None  # пока пула нет

    async def connect(self):
        self.pool = await asyncpg.create_pool("postgresql://charts:1234@localhost/charts")

    
    async def insert_data(self, exchange_long, exchange_long_type, exchange_short, exchange_short_type, spread, symbol):
        async with self.pool.acquire() as conn: # type: ignore
            await conn.execute(f"INSERT INTO crypto (exchange_long, exchange_long_type, exchange_short, exchange_short_type, spread, symbol) VALUES ('{exchange_long}', '{exchange_long_type}', '{exchange_short}', '{exchange_short_type}', {spread}, '{symbol}')")
            # row = await conn.fetch("SELECT * FROM crypto")
            # print(row)
    
    async def data(self, data):
        async with self.pool.acquire() as conn: # type: ignore
            row = await conn.fetch(f"SELECT * FROM crypto WHERE symbol = '{data[0]}' AND spread >= {data[1]} ORDER BY created_at ASC")
            dict_row = [dict(r) for r in row]
            print(dict_row)

    
            
# async def main():
#     db = Database()
#     await db.connect()         # сначала подключаем пул
#     await db.data(['BTCUSDT', 34])     # вызываем insert_data

# # Запуск через asyncio
# asyncio.run(main())            
