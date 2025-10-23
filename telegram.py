from filtr import api
import asyncio
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
import math
from datetime import datetime
import json
import time
from collections import defaultdict
from orderbook import binance, bybit, bingx, bitget, htx, kucoin, okx, gate, mexc

semaphores = {
    'binance': asyncio.Semaphore(10),
    'kucoin': asyncio.Semaphore(10),
    'mexc': asyncio.Semaphore(10),
    'htx': asyncio.Semaphore(10),
    'bybit': asyncio.Semaphore(10),
    'bingx': asyncio.Semaphore(10),
    'bitget': asyncio.Semaphore(10),
    'gateio': asyncio.Semaphore(10),
    'okx': asyncio.Semaphore(10),
}

async def safe_fetch_order_book(exchange, pair, limit=100, retries=4, delay=2):
    """Безопасно получает order book с retry"""
    sem = semaphores.get(exchange.id, asyncio.Semaphore(2))
    for attempt in range(retries):
        async with sem:
            try:
                if exchange.id == 'binance':
            
                    result = await binance(pair, limit)
                elif exchange.id == 'kucoin':
                    
                    result = await kucoin(pair, limit)
                elif exchange.id == 'mexc':
                    
                    result = await mexc(pair, limit)
                elif exchange.id == 'htx':
                    
                    result = await htx(pair, limit)
                elif exchange.id == 'bybit':
                    
                    result = await bybit(pair, limit)
                elif exchange.id == 'bingx':
                    await asyncio.sleep(0.5)
                    result = await bingx(pair, limit)
                elif exchange.id == 'bitget':
                    
                    result = await bitget(pair, limit)
                elif exchange.id == 'gateio':
                    
                    result = await gate(pair, limit)
                elif exchange.id == 'okx':
                    
                    result = await okx(pair, limit)
                else:
                    print(f'Говно какое-то')
                
                await asyncio.sleep(3)
                return result
            except Exception as e:
                #print(f"{exchange.id} Ошибка сети при {pair}, попытка {attempt+1}/{retries}: {e}")
                await asyncio.sleep(delay)
    raise Exception(f"Не удалось получить order book для {pair} на {exchange.id}")


async def telegram(message):
    message_for_edit = 0
    msg_for_edit = None
    start_time = time.time()
    id = message.message_id
    
    while True:
        for i in range(0, 6):
            telegram = defaultdict(dict)
            
            