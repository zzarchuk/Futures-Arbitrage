from config.config import state_candles
from database.database import Candles, new_session, run_database
import asyncio
import time
import random
import logging

logging.basicConfig(level=logging.INFO)

#key = биржа лонга / фьючи или спот / биржа шорта / фьючи или спот / монета / обьем
async def update_candle(key, time, spread, exchange_long, exchange_short, symbol, volume, type_long, type_short):
    candle_to_save = None
    
    async with state_candles.lock:
        #logging.info('работает')
        
        if key in state_candles.candle_data:
               
            time_open = state_candles.candle_data[key].get('time_open')
            
            life = time - time_open
                
            if life >= 180:
                candle_to_save = {key: state_candles.candle_data[key]}
                del state_candles.candle_data[key]
                
                    
                    
                    
            else: 
                state_candles.candle_data[key]['spreads'].append(spread)
                
                state_candles.candle_data[key]['life'] = life
                
                
                    
        else:

            state_candles.candle_data[key] = {
                "symbol": symbol,
                'exchange_long': exchange_long,
                'type_long': type_long,
                'exchange_short': exchange_short,
                'type_short': type_short,
                'time_open': time,
                'open': spread,
                'volume': volume,
                'spreads': [spread],
                'life': 0
                
            }
    
    if candle_to_save:
        spreads = candle_to_save[key].get('spreads')
                
        low = min(spreads)
        high = max(spreads)
        close = spreads[-1]
                
        data = Candles(
            exchange_long = candle_to_save[key].get('exchange_long'),
            type_long = candle_to_save[key].get('type_long'),
            exchange_short = candle_to_save[key].get('exchange_short'),
            type_short = candle_to_save[key].get('type_short'),
            time = candle_to_save[key].get('time_open'),
            token = candle_to_save[key].get('symbol'),
            open_spread = candle_to_save[key].get('open'),
            volume = candle_to_save[key].get('volume'),
            low = low,
            high = high,
            close = close
        )
        async with new_session() as session:
            session.add(data)
            await session.commit()
#смотри это 1 функция которая добавляет спреды в бд еще нужна другая которая будет 
# чисто мониторить состояние словаря в конфиге и если допустим спред мимолетный на 
# 30 сек и его уже нету хуйеву тучу времени то чтобы он тоже его добавил            
# функцию эту запускаю только я просто с обьемом там от 100 доларов к 5000 с gap 100
# а сам пользователь чтобы получить график будет просто отдавать параметры по типу монеты бирж обьема и ему 
# будет строить график на данных воркера которого запустил именно я а не пользователь
            
            
async def main():
    await run_database()
    while True:
        await asyncio.sleep(1)
        timer  = int(time.time())
        spread = random.randrange(20, 30, 1)
        await update_candle('binance_bybit_BTCUSDT_500', timer, spread, 'binance', 'bybit', 'BTCUSDT', 500, 'futures', 'futures')
            
            
            
if __name__ == '__main__':
    asyncio.run(main())
        