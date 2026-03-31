import time
import asyncio
from collections import defaultdict
#data_for_db = {str: [{'timestamp': 1,  'spread': 2}, {'timestamp': 3,  'spread': 4}],}


async def candles(data_for_db, lock, for_db):
    
    while True:
        #print('работа')
        await asyncio.sleep(30)

        for_db.clear()

        async with lock:
            keys_to_delete = []   # сюда будем складывать что удалить

            for k, v in list(data_for_db.items()):

                if not v:   # защита от пустого списка
                    continue

                timestamps = [item['timestamp'] for item in v]
                spreads = [item['spread'] for item in v]

                open_time = min(timestamps)
                close_time = max(timestamps)

                if close_time - open_time < 30:
                    continue

                max_spread = max(spreads)
                min_spread = min(spreads)

                open_spread = None
                close_spread = None

                for data in v:
                    if data['timestamp'] == open_time:
                        open_spread = data['spread']
                    if data['timestamp'] == close_time:
                        close_spread = data['spread']

                if open_spread is None or close_spread is None:
                    continue

                for_db[k] = {
                    'open': open_spread,
                    'close': close_spread,
                    'max': max_spread,
                    'min': min_spread
                }

                keys_to_delete.append(k)  # помечаем на удаление

            # удаляем ПОСЛЕ цикла

            for k in keys_to_delete:
                del data_for_db[k]

        #print('работа 2')
        #print(for_db)
        
            
                

        

            
#asyncio.run(candles())         
            
