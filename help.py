import math
import json
import asyncio
import time
from collections import defaultdict

async def разделение_фильтра(количество_потоков, переменная_словаря, api):
    сам_словарь = await api()
    print('начало')
    валютные_пары_в_бота = math.ceil(len(сам_словарь.keys()) / количество_потоков)
    
    def zalupa(d, chunk):
        ff = list(d.items())
        for w in range(0, len(ff), chunk):
            x = dict(ff[w : w + chunk])
            yield x

    i = 1
    for part in zalupa(сам_словарь, валютные_пары_в_бота):
        переменная_словаря[f"th_{i}"] = part
        i += 1
        
    with open('fil.txt', 'w', encoding='utf-8') as f:
        json.dump(переменная_словаря, f, ensure_ascii=False, indent=4)
        
    return переменная_словаря


async def разброс_пар_по_боту(пары, арбитраж, количество_потоков):
    if пары:
        try:
            for i in range(1, (количество_потоков + 1)):
                if f'th_{i}' in пары:
                    await арбитраж(пары[f'th_{i}'])
                        
        except Exception as e:
            print(f'Ошибка {e}')
            
async def отправка_в_тг(пара, биржи_на_монете, возможности, комиссии, обьем, бот, чат_айди, сообщение, приоритетные_биржи, мин_спред_юсдт, safe_fetch_orderbook):
    редакт_сообщение = 0
    сообщ_под_редакт = None
    стартовое_время = time.time()
    айди = сообщение.id
    
    while True:
        for i in range(0, 6):
            концовый_словарь = defaultdict(dict)
            for биржи in биржи_на_монете.values():
                for биржа in биржи:                    
                    for дата in возможности:
                        if биржа == дата.get('ex_long_id') or биржа == дата.get('ex_short_id'):
                            try:
                                order_book = await safe_fetch_orderbook(биржа, пара)
                                asks = order_book.get("asks") or []
                                bids = order_book.get("bids") or []
                                if not asks or not bids:
                                    continue

                                remaining_money = обьем  # сколько USDT хотим потратить
                                total_spent = 0.0
                                total_coins = 0.0

                                for price in asks:
                                    if remaining_money <= 0:
                                        break
                                    coins_can_buy = remaining_money / price[0]
                                    actual_coins = min(price[1], coins_can_buy)
                                    total_spent += actual_coins * price[0]
                                    total_coins += actual_coins
                                    remaining_money -= actual_coins * price[0]

                                        # если ничего не купили — пропускаем
                                if total_coins == 0:
                                    continue

                                buy_avg = total_spent / total_coins  # средняя цена покупки


                                        # ==== VWAP продажа ====
                                remaining_coins = total_coins  # продаём то, что купили
                                total_revenue = 0.0
                                coins_sold = 0.0
                                    
                                for price in bids:
                                    if remaining_coins <= 0:
                                        break
                                    actual_coins = min(price[1], remaining_coins)
                                    total_revenue += actual_coins * price[0]
                                    coins_sold += actual_coins
                                    remaining_coins -= actual_coins

                                        # если нечего продать — пропускаем
                                if coins_sold == 0:
                                    continue

                                sell_avg = total_revenue / coins_sold  # средняя цена продажи


                                концовый_словарь[пара][биржа] = {
                                    "buy_avg": buy_avg,
                                    "sell_avg": sell_avg,
                                    "fee": дата.get('fees'),
                                    'funding': дата.get('funding'),
                                    'get_funding': дата.get('get_funding')
                                }
                            except Exception as e:
                                print(f'Ошибка в отправке тг: {e}')
               
            
            




# async def арбитраж(пары, биржи, лимит, мин_обьем, макс_обьем, шаг):
#     async with semaphore:
#         все_биржи_на_которых_есть_пара = defaultdict(list)
#         for symbol, exchanges in пары.items():
#             словарь_с_ценами = defaultdict(lambda: defaultdict(dict))
#             for exhange, data in exchanges.items():
#                 for conn in биржи.values():
#                     if exhange == conn.id:
#                         try:
#                             order_book = await safe_fetch_order_book(conn, symbol)
#                             if order_book:
#                                 все_биржи_на_которых_есть_пара[symbol].append(conn.id)
#                             for volume in range(мин_обьем, макс_обьем + 1, шаг):
#                                 # ==== VWAP покупка ====
#                                 asks = order_book.get("asks") or []
#                                 bids = order_book.get("bids") or []
#                                 if not asks or not bids:
#                                     continue

#                                 remaining_money = volume  # сколько USDT хотим потратить
#                                 total_spent = 0.0
#                                 total_coins = 0.0

#                                 for price in asks:
#                                     if remaining_money <= 0:
#                                         break
#                                     coins_can_buy = remaining_money / float(price[0])
#                                     actual_coins = min(float(price[1]), coins_can_buy)
#                                     total_spent += actual_coins * float(price[0])
#                                     total_coins += actual_coins
#                                     remaining_money -= actual_coins * float(price[0])

#                                 # если ничего не купили — пропускаем
#                                 if total_coins == 0:
#                                     continue

#                                 buy_avg = total_spent / total_coins  # средняя цена покупки


#                                 # ==== VWAP продажа ====
#                                 remaining_coins = total_coins  # продаём то, что купили
#                                 total_revenue = 0.0
#                                 coins_sold = 0.0

#                                 for price in bids:
#                                     if remaining_coins <= 0:
#                                         break
#                                     actual_coins = min(float(price[1]), remaining_coins)
#                                     total_revenue += actual_coins * float(price[0])
#                                     coins_sold += actual_coins
#                                     remaining_coins -= actual_coins

#                                 # если нечего продать — пропускаем
#                                 if coins_sold == 0:
#                                     continue

#                                 sell_avg = total_revenue / coins_sold  # средняя цена продажи
                                
#                                 словарь_с_ценами[symbol][volume][conn.id] = {
#                                     'buy_avg': buy_avg,
#                                     'sell_avg': sell_avg,
#                                     'volume': volume,
#                                     'fee_maker': data.get('maker'),
#                                     'fee_taker': data.get('taker'),
#                                     'funding': data.get('funding'),
#                                     'get_funding': data.get('get_funding')
#                                 }
                                
#                         except Exception as e:
#                             print(f'Ошибка тут: {e}')
#             if словарь_с_ценами:
#                 for пара, обьем_дата in словарь_с_ценами.items():
#                     for обьем, биржа_дата in обьем_дата.items():
#                         for биржа, дата in биржа_дата.items():
                            
                #await asyncio.sleep(20) 
    

    
