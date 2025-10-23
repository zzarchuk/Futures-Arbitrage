from filtr import api
import asyncio
from aiogram import Bot, Dispatcher
from datetime import datetime
from orderbook import binance, bybit, bingx, bitget, htx, kucoin, okx, gate, mexc
from collections import defaultdict
import ccxt.async_support as ccxt
from help import разделение_фильтра, разброс_пар_по_боту, отправка_в_тг
import time

текущий_словарь = {}
обновление_event = asyncio.Event()
активные_задачи = []

черный_список = ['ALLUSDT', 'AVNTUSDT']

TOKEN = '8414063749:AAFq1sRWe6gSUn6yAQHAZoF1BcmErvzwyvM'
чат_айди = -1002927729443

bot = Bot(token=TOKEN)
dp = Dispatcher()
текущий_словарь = {}
def chunk_dict(d: dict):
    for k, v in d.items():
        yield {k: v}
биржи = {
    "binance": ccxt.binance({"options": {"defaultType": "swap"}, 'timeout': 30000, 'rateLimit': 100, "enableRateLimit": True}),  # type: ignore
    "bybit": ccxt.bybit({"options": {"defaultType": "swap"}, 'timeout': 30000, 'rateLimit': 100, "enableRateLimit": True}),  # type: ignore
    "okx": ccxt.okx({"options": {"defaultType": "swap"}, 'timeout': 30000, 'rateLimit': 100, "enableRateLimit": True}),  # type: ignore
    "kucoin": ccxt.kucoin({"options": {"defaultType": "swap"}, 'timeout': 30000, 'rateLimit': 100, "enableRateLimit": True}),  # type: ignore
    "gateio": ccxt.gateio({"options": {"defaultType": "swap"}, 'timeout': 30000, 'rateLimit': 100, "enableRateLimit": True}),  # type: ignore
    #"mexc": ccxt.mexc({"options": {"defaultType": "swap"}, 'timeout': 30000, 'rateLimit': 700, "enableRateLimit": True}),  # type: ignore
    # "coinbase": ccxt.coinbase({"options": {"defaultType": "spot"}}),
    "bitget": ccxt.bitget({"options": {"defaultType": "swap"}, 'timeout': 30000, 'rateLimit': 800, "enableRateLimit": True}),  # type: ignore
    "bingx": ccxt.bingx({"options": {"defaultType": "swap"}, 'timeout': 30000, 'rateLimit': 100, "enableRateLimit": True}),  # type: ignore
    #"htx": ccxt.htx({"options": {"defaultType": "swap"}, 'timeout': 30000, 'rateLimit': 100, "enableRateLimit": True}), # type: ignore
}
def создать_текст(beast, возможности, lifetime):
    msg = (
        f"Валютная пара: {beast.get('symbol')}\n\n"
        f"Общий объем: {(beast.get('volume') * 2)} USDT\n\n"
        f'Вход в сделку на {beast.get('volume')} USDT\n'
        f"Лонг на: {beast.get('ex_long_id')}\nПо цене: {beast.get('ex_long'):.6f}\n"
        f"Фандинг: {beast.get('funding_long'):.6f}%\nВремя: {beast.get('funding_long_time')}\n\n"
        f"Возможности шорта:\n\n"
    )

    for data in возможности:
        if (data.get('volume') == beast.get('volume') and
            data.get('ex_long_id') == beast.get('ex_long_id')):
            msg += (
                f'Вход в сделку на {beast.get('volume')} USDT\n'
                f"Шорт на {data.get('ex_short_id')}\nПо цене: {data.get('ex_short'):.6f}\n"
                f"Комиссии: {data.get('fees')}%\n"
                f"Фандинг: {data.get('funding_short'):.6f}%\nВремя: {data.get('funding_short_time')}\n"
                f"Спред: {data.get('spread'):.2f}% / {data.get('spread_usdt'):.2f}$ / {(data.get('volume') * 2)}$\n\n"
            )

    # добавляем "время жизни"
    msg += f"🕓 Время жизни: {int(lifetime)} сек."
    return msg

пар_на_бота = 20

semaphores = {
    'binance': asyncio.Semaphore(15),
    'kucoin': asyncio.Semaphore(15),
    'mexc': asyncio.Semaphore(15),
    'htx': asyncio.Semaphore(15),
    'bybit': asyncio.Semaphore(15),
    'bingx': asyncio.Semaphore(15),
    'bitget': asyncio.Semaphore(15),
    'gateio': asyncio.Semaphore(15),
    'okx': asyncio.Semaphore(15),
}

async def safe_fetch_order_book(exchange, pair, limit=100, retries=4, delay=2):
    """Безопасно получает order book с retry"""
    sem = semaphores.get(exchange.id, asyncio.Semaphore(2))
    for attempt in range(retries):
        async with sem:
            try:
                await asyncio.sleep(2.1)
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
                    #await asyncio.sleep(0.3)
                    result = await bingx(pair, limit)
                elif exchange.id == 'bitget':
                    
                    result = await bitget(pair, limit)
                elif exchange.id == 'gateio':
                    
                    result = await gate(pair, limit)
                elif exchange.id == 'okx':
                    
                    result = await okx(pair, limit)
                else:
                    print(f'Говно какое-то')
                
                
                return result
            except Exception as e:
                #print(f"{exchange.id} Ошибка сети при {pair}, попытка {attempt+1}/{retries}: {e}")
                await asyncio.sleep(delay)
    raise Exception(f"Не удалось получить order book для {pair} на {exchange.id}")

semaphore = asyncio.Semaphore(пар_на_бота)


active_arbitrages = {}



async def арбитраж(пары, биржи, лимит, мин_обьем, макс_обьем, шаг):
    айдишники = None
    пустых_итераций = 0
    msg_for_edit = None
    start_time = time.time()
    while True:
        async with semaphore:
            for iam in range(0, 6):
                все_биржи_на_которых_есть_пара = defaultdict(list)
                for symbol, exchanges in пары.items():
                    #print(f'Монета: {symbol}')
                    словарь_с_ценами = defaultdict(lambda: defaultdict(dict))
                    for exhange, data in exchanges.items():
                        for conn in биржи.values():
                            if exhange == conn.id:
                                try:
                                    order_book = await safe_fetch_order_book(conn, symbol)
                                    if order_book:
                                        все_биржи_на_которых_есть_пара[symbol].append(conn.id)
                                    for volume in range(мин_обьем, макс_обьем + 1, шаг):
                                        # ==== VWAP покупка ====
                                        asks = order_book.get("asks") or []
                                        bids = order_book.get("bids") or []
                                        if not asks or not bids:
                                            continue

                                        remaining_money = volume  # сколько USDT хотим потратить
                                        total_spent = 0.0
                                        total_coins = 0.0

                                        for price in asks:
                                            if remaining_money <= 0:
                                                break
                                            coins_can_buy = remaining_money / float(price[0])
                                            actual_coins = min(float(price[1]), coins_can_buy)
                                            total_spent += actual_coins * float(price[0])
                                            total_coins += actual_coins
                                            remaining_money -= actual_coins * float(price[0])

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
                                            actual_coins = min(float(price[1]), remaining_coins)
                                            total_revenue += actual_coins * float(price[0])
                                            coins_sold += actual_coins
                                            remaining_coins -= actual_coins

                                        # если нечего продать — пропускаем
                                        if coins_sold == 0:
                                            continue

                                        sell_avg = total_revenue / coins_sold  # средняя цена продажи
                                        
                                        словарь_с_ценами[symbol][volume][conn.id] = {
                                            'buy_avg': buy_avg,
                                            'sell_avg': sell_avg,
                                            'volume': volume,
                                            'fee_maker': data.get('maker'),
                                            'fee_taker': data.get('taker'),
                                            'funding': data.get('funding'),
                                            'get_funding': data.get('get_funding')
                                        }
                                        
                                except Exception as e:
                                    print(f'Ошибка тут: {e}')
                    if словарь_с_ценами:
                        print(f'Прошла работа по монете: {symbol}')
                        возможности = []
                        for symbol, volumes in словарь_с_ценами.items():
                            for volume, exchanges in volumes.items():
                                # ищем биржу с минимальной buy_avg
                                min_exchange = min(exchanges.items(), key=lambda x: x[1]['buy_avg'])
                                мин_биржа, мин_данные = min_exchange
                                мин_цена = мин_данные.get('buy_avg')
                                мин_тейкер = мин_данные.get('fee_taker')
                                мин_мейкер = мин_данные.get('fee_maker')
                                мин_фандинг = мин_данные.get('funding')
                                мин_время_к_фандингy = мин_данные.get('get_funding')
                                
                                
                                for биржа, дата in exchanges.items():
                                    if мин_биржа == биржа:
                                        continue
                                    цена = дата.get('sell_avg')
                                    тейкер = дата.get('fee_taker')
                                    мейкер = дата.get('fee_maker')
                                    фандинг = дата.get('funding')
                                    время_к_фандингy = дата.get('get_funding')
                                    
                                    комиссии = тейкер + мин_тейкер
                                    #фандинг = захожу в лонг = фандинг платят шортистам
                                    #мин_фандинг = захожу в шорт = фандинг платять лонгистам
                                    
                                    if фандинг >= 0 and мин_фандинг >= 0:                                 
                                        спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии - фандинг + мин_фандинг
                                    if фандинг < 0 and мин_фандинг < 0:
                                        спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии + фандинг - мин_фандинг
                                    if фандинг < 0 and мин_фандинг > 0:
                                        спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии + фандинг + мин_фандинг
                                    if фандинг > 0 and мин_фандинг < 0:
                                        спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии - фандинг - мин_фандинг
                                    #спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии - (фандинг - мин_фандинг)
                                    спред_юсдт = ((volume * 2) / 100) * спред_проценты
                                    if спред_юсдт >= 5:
                                        возможности.append({
                                            'symbol': symbol,
                                            "ex_long": мин_цена,
                                            "ex_long_id": мин_биржа,
                                            'ex_short': цена,
                                            'ex_short_id': биржа,
                                            'fees': комиссии,
                                            'spread': спред_проценты,
                                            'spread_usdt': спред_юсдт,
                                            'funding_long': мин_фандинг,
                                            'funding_long_time': мин_время_к_фандингy,
                                            'funding_short': фандинг,
                                            'funding_short_time': время_к_фандингy,
                                            'volume': volume,
                                        })

                if возможности:  # Если спред найден
                    пустых_итераций = 0  # сбрасываем счетчик
                    
                    for k in возможности:
                        if k.get('symbol') in черный_список:
                            return
                    
                    прошедшие_секунды = time.time() - start_time
                    minutes = int(прошедшие_секунды // 60)
                    sec = int(прошедшие_секунды % 60)
                    beast = max(возможности, key=lambda x: x['spread_usdt'])
                    
                    msg = (
                        f"Валютная пара: {beast.get('symbol')}\n\n"
                        f"Общий объем: {(beast.get('volume') * 2)} USDT\n\n"
                        f'Вход в сделку на {beast.get('volume')} USDT\n'
                        f"Лонг на: {beast.get('ex_long_id')}\nПо цене: {beast.get('ex_long'):.6f}\n"
                        f"Фандинг: {beast.get('funding_long'):.6f}%\nВремя: {beast.get('funding_long_time')}\n\n"
                        f"Возможности шорта:\n\n"
                    )
                    
                    for data in возможности:
                        if (data.get('volume') == beast.get('volume') and
                            data.get('ex_long_id') == beast.get('ex_long_id')):
                            msg += (
                                f'Вход в сделку на {beast.get('volume')} USDT\n'
                                f"Шорт на {data.get('ex_short_id')}\nПо цене: {data.get('ex_short'):.6f}\n"
                                f"Комиссии: {data.get('fees')}%\n"
                                f"Фандинг: {data.get('funding_short'):.6f}%\nВремя: {data.get('funding_short_time')}\n"
                                f"Спред: {data.get('spread'):.2f}% / {data.get('spread_usdt'):.2f}$ / {(data.get('volume') * 2)}$\n\n"
                            )
                    
                    try:
                        if айдишники is None:
                            # Первая отправка
                            айди_сообщения = await bot.send_message(chat_id=чат_айди, text=msg)
                            айдишники = айди_сообщения.message_id
                        else:
                            # Обновление существующего
                            await bot.edit_message_text(
                                chat_id=чат_айди, 
                                message_id=айдишники, 
                                text=f"{msg}\n\nВремя жизни: {minutes} минут {sec} секунд"
                            )
                            msg_for_edit = f"{msg}\n\nВремя жизни: {minutes} минут {sec} секунд"
                    except Exception as e:
                        print(f'Ошибка в функции арбитража: {e}')
                        
                else:  # Спред не найден
                    пустых_итераций += 1
                    
                    
                    if iam == 0 and айдишники is None:
                        print(f'Первая итерация - спред не найден, выходим {symbol}')
                        return
                    # Если спред был, но пропал - пишем что умер и выходим
                    if айдишники is not None and msg_for_edit is not None:
                        try:
                            await bot.edit_message_text(
                                chat_id=чат_айди, 
                                message_id=айдишники, 
                                text=f'{msg_for_edit}\n\n❌ Спред потерялся'
                            )
                        except Exception as e:
                            print(f'Ошибка редактирования: {e}')
                        return
                    
                    # Если 5 итераций подряд нет спреда - удаляем сообщение (если было)
                    if пустых_итераций >= 5 and айдишники is not None:
                        try:
                            await bot.delete_message(chat_id=чат_айди, message_id=айдишники)
                        except Exception as e:
                            print(f'Ошибка удаления: {e}')
                        return

# async def арбитраж(пары, биржи, лимит, мин_обьем, макс_обьем, шаг):
#     while True:
#         async with semaphore:
#             все_биржи_на_которых_есть_пара = defaultdict(list)
#             for symbol, exchanges in пары.items():
#                 #print(f'Монета: {symbol}')
#                 словарь_с_ценами = defaultdict(lambda: defaultdict(dict))
#                 for exhange, data in exchanges.items():
#                     for conn in биржи.values():
#                         if exhange == conn.id:
#                             try:
#                                 order_book = await safe_fetch_order_book(conn, symbol)
#                                 if order_book:
#                                     все_биржи_на_которых_есть_пара[symbol].append(conn.id)
#                                 for volume in range(мин_обьем, макс_обьем + 1, шаг):
#                                     # ==== VWAP покупка ====
#                                     asks = order_book.get("asks") or []
#                                     bids = order_book.get("bids") or []
#                                     if not asks or not bids:
#                                         continue

#                                     remaining_money = volume  # сколько USDT хотим потратить
#                                     total_spent = 0.0
#                                     total_coins = 0.0

#                                     for price in asks:
#                                         if remaining_money <= 0:
#                                             break
#                                         coins_can_buy = remaining_money / float(price[0])
#                                         actual_coins = min(float(price[1]), coins_can_buy)
#                                         total_spent += actual_coins * float(price[0])
#                                         total_coins += actual_coins
#                                         remaining_money -= actual_coins * float(price[0])

#                                     # если ничего не купили — пропускаем
#                                     if total_coins == 0:
#                                         continue

#                                     buy_avg = total_spent / total_coins  # средняя цена покупки


#                                     # ==== VWAP продажа ====
#                                     remaining_coins = total_coins  # продаём то, что купили
#                                     total_revenue = 0.0
#                                     coins_sold = 0.0

#                                     for price in bids:
#                                         if remaining_coins <= 0:
#                                             break
#                                         actual_coins = min(float(price[1]), remaining_coins)
#                                         total_revenue += actual_coins * float(price[0])
#                                         coins_sold += actual_coins
#                                         remaining_coins -= actual_coins

#                                     # если нечего продать — пропускаем
#                                     if coins_sold == 0:
#                                         continue

#                                     sell_avg = total_revenue / coins_sold  # средняя цена продажи
                                    
#                                     словарь_с_ценами[symbol][volume][conn.id] = {
#                                         'buy_avg': buy_avg,
#                                         'sell_avg': sell_avg,
#                                         'volume': volume,
#                                         'fee_maker': data.get('maker'),
#                                         'fee_taker': data.get('taker'),
#                                         'funding': data.get('funding'),
#                                         'get_funding': data.get('get_funding')
#                                     }
#                                     print(f'Прошла работа по монете: {symbol}')
                                    
#                             except Exception as e:
#                                 print(f'Ошибка тут: {e}')
#                 if словарь_с_ценами:
#                     возможности = []
#                     for symbol, volumes in словарь_с_ценами.items():
#                         for volume, exchanges in volumes.items():
#                             # ищем биржу с минимальной buy_avg
#                             min_exchange = min(exchanges.items(), key=lambda x: x[1]['buy_avg'])
#                             мин_биржа, мин_данные = min_exchange
#                             мин_цена = мин_данные.get('buy_avg')
#                             мин_тейкер = мин_данные.get('fee_taker')
#                             мин_мейкер = мин_данные.get('fee_maker')
#                             мин_фандинг = мин_данные.get('funding')
#                             мин_время_к_фандингy = мин_данные.get('get_funding')
                            
                            
#                             for биржа, дата in exchanges.items():
#                                 if мин_биржа == биржа:
#                                     continue
#                                 цена = дата.get('sell_avg')
#                                 тейкер = дата.get('fee_taker')
#                                 мейкер = дата.get('fee_maker')
#                                 фандинг = дата.get('funding')
#                                 время_к_фандингy = дата.get('get_funding')
                                
#                                 комиссии = тейкер + мин_тейкер
#                                 #фандинг = захожу в лонг = фандинг платят шортистам
#                                 #мин_фандинг = захожу в шорт = фандинг платять лонгистам
                                
#                                 if фандинг >= 0 and мин_фандинг >= 0:                                 
#                                     спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии - фандинг + мин_фандинг
#                                 if фандинг < 0 and мин_фандинг < 0:
#                                     спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии + фандинг - мин_фандинг
#                                 if фандинг < 0 and мин_фандинг > 0:
#                                     спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии + фандинг + мин_фандинг
#                                 if фандинг > 0 and мин_фандинг < 0:
#                                     спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии - фандинг - мин_фандинг
#                                 #спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии - (фандинг - мин_фандинг)
#                                 спред_юсдт = ((volume * 2) / 100) * спред_проценты
#                                 if спред_юсдт >= 5:
#                                     возможности.append({
#                                         'symbol': symbol,
#                                         "ex_long": мин_цена,
#                                         "ex_long_id": мин_биржа,
#                                         'ex_short': цена,
#                                         'ex_short_id': биржа,
#                                         'fees': комиссии,
#                                         'spread': спред_проценты,
#                                         'spread_usdt': спред_юсдт,
#                                         'funding_long': мин_фандинг,
#                                         'funding_long_time': мин_время_к_фандингy,
#                                         'funding_short': фандинг,
#                                         'funding_short_time': время_к_фандингy,
#                                         'volume': volume,
#                                     })

#         if возможности == []:
#             return
#         for k in возможности:
#             if k.get('symbol') == 'ALLUSDT':
#                 return

#         #print(возможности)

#                     # выбираем лучший вариант
#         beast = max(возможности, key=lambda x: x['spread_usdt'])
#         # arb_id = f"{beast['symbol']}_{beast['ex_long_id']}_{beast['ex_short_id']}_{beast['volume']}"
#         arb_id = f"{beast['symbol']}_{beast['volume']}"
#         now = datetime.utcnow()
#         print(f'{active_arbitrages}\n\n')
#                     # === Проверяем, есть ли уже активный арбитраж ===
#         if arb_id not in active_arbitrages:
#             msg = await bot.send_message(chat_id=чат_айди, text=создать_текст(beast, возможности, 0))
#             active_arbitrages[arb_id] = {
#                 'message_id': msg.message_id,
#                 'start_time': now
#             }
#         else:
#             start_time = active_arbitrages[arb_id]['start_time']
#             lifetime = (now - start_time).total_seconds()
#             msg_id = active_arbitrages[arb_id]['message_id']

#             try:
#                 print('редачим')
#                 # await bot.edit_message_text(
#                 #     chat_id=чат_айди,
#                 #     message_id=msg_id,
#                 #     text=создать_текст(beast, возможности, lifetime)
#                 # )
#                 await bot.edit_message_text(
#                     chat_id=чат_айди,
#                     message_id=msg_id,
#                     text=создать_текст(beast, возможности, lifetime)
#                 )
#                 await asyncio.sleep(0.4)
#             except Exception as e:
#                 print(f"Ошибка при редактировании: {e}")








async def обновление_словаря():
    print(f'поиск')
    """Обновляет словарь монет каждые 15 секунд (для теста)"""
    global текущий_словарь
    print("📡 Обновление словаря запущено!")

    while True:
        try:
            разделение_словаря = await api()
            if разделение_словаря:
                текущий_словарь = разделение_словаря
                print(f"🔄 Словарь обновлён! Монет: {len(разделение_словаря)}")
                #обновление_event.set()  # 🔔 сигнал арбитраж боту
            else:
                print("⚠️ API вернуло пустой словарь")
        except Exception as e:
            print(f'❌ Ошибка в обновлении словаря: {e}')
        
        await asyncio.sleep(5 * 60)  # тест: 15 секунд, реальность — 15 * 60


async def арбитраж_бот():
    """Следит за обновлениями словаря и перезапускает задачи"""
    #global активные_задачи
    global текущий_словарь
    print("✅ Арбитраж бот запущен!")

    while True:
        # ждём сигнала от обновления словаря
        # await обновление_event.wait()
        # обновление_event.clear()

        # # отменяем старые задачи, если есть
        # if активные_задачи:
        #     print("🧹 Ожидание завершения старых задач...")
        #     await asyncio.gather(*активные_задачи, return_exceptions=True)
        #     активные_задачи = []
        if текущий_словарь:
        # создаём новые задачи на основе нового словаря
            print("🚀 Запуск новых арбитражных задач...")
            tasks = []
            for пара in chunk_dict(текущий_словарь):
                task = asyncio.create_task(арбитраж(пара, биржи, 100, 50, 250, 25))
                tasks.append(task)
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            await asyncio.sleep(5)
            continue
                



# async def арбитраж_бот():
#     print("📡 обновление_словаря запущено!")
#     while True:
#         try:
#             разделение_словаря = await api()
#             print("🔄 Словарь обновлён!")
#             if разделение_словаря:
#                 print("✅ арбитраж_бота запущен!")
#                 tasks = []
#                 for пара in chunk_dict(разделение_словаря):
#                     task = asyncio.create_task(арбитраж(пара, биржи, 100, 50, 250, 25))
#                     tasks.append(task)
                
#                 # Ждём завершения всех задач
#                 results = await asyncio.gather(*tasks, return_exceptions=True)
                
#                 # Проверяем, все ли задачи завершились
#                 print(f"✅ Итерация завершена. Результаты: {len(results)} задач")
                
#         except Exception as e:
#             print(f'Ошибка в обновлении словаря: {e}')
        
#         await asyncio.sleep(2)  # <- Только после этого запустится новая итерация

async def телеграм():
    await dp.start_polling(bot)

async def main():
    asyncio.create_task(обновление_словаря())
    asyncio.create_task(арбитраж_бот())
    await телеграм()
    #await asyncio.gather(арбитраж_бот(), телеграм(), обновление_словаря())










# async def main():
# #    while True:
#         #разделение_словаря = await разделение_фильтра(количество_потоков, threads, api)
#     разделение_словаря = await api()
#     if разделение_словаря:
#         tasks = [asyncio.create_task(арбитраж(chunk_dict(разделение_словаря), биржи, 100, 300, 1000, 100))]
#         await asyncio.gather(*tasks)
#         print('сон')
#         await asyncio.sleep(10)
#         #     await разброс_пар_по_боту(разделение_словаря, арбитраж, количество_потоков)
#         # await asyncio.sleep(10)
    
# async def main():
#     while True:
#         разделение_словаря = await api()  # получаешь большой словарь
#         if разделение_словаря:
#             tasks = []
#             for пара in chunk_dict(разделение_словаря):  # перебор по одной паре
#                 task = asyncio.create_task(арбитраж(пара, биржи, 100, 300, 1000, 100))
#                 tasks.append(task)
#             await asyncio.gather(*tasks)
#             print('Сон...')

#         print(f'Ждем 120 сек для обновление словаря')
#         await asyncio.sleep(120)


# разделение_словаря = {}

# async def обновление_словаря():
#     print("📡 обновление_словаря запущено!")
#     global разделение_словаря
#     while True:
#         try:
#             разделение_словаря = await api()
#             print("🔄 Словарь обновлён!")
#         except Exception as e:
#             print(f'Ошибка в обновлении словаря: {e}')
#         await asyncio.sleep(600)  # 10 минут

# async def арбитраж_бота():
#     global разделение_словаря
#     while True:
#         if разделение_словаря:
#             print("✅ арбитраж_бота запущен!")
#             tasks = []
#             for пара in chunk_dict(разделение_словаря):
#                 task = asyncio.create_task(арбитраж(пара, биржи, 100, 300, 1000, 100))
#                 tasks.append(task)
#                 # await арбитраж(пара, биржи, 100, 300, 1000, 100)
#             await asyncio.gather(*tasks, return_exceptions=True)
#         await asyncio.sleep(50)

# async def main():
#     await asyncio.gather(
#         обновление_словаря(),
#         арбитраж_бота()
#     )
if __name__ == '__main__':
    asyncio.run(main())