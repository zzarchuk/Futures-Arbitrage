from web import send_message_to_site, app, delete_message_from_site
import uvicorn
from filtr import api
import asyncio
import aiohttp
from aiogram import Bot, Dispatcher
from orderbook import binance, bybit, bingx, bitget, htx, kucoin, okx, gate, mexc
from collections import defaultdict
import time

мин_спред = 6
текущий_словарь = {}

работающие_арбитражи = set()
работающие_арбитражи_lock = asyncio.Lock()
черный_список = ['OMUSDT', 'KTAUSDT', 'MEGAUSDT', 'XNAPUSDT', 'NUMIUSDT', 'SVSAUSDT', 'IQUSDT', 'AMUSDT', 'SIGMAISDT', 'ALPHAUSDT', 'BNBHOLDERUSDT', 'UUSDT', 'ALLUSDT', 'ARCUSDT',  'MEMECOINUSDT', 'RICEUSDT', 'BDXNUSDT', 'AURAUSDT', 'METUSDT', 'MONUSDT']
светлофор = asyncio.Semaphore(300000)
# TOKEN = "8414063749:AAFq1sRWe6gSUn6yAQHAZoF1BcmErvzwyvM"
# чат_айди = -1002927729443

#bot = Bot(token=TOKEN)
#dp = Dispatcher()


# def chunk_dict(d: dict):
#     for k, v in d.items():
#         yield {k: v}
def chunk_dict(d: dict, n: int = 20):
    items = list(d.items())
    for i in range(0, len(items), n):
        yield dict(items[i:i + n])

биржи = {
    "binance": "binance",  # type: ignore
    "bybit": "bybit",  # type: ignore
    "okx": "okx",  # type: ignore
    "kucoin": "kucoin",  # type: ignore
    "gateio": "gateio",  # type: ignore
    "mexc": "mexc",  # type: ignore
    "bitget": "bitget",  # type: ignore
    "bingx": "bingx",  # type: ignore
    "htx": "htx",  # type: ignore
}


async def safe_fetch_order_book(
    exchange, pair, sessions, limit=100, retries=8, delay=0.5
):
    #sem = semaphores.get(exchange, asyncio.Semaphore(10))
    #async with semaphoress:
    for attempt in range(retries):
        try:
            await asyncio.sleep(0.1)
            session = sessions.get(exchange)
            if exchange == "binance":
                params = {"symbol": pair, "limit": limit}

                async with session.get(
                    url="https://fapi.binance.com/fapi/v1/depth", params=params
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book["asks"]
                    bids = order_book["bids"]
                    #await asyncio.sleep(0.2)
                    return {"asks": asks, "bids": bids}
            elif exchange == "kucoin":
                z = pair.replace("USDT", "USDTM")

                async with session.get(
                    f"https://api-futures.kucoin.com/api/v1/level2/depth{limit}?symbol={z}"
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book["data"]["asks"]
                    bids = order_book["data"]["bids"]
                    #await asyncio.sleep(0.2)
                    return {"asks": asks, "bids": bids}
            elif exchange == "mexc":
                params = {"limit": limit}
                async with session.get(
                    url=f"https://contract.mexc.com/api/v1/contract/depth/{pair.replace('USDT', '_USDT')}",
                    params=params,
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book["data"]["asks"]
                    bids = order_book["data"]["bids"]
                    #await asyncio.sleep(0.2)
                    return {"asks": asks, "bids": bids}
            elif exchange == "htx":
                params = {"contract_code": pair.replace("USDT", "-USDT"), "type": "step0"}
                async with session.get(
                    url="https://api.hbdm.com/linear-swap-ex/market/depth", params=params
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book["tick"]["asks"]
                    bids = order_book["tick"]["bids"]
                    #await asyncio.sleep(0.2)
                    return {"asks": asks, "bids": bids}
            elif exchange == "bybit":
                params = {"category": "linear", "symbol": pair, "limit": limit}
                async with session.get(
                    url="https://api.bybit.com/v5/market/orderbook", params=params
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book["result"]["a"]
                    bids = order_book["result"]["b"]
                    #await asyncio.sleep(0.2)
                    return {"asks": asks, "bids": bids}
            elif exchange == "bingx":
                params = {"symbol": pair.replace("USDT", "-USDT"), "limit": limit}
                async with session.get(
                    url="https://open-api.bingx.com/openApi/swap/v2/quote/depth", params=params
                ) as respone:
                    order_book = await respone.json()
                    #print(order_book)
                    asks = order_book["data"]["asks"]
                    bids = order_book["data"]["bids"]
                    #await asyncio.sleep(0.2)
                    return {"asks": asks, "bids": bids}
            elif exchange == "bitget":
                params = {"symbol": pair, "productType": "USDT-FUTURES", "limit": limit}
                async with session.get(
                    url="https://api.bitget.com/api/v2/mix/market/merge-depth", params=params
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book["data"]["asks"]
                    bids = order_book["data"]["bids"]
                    #await asyncio.sleep(0.2)
                    return {"asks": asks, "bids": bids}
            elif exchange == "gateio":
                params = {
                    "contract": pair.replace("USDT", "_USDT"),
                    "limit": limit,
                    "settle": "USDT",
                }
                headers = {"Accept": "application/json", "Content-Type": "application/json"}

                async with session.get(
                    url="https://api.gateio.ws/api/v4/futures/usdt/order_book",
                    params=params,
                    headers=headers,
                ) as respone:
                    order_book = await respone.json()
                    asks = [
                        [float(item["p"]), int(item["s"])]
                        for item in order_book.get("asks", [])
                    ]
                    bids = [
                        [float(item["p"]), int(item["s"])]
                        for item in order_book.get("bids", [])
                    ]
                    #await asyncio.sleep(0.2)
                    return {"asks": asks, "bids": bids}
            elif exchange == "okx":
                params = {"instId": pair.replace("USDT", "-USDT-SWAP"), "sz": limit}
                async with session.get(
                    url="https://www.okx.com/api/v5/market/books", params=params
                ) as respone:
                    order_book = await respone.json()
                    #print(order_book)
                    asks = order_book["data"][0]["asks"]
                    bids = order_book["data"][0]["bids"]
                    #await asyncio.sleep(0.2)
                    return {"asks": asks, "bids": bids}

            else:
                print(f"Говно какое-то")

            #return result
        except Exception as e:
            #if attempt < retries - 1:
            print(f"{exchange} Ошибка {pair}, попытка {attempt+1}/{retries}: {e}")
            await asyncio.sleep(delay)
    raise Exception(f"Не удалось получить order book для {pair} на {exchange}")



async def арбитраж(пары, мин_обьем, макс_обьем, шаг, session):
    global работающие_арбитражи_lock
    global работающие_арбитражи

    #async with semaphore:
        #while True:
    for symbol, exchanges in пары.items():
        #print(f'Работа по {symbol}')
        #await asyncio.sleep(2)
        async with работающие_арбитражи_lock:
            if symbol in работающие_арбитражи:
                return
        if symbol in черный_список:
            return
        словарь_с_ценами = defaultdict(lambda: defaultdict(dict))
        tasks = []  # создаём список задач для конкретной монеты
        meta = []   # храним (биржа, данные)

        # создаём задачи на все биржи
        for exchange, data in exchanges.items():
            task = asyncio.create_task(
                safe_fetch_order_book(exchange, symbol, sessions=session)
            )
            tasks.append(task)
            meta.append((exchange, data))

        # ждём, пока все биржи ответят
        order_books = await asyncio.gather(*tasks, return_exceptions=False)

        for (exchange, data), order_book in zip(meta, order_books):
            # if isinstance(order_book, Exception):
            #     print(f"❌ Ошибка {exchange} ({symbol}): {type(order_book).__name__}: {order_book}")
            #     continue

            asks = order_book.get("asks") or []
            bids = order_book.get("bids") or []
            if not asks or not bids:
                continue

            for volume in range(мин_обьем, макс_обьем + 1, шаг):
                # ==== VWAP покупка ====
                remaining_money = volume
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

                if total_coins == 0:
                    continue

                buy_avg = total_spent / total_coins

                # ==== VWAP продажа ====
                remaining_coins = total_coins
                total_revenue = 0.0
                coins_sold = 0.0

                for price in bids:
                    if remaining_coins <= 0:
                        break
                    actual_coins = min(float(price[1]), remaining_coins)
                    total_revenue += actual_coins * float(price[0])
                    coins_sold += actual_coins
                    remaining_coins -= actual_coins

                if coins_sold == 0:
                    continue

                sell_avg = total_revenue / coins_sold

                словарь_с_ценами[symbol][volume][exchange] = {
                    "buy_avg": buy_avg,
                    "sell_avg": sell_avg,
                    "volume": volume,
                    "fee_maker": data.get("maker"),
                    "fee_taker": data.get("taker"),
                    "funding": data.get("funding"),
                    "get_funding": data.get("get_funding"),
                }
        if not словарь_с_ценами:
            return                
        
        if словарь_с_ценами:
            возможности = []
            #print(f"Прошла работа по монете: {symbol}")
            for symbol, volumes in словарь_с_ценами.items():
                for volume, exchanges in volumes.items():
                    # ищем биржу с минимальной buy_avg
                    min_exchange = min(
                        exchanges.items(), key=lambda x: x[1]["buy_avg"]
                    )
                    мин_биржа, мин_данные = min_exchange
                    мин_цена = мин_данные.get("buy_avg")
                    мин_тейкер = мин_данные.get("fee_taker")
                    мин_мейкер = мин_данные.get("fee_maker")
                    мин_фандинг = мин_данные.get("funding")
                    мин_время_к_фандингy = мин_данные.get("get_funding")

                    for биржа, дата in exchanges.items():
                        if мин_биржа == биржа:
                            continue
                        цена = дата.get("sell_avg")
                        тейкер = дата.get("fee_taker")
                        мейкер = дата.get("fee_maker")
                        фандинг = дата.get("funding")
                        время_к_фандингy = дата.get("get_funding")

                        комиссии = тейкер + мин_тейкер
                        # фандинг = захожу в лонг = фандинг платят шортистам
                        # мин_фандинг = захожу в шорт = фандинг платять лонгистам
                        if фандинг <= 0 and мин_фандинг <= 0:
                            спред_проценты = (
                                ((цена - мин_цена) / мин_цена * 100)
                                - комиссии
                                #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги

                                + фандинг
                                #спред с учетом фандингов даже если основной спред это сам фандинг
                                #- фандинг
                                #+ мин_фандинг
                            )

                        elif фандинг >= 0 and мин_фандинг >= 0:
                            спред_проценты = (
                                ((цена - мин_цена) / мин_цена * 100)
                                - комиссии
                                #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги

                                - мин_фандинг
                                #спред с учетом фандингов даже если основной спред это сам фандинг
                                #- фандинг
                                #+ мин_фандинг
                            )
                        # elif фандинг < 0 and мин_фандинг < 0:
                        #     спред_проценты = (
                        #         ((цена - мин_цена) / мин_цена * 100)
                        #         - комиссии
                        #         #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                        #         + фандинг

                        #         #спред с учетом фандингов даже если основной спред это сам фандинг
                        #         #- фандинг
                        #         #+ мин_фандинг
                        #     )
                        elif фандинг < 0 and мин_фандинг > 0:
                            спред_проценты = (
                                ((цена - мин_цена) / мин_цена * 100)
                                - комиссии
                                #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                                + фандинг
                                - мин_фандинг
                                #спред с учетом фандингов даже если основной спред это сам фандинг
                                #- фандинг
                                #+ мин_фандинг
                            )
                        elif фандинг > 0 and мин_фандинг < 0:
                            спред_проценты = (
                                ((цена - мин_цена) / мин_цена * 100)
                                - комиссии
                                #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги

                                #спред с учетом фандингов даже если основной спред это сам фандинг
                                #- фандинг
                                #+ мин_фандинг
                            )
                        # спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии - (фандинг - мин_фандинг)
                        
                        else:
                            # 🛑 резервная защита — если что-то не попало в условия
                            print(f"⚠️ Не попало ни в одно условие: фандинг={фандинг}, мин_фандинг={мин_фандинг}")
                            спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии
                            
                        спред_юсдт = ((volume * 2) / 100) * спред_проценты
                        if спред_юсдт >= мин_спред:
                            возможности.append(
                                {
                                    "symbol": symbol,
                                    "ex_long": мин_цена,
                                    "ex_long_id": мин_биржа,
                                    "ex_short": цена,
                                    "ex_short_id": биржа,
                                    "fees": комиссии,
                                    "spread": спред_проценты,
                                    "spread_usdt": спред_юсдт,
                                    "funding_long": мин_фандинг,
                                    "funding_long_time": мин_время_к_фандингy,
                                    "funding_short": фандинг,
                                    "funding_short_time": время_к_фандингy,
                                    "volume": volume,
                                }
                            )

        if возможности:  # Если спред найден

            # for k in возможности:
            #     if k.get("symbol") in черный_список:
            #         #await asyncio.sleep(2)
            #         return

            beast = max(возможности, key=lambda x: x["spread_usdt"])

            msg = (
                f"Валютная пара: {beast.get('symbol')}<br><br>"
                f"Общий объем: {(beast.get('volume') * 2)} USDT<br><br>"
                f"Вход в сделку на {beast.get('volume')} USDT<br>"
                f"Лонг на: {beast.get('ex_long_id')}<br>По цене: {beast.get('ex_long'):.6f}<br>"
                f"Фандинг: {beast.get('funding_long'):.6f}%<br>Время: {beast.get('funding_long_time')}<br><br>"
                f"Возможности шорта:<br><br>"
            )

            for data in возможности:
                if data.get("volume") == beast.get("volume") and data.get(
                    "ex_long_id"
                ) == beast.get("ex_long_id"):
                    msg += (
                        f"Вход в сделку на {beast.get('volume')} USDT<br>"
                        f"Шорт на {data.get('ex_short_id')}<br>По цене: {data.get('ex_short'):.6f}<br>"
                        f"Комиссии: {data.get('fees')}%<br>"
                        f"Фандинг: {data.get('funding_short'):.6f}%\nВремя: {data.get('funding_short_time')}<br>"
                        f"Спред: {data.get('spread'):.2f}% / {data.get('spread_usdt'):.2f}$ / {(data.get('volume') * 2)}$<br><br>"
                    )

            try:
                #print(f'отправка в тг {symbol}')
                
                # айдишка = await bot.send_message(
                #     chat_id=чат_айди, text=msg
                # )
                # айди = айдишка.message_id
                айди = await send_message_to_site(msg)


                asyncio.create_task(арбитраж_повтор(пары, мин_обьем, макс_обьем, шаг, session, айди))
                async with работающие_арбитражи_lock:
                    работающие_арбитражи.add(symbol)
                #await asyncio.sleep(2)
                return
                


            except Exception as e:
                await asyncio.sleep(23)
                print(f"Ошибка в функции арбитража: {e}")
        else:
            #await asyncio.sleep(2)
            return



async def арбитраж_повтор(пары, мин_обьем, макс_обьем, шаг, session, айди):
    global работающие_арбитражи_lock
    global работающие_арбитражи
    пустых_итераций = 0
    msg_for_edit = None
    start_time = time.time()
   #async with светлофор:
    while True:
        await asyncio.sleep(2.5)
        for symbol, exchanges in пары.items():
            словарь_с_ценами = defaultdict(lambda: defaultdict(dict))
            tasks = []  # создаём список задач для конкретной монеты
            meta = []   # храним (биржа, данные)

            # создаём задачи на все биржи
            for exchange, data in exchanges.items():
                task = asyncio.create_task(
                    safe_fetch_order_book(exchange, symbol, sessions=session)
                )
                tasks.append(task)
                meta.append((exchange, data))

            # ждём, пока все биржи ответят
            order_books = await asyncio.gather(*tasks, return_exceptions=False)

            for (exchange, data), order_book in zip(meta, order_books):
                # if isinstance(order_book, Exception):
                #     print(f"❌ Ошибка {exchange} ({symbol}): {type(order_book).__name__}: {order_book}")
                #     continue

                asks = order_book.get("asks") or []
                bids = order_book.get("bids") or []
                if not asks or not bids:
                    continue

                for volume in range(мин_обьем, макс_обьем + 1, шаг):
                    # ==== VWAP покупка ====
                    remaining_money = volume
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

                    if total_coins == 0:
                        continue

                    buy_avg = total_spent / total_coins

                    # ==== VWAP продажа ====
                    remaining_coins = total_coins
                    total_revenue = 0.0
                    coins_sold = 0.0

                    for price in bids:
                        if remaining_coins <= 0:
                            break
                        actual_coins = min(float(price[1]), remaining_coins)
                        total_revenue += actual_coins * float(price[0])
                        coins_sold += actual_coins
                        remaining_coins -= actual_coins

                    if coins_sold == 0:
                        continue

                    sell_avg = total_revenue / coins_sold

                    словарь_с_ценами[symbol][volume][exchange] = {
                        "buy_avg": buy_avg,
                        "sell_avg": sell_avg,
                        "volume": volume,
                        "fee_maker": data.get("maker"),
                        "fee_taker": data.get("taker"),
                        "funding": data.get("funding"),
                        "get_funding": data.get("get_funding"),
                    }
            if not словарь_с_ценами:
                return
                            
            
            if словарь_с_ценами:
                возможности = []
                #print(f"Прошла работа по монете: {symbol}")
                for symbol, volumes in словарь_с_ценами.items():
                    for volume, exchanges in volumes.items():
                        # ищем биржу с минимальной buy_avg
                        min_exchange = min(
                            exchanges.items(), key=lambda x: x[1]["buy_avg"]
                        )
                        мин_биржа, мин_данные = min_exchange
                        мин_цена = мин_данные.get("buy_avg")
                        мин_тейкер = мин_данные.get("fee_taker")
                        мин_мейкер = мин_данные.get("fee_maker")
                        мин_фандинг = мин_данные.get("funding")
                        мин_время_к_фандингy = мин_данные.get("get_funding")

                        for биржа, дата in exchanges.items():
                            if мин_биржа == биржа:
                                continue
                            цена = дата.get("sell_avg")
                            тейкер = дата.get("fee_taker")
                            мейкер = дата.get("fee_maker")
                            фандинг = дата.get("funding")
                            время_к_фандингy = дата.get("get_funding")

                            комиссии = тейкер + мин_тейкер
                            # фандинг = захожу в лонг = фандинг платят шортистам
                            # мин_фандинг = захожу в шорт = фандинг платять лонгистам

                            if фандинг <= 0 and мин_фандинг <= 0:
                                спред_проценты = (
                                    ((цена - мин_цена) / мин_цена * 100)
                                    - комиссии
                                    #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги

                                    + фандинг
                                    #спред с учетом фандингов даже если основной спред это сам фандинг
                                    #- фандинг
                                    #+ мин_фандинг
                                )

                            elif фандинг >= 0 and мин_фандинг >= 0:
                                спред_проценты = (
                                    ((цена - мин_цена) / мин_цена * 100)
                                    - комиссии
                                    #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги

                                    - мин_фандинг
                                    #спред с учетом фандингов даже если основной спред это сам фандинг
                                    #- фандинг
                                    #+ мин_фандинг
                                )
                            # elif фандинг < 0 and мин_фандинг < 0:
                            #     спред_проценты = (
                            #         ((цена - мин_цена) / мин_цена * 100)
                            #         - комиссии
                            #         #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                            #         + фандинг

                            #         #спред с учетом фандингов даже если основной спред это сам фандинг
                            #         #- фандинг
                            #         #+ мин_фандинг
                            #     )
                            elif фандинг < 0 and мин_фандинг > 0:
                                спред_проценты = (
                                    ((цена - мин_цена) / мин_цена * 100)
                                    - комиссии
                                    #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                                    + фандинг
                                    - мин_фандинг
                                    #спред с учетом фандингов даже если основной спред это сам фандинг
                                    #- фандинг
                                    #+ мин_фандинг
                                )
                            elif фандинг > 0 and мин_фандинг < 0:
                                спред_проценты = (
                                    ((цена - мин_цена) / мин_цена * 100)
                                    - комиссии
                                    #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги

                                    #спред с учетом фандингов даже если основной спред это сам фандинг
                                    #- фандинг
                                    #+ мин_фандинг
                                )
                            # спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии - (фандинг - мин_фандинг)
                            
                            else:
                                # 🛑 резервная защита — если что-то не попало в условия
                                print(f"⚠️ Не попало ни в одно условие: фандинг={фандинг}, мин_фандинг={мин_фандинг}")
                                спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии
                        
                            спред_юсдт = ((volume * 2) / 100) * спред_проценты
                            if спред_юсдт >= 3:
                                возможности.append(
                                    {
                                        "symbol": symbol,
                                        "ex_long": мин_цена,
                                        "ex_long_id": мин_биржа,
                                        "ex_short": цена,
                                        "ex_short_id": биржа,
                                        "fees": комиссии,
                                        "spread": спред_проценты,
                                        "spread_usdt": спред_юсдт,
                                        "funding_long": мин_фандинг,
                                        "funding_long_time": мин_время_к_фандингy,
                                        "funding_short": фандинг,
                                        "funding_short_time": время_к_фандингy,
                                        "volume": volume,
                                    }
                                )

                if возможности:  # Если спред найден
                    пустых_итераций = 0  # сбрасываем счетчик


                    прошедшие_секунды = time.time() - start_time
                    minutes = int(прошедшие_секунды // 60)
                    sec = int(прошедшие_секунды % 60)
                    время_жизни = f'{minutes} минут {sec} секунд'
                    beast = max(возможности, key=lambda x: x["spread_usdt"])

                    msg = (
                        f"Валютная пара: {beast.get('symbol')}<br><br>"
                        f"Общий объем: {(beast.get('volume') * 2)} USDT<br><br>"
                        f"Вход в сделку на {beast.get('volume')} USDT<br>"
                        f"Лонг на: {beast.get('ex_long_id')}<br>По цене: {beast.get('ex_long'):.6f}<br>"
                        f"Фандинг: {beast.get('funding_long'):.6f}%<br>Время: {beast.get('funding_long_time')}<br><br>"
                        f"Возможности шорта:<br><br>"
                    )

                    for data in возможности:
                        if data.get("volume") == beast.get("volume") and data.get(
                            "ex_long_id"
                        ) == beast.get("ex_long_id"):
                            msg += (
                                f"Вход в сделку на {beast.get('volume')} USDT<br>"
                                f"Шорт на {data.get('ex_short_id')}<br>По цене: {data.get('ex_short'):.6f}<br>"
                                f"Комиссии: {data.get('fees')}%<br>"
                                f"Фандинг: {data.get('funding_short'):.6f}%<br>Время: {data.get('funding_short_time')}<br>"
                                f"Спред: {data.get('spread'):.2f}% / {data.get('spread_usdt'):.2f}$ / {(data.get('volume') * 2)}$<br><br>"
                            )

                    try:
                        await send_message_to_site(f"{msg}<br><br>Время жизни: {время_жизни}", message_id=айди)
                        msg_for_edit = (
                            f"{msg}<br><br>Время жизни: {время_жизни}"
                        )


                    except Exception as e:
                        await asyncio.sleep(23)
                        print(f"Ошибка в функции арбитража: {e}")
                    continue

                else:
                    пустых_итераций += 1
                    
                    
                    if пустых_итераций >= 12:
                        try:
                            if msg_for_edit is not None:
                                #print(f'Редакт в тг что спред потерялся {symbol}')
                                # await bot.edit_message_text(
                                #     chat_id=чат_айди,
                                #     message_id=айди,
                                #     text=f"{msg_for_edit}\n\n❌ Спред потерялся",
                                # )
                                await delete_message_from_site(message_id=айди)

                                msg_for_edit = None
                            else:
                                #print(f'Удаление тг если спред хуйню прожил {symbol}')
                                await delete_message_from_site(message_id=айди)


                        except Exception as e:
                            print(f"Ошибка при обработке потери спреда: {e}")
                        async with работающие_арбитражи_lock:
                            работающие_арбитражи.remove(symbol)

                        return

                    else:
                        #await asyncio.sleep(2)
                        continue


async def обновление_словаря():
    global текущий_словарь
    print("📡 Обновление словаря запущено!")

    while True:
        try:
            разделение_словаря = await api()
            if разделение_словаря:
                текущий_словарь = разделение_словаря
                print(f"🔄 Словарь обновлён! Монет: {len(разделение_словаря)}")
            else:
                print("⚠️ API вернуло пустой словарь")
        except Exception as e:
            print(f"❌ Ошибка в обновлении словаря: {e}")

        await asyncio.sleep(15 * 60)



async def арбитраж_бот(session):
    global текущий_словарь

    while True:
        if текущий_словарь:
            async with работающие_арбитражи_lock:
                свободно = 10 - len(работающие_арбитражи)
            print("🚀 Запуск новых арбитражных задач...")
            for chunk in chunk_dict(текущий_словарь, свободно):
                tasks = [
                    asyncio.create_task(
                        арбитраж({symbol: exchanges}, 100, 250, 25, session)
                    )
                    for symbol, exchanges in chunk.items()
                ]
                await asyncio.gather(*tasks)
                await asyncio.sleep(0.9)
        else:
            await asyncio.sleep(5)
            continue



async def on_startup():
    print("🚀 Создание aiohttp сессий...")

    binance_session = aiohttp.ClientSession()
    bybit_session = aiohttp.ClientSession()
    bitget_session = aiohttp.ClientSession()
    bingx_session = aiohttp.ClientSession()
    mexc_session = aiohttp.ClientSession()
    okx_session = aiohttp.ClientSession()
    gate_session = aiohttp.ClientSession()
    htx_session = aiohttp.ClientSession()
    kucoin_session = aiohttp.ClientSession()

    sessions = {
        "binance": binance_session,
        "bybit": bybit_session,
        "bitget": bitget_session,
        "bingx": bingx_session,
        "mexc": mexc_session,
        "okx": okx_session,
        "gateio": gate_session,
        "htx": htx_session,
        "kucoin": kucoin_session,
    }

    print("✅ Сессии созданы")


    asyncio.create_task(обновление_словаря())
    asyncio.create_task(арбитраж_бот(sessions))

    return sessions


async def on_shutdown(sessions):
    print("🛑 Закрываем aiohttp сессии...")
    for name, s in sessions.items():
        await s.close()
        print(f"🔒 {name} закрыта")


# async def телеграм():
#     await dp.start_polling(bot)


# async def main():
#     session = await on_startup()

#     try:
#         await телеграм()
#     finally:
#         await on_shutdown(session)

async def main():
    sessions = await on_startup()

    # Запускаем FastAPI-сервер и бота одновременно
    config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)

    try:
        await server.serve()
    finally:
        await on_shutdown(sessions)

if __name__ == "__main__":
    asyncio.run(main())
