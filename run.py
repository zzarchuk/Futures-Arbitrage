from filtr import api
import asyncio
import aiohttp
from aiogram import Bot, Dispatcher
from orderbook import binance, bybit, bingx, bitget, htx, kucoin, okx, gate, mexc
from collections import defaultdict
import time
#from повтор_арбитража import арбитраж_повтор
мин_спред = 6
текущий_словарь = {}
работающие_арбитражи = set()
работающие_арбитражи_lock = asyncio.Lock()
черный_список = ['KTAUSDT' , 'MEGAUSDT', 'XNAPUSDT', 'NUMIUSDT', 'SVSAUSDT', 'IQUSDT', 'AMUSDT', 'SIGMAISDT', 'ALPHAUSDT', 'BNBHOLDERUSDT', 'UUSDT', 'ALLUSDT', 'ARCUSDT',  'MEMECOINUSDT', 'RICEUSDT', 'BDXNUSDT', 'AURAUSDT', 'METUSDT', 'MONUSDT']

TOKEN = "8414063749:AAFq1sRWe6gSUn6yAQHAZoF1BcmErvzwyvM"
чат_айди = -1002927729443

bot = Bot(token=TOKEN)
dp = Dispatcher()


def chunk_dict(d: dict):
    for k, v in d.items():
        yield {k: v}


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

semaphores = {
    "binance": asyncio.Semaphore(20),
    "kucoin": asyncio.Semaphore(20),
    "mexc": asyncio.Semaphore(10),
    "htx": asyncio.Semaphore(20),
    "bybit": asyncio.Semaphore(20),
    "bingx": asyncio.Semaphore(10),
    "bitget": asyncio.Semaphore(20),
    "gateio": asyncio.Semaphore(20),
    "okx": asyncio.Semaphore(20),
}

пар_на_бота = 30


async def safe_fetch_order_book(
    exchange, pair, sessions, limit=100, retries=4, delay=2
):
    sem = semaphores.get(exchange, asyncio.Semaphore(10))
    for attempt in range(retries):
        async with sem:
            try:
                await asyncio.sleep(2)
                session = sessions.get(exchange)
                if exchange == "binance":

                    result = await binance(pair, limit, session)
                elif exchange == "kucoin":

                    result = await kucoin(pair, limit, session)
                elif exchange == "mexc":

                    result = await mexc(pair, limit, session)
                elif exchange == "htx":

                    result = await htx(pair, limit, session)
                elif exchange == "bybit":

                    result = await bybit(pair, limit, session)
                elif exchange == "bingx":
                    result = await bingx(pair, limit, session)
                elif exchange == "bitget":

                    result = await bitget(pair, limit, session)
                elif exchange == "gateio":

                    result = await gate(pair, limit, session)
                elif exchange == "okx":

                    result = await okx(pair, limit, session)
                else:
                    print(f"Говно какое-то")

                return result
            except Exception as e:
                # print(f"{exchange.id} Ошибка сети при {pair}, попытка {attempt+1}/{retries}: {e}")
                await asyncio.sleep(delay)
    raise Exception(f"Не удалось получить order book для {pair} на {exchange}")


semaphore = asyncio.Semaphore(пар_на_бота)

async def арбитраж(пары, биржи, лимит, мин_обьем, макс_обьем, шаг, session):
    global работающие_арбитражи_lock
    global работающие_арбитражи
    async with semaphore:
        while True:
            все_биржи_на_которых_есть_пара = defaultdict(list)
            for symbol, exchanges in пары.items():
                # print(f'Монета: {symbol}')
                словарь_с_ценами = defaultdict(lambda: defaultdict(dict))
                for exhange, data in exchanges.items():
                    for conn in биржи.values():
                        if exhange == conn:
                            try:
                                order_book = await safe_fetch_order_book(
                                    conn, symbol, sessions=session
                                )
                                if order_book:
                                    все_биржи_на_которых_есть_пара[symbol].append(
                                        conn
                                    )
                                for volume in range(мин_обьем, макс_обьем + 1, шаг):
                                    # ==== VWAP покупка ====
                                    asks = order_book.get("asks") or []
                                    bids = order_book.get("bids") or []
                                    if not asks or not bids:
                                        continue

                                    remaining_money = (
                                        volume  # сколько USDT хотим потратить
                                    )
                                    total_spent = 0.0
                                    total_coins = 0.0

                                    for price in asks:
                                        if remaining_money <= 0:
                                            break
                                        coins_can_buy = remaining_money / float(
                                            price[0]
                                        )
                                        actual_coins = min(
                                            float(price[1]), coins_can_buy
                                        )
                                        total_spent += actual_coins * float(
                                            price[0]
                                        )
                                        total_coins += actual_coins
                                        remaining_money -= actual_coins * float(
                                            price[0]
                                        )

                                    # если ничего не купили — пропускаем
                                    if total_coins == 0:
                                        continue

                                    buy_avg = (
                                        total_spent / total_coins
                                    )  # средняя цена покупки

                                    # ==== VWAP продажа ====
                                    remaining_coins = (
                                        total_coins  # продаём то, что купили
                                    )
                                    total_revenue = 0.0
                                    coins_sold = 0.0

                                    for price in bids:
                                        if remaining_coins <= 0:
                                            break
                                        actual_coins = min(
                                            float(price[1]), remaining_coins
                                        )
                                        total_revenue += actual_coins * float(
                                            price[0]
                                        )
                                        coins_sold += actual_coins
                                        remaining_coins -= actual_coins

                                    # если нечего продать — пропускаем
                                    if coins_sold == 0:
                                        continue

                                    sell_avg = (
                                        total_revenue / coins_sold
                                    )  # средняя цена продажи

                                    словарь_с_ценами[symbol][volume][conn] = {
                                        "buy_avg": buy_avg,
                                        "sell_avg": sell_avg,
                                        "volume": volume,
                                        "fee_maker": data.get("maker"),
                                        "fee_taker": data.get("taker"),
                                        "funding": data.get("funding"),
                                        "get_funding": data.get("get_funding"),
                                    }

                            except Exception as e:
                                print(f"Ошибка тут: {e}")
                                
                
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

                                if фандинг >= 0 and мин_фандинг >= 0:
                                    спред_проценты = (
                                        ((цена - мин_цена) / мин_цена * 100)
                                        - комиссии
                                        #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                                        - фандинг
                                        #- мин_фандинг
                                        #спред с учетом фандингов даже если основной спред это сам фандинг
                                        #- фандинг
                                        #+ мин_фандинг
                                    )
                                if фандинг < 0 and мин_фандинг < 0:
                                    спред_проценты = (
                                        ((цена - мин_цена) / мин_цена * 100)
                                        - комиссии
                                        #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                                        #+ фандинг
                                        + мин_фандинг
                                        #спред с учетом фандингов даже если основной спред это сам фандинг
                                        #- фандинг
                                        #+ мин_фандинг
                                    )
                                if фандинг < 0 and мин_фандинг > 0:
                                    спред_проценты = (
                                        ((цена - мин_цена) / мин_цена * 100)
                                        - комиссии
                                        #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                                        #+ фандинг
                                        #- мин_фандинг
                                        #спред с учетом фандингов даже если основной спред это сам фандинг
                                        #- фандинг
                                        #+ мин_фандинг
                                    )
                                if фандинг > 0 and мин_фандинг < 0:
                                    спред_проценты = (
                                        ((цена - мин_цена) / мин_цена * 100)
                                        - комиссии
                                        #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                                        - фандинг
                                        + мин_фандинг
                                        #спред с учетом фандингов даже если основной спред это сам фандинг
                                        #- фандинг
                                        #+ мин_фандинг
                                    )
                                # спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии - (фандинг - мин_фандинг)
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

                    for k in возможности:
                        if k.get("symbol") in черный_список:
                            return

                    beast = max(возможности, key=lambda x: x["spread_usdt"])

                    msg = (
                        f"Валютная пара: {beast.get('symbol')}\n\n"
                        f"Общий объем: {(beast.get('volume') * 2)} USDT\n\n"
                        f"Вход в сделку на {beast.get('volume')} USDT\n"
                        f"Лонг на: {beast.get('ex_long_id')}\nПо цене: {beast.get('ex_long'):.6f}\n"
                        f"Фандинг: {beast.get('funding_long'):.6f}%\nВремя: {beast.get('funding_long_time')}\n\n"
                        f"Возможности шорта:\n\n"
                    )

                    for data in возможности:
                        if data.get("volume") == beast.get("volume") and data.get(
                            "ex_long_id"
                        ) == beast.get("ex_long_id"):
                            msg += (
                                f"Вход в сделку на {beast.get('volume')} USDT\n"
                                f"Шорт на {data.get('ex_short_id')}\nПо цене: {data.get('ex_short'):.6f}\n"
                                f"Комиссии: {data.get('fees')}%\n"
                                f"Фандинг: {data.get('funding_short'):.6f}%\nВремя: {data.get('funding_short_time')}\n"
                                f"Спред: {data.get('spread'):.2f}% / {data.get('spread_usdt'):.2f}$ / {(data.get('volume') * 2)}$\n\n"
                            )

                    try:
                        #print(f'отправка в тг {symbol}')
                        async with работающие_арбитражи_lock:
                            if symbol in работающие_арбитражи:
                                return
                        
                        айдишка = await bot.send_message(
                            chat_id=чат_айди, text=msg
                        )
                        айди = айдишка.message_id
                        asyncio.create_task(арбитраж_повтор(пары, биржи, мин_обьем, макс_обьем, шаг, session, safe_fetch_order_book, чат_айди, bot, айди))
                        async with работающие_арбитражи_lock:
                            работающие_арбитражи.add(symbol)
                        return
                        


                    except Exception as e:
                        await asyncio.sleep(23)
                        print(f"Ошибка в функции арбитража: {e}")
                else:
                    return



async def арбитраж_повтор(пары, биржи, мин_обьем, макс_обьем, шаг, session, функция_ордербук, чат_айди, bot, айди):
    global работающие_арбитражи_lock
    global работающие_арбитражи
    #print(работающие_арбитражи)
    пустых_итераций = 0
    msg_for_edit = None
    start_time = time.time()
    while True:
        все_биржи_на_которых_есть_пара = defaultdict(list)
        for symbol, exchanges in пары.items():
            # print(f'Монета: {symbol}')
            словарь_с_ценами = defaultdict(lambda: defaultdict(dict))
            for exhange, data in exchanges.items():
                for conn in биржи.values():
                    if exhange == conn:
                        try:
                            order_book = await функция_ордербук(
                                conn, symbol, sessions=session
                            )
                            if order_book:
                                все_биржи_на_которых_есть_пара[symbol].append(
                                    conn
                                )
                            for volume in range(мин_обьем, макс_обьем + 1, шаг):
                                # ==== VWAP покупка ====
                                asks = order_book.get("asks") or []
                                bids = order_book.get("bids") or []
                                if not asks or not bids:
                                    continue

                                remaining_money = (
                                    volume  # сколько USDT хотим потратить
                                )
                                total_spent = 0.0
                                total_coins = 0.0

                                for price in asks:
                                    if remaining_money <= 0:
                                        break
                                    coins_can_buy = remaining_money / float(
                                        price[0]
                                    )
                                    actual_coins = min(
                                        float(price[1]), coins_can_buy
                                    )
                                    total_spent += actual_coins * float(
                                        price[0]
                                    )
                                    total_coins += actual_coins
                                    remaining_money -= actual_coins * float(
                                        price[0]
                                    )

                                # если ничего не купили — пропускаем
                                if total_coins == 0:
                                    continue

                                buy_avg = (
                                    total_spent / total_coins
                                )  # средняя цена покупки

                                # ==== VWAP продажа ====
                                remaining_coins = (
                                    total_coins  # продаём то, что купили
                                )
                                total_revenue = 0.0
                                coins_sold = 0.0

                                for price in bids:
                                    if remaining_coins <= 0:
                                        break
                                    actual_coins = min(
                                        float(price[1]), remaining_coins
                                    )
                                    total_revenue += actual_coins * float(
                                        price[0]
                                    )
                                    coins_sold += actual_coins
                                    remaining_coins -= actual_coins

                                # если нечего продать — пропускаем
                                if coins_sold == 0:
                                    continue

                                sell_avg = (
                                    total_revenue / coins_sold
                                )  # средняя цена продажи

                                словарь_с_ценами[symbol][volume][conn] = {
                                    "buy_avg": buy_avg,
                                    "sell_avg": sell_avg,
                                    "volume": volume,
                                    "fee_maker": data.get("maker"),
                                    "fee_taker": data.get("taker"),
                                    "funding": data.get("funding"),
                                    "get_funding": data.get("get_funding"),
                                }

                        except Exception as e:
                            print(f"Ошибка тут: {e}")
                            
            
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

                            if фандинг >= 0 and мин_фандинг >= 0:
                                спред_проценты = (
                                    ((цена - мин_цена) / мин_цена * 100)
                                    - комиссии
                                    #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                                    - фандинг
                                    
                                    #спред с учетом фандингов даже если основной спред это сам фандинг
                                    #- фандинг
                                    #+ мин_фандинг
                                )
                            if фандинг < 0 and мин_фандинг < 0:
                                спред_проценты = (
                                    ((цена - мин_цена) / мин_цена * 100)
                                    - комиссии
                                    #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                                   
                                    + мин_фандинг
                                    #спред с учетом фандингов даже если основной спред это сам фандинг
                                    #- фандинг
                                    #+ мин_фандинг
                                )
                            if фандинг < 0 and мин_фандинг > 0:
                                спред_проценты = (
                                    ((цена - мин_цена) / мин_цена * 100)
                                    - комиссии
                                    #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                                    #+ фандинг
                                    #- мин_фандинг
                                    #спред с учетом фандингов даже если основной спред это сам фандинг
                                    #- фандинг
                                    #+ мин_фандинг
                                )
                            if фандинг > 0 and мин_фандинг < 0:
                                спред_проценты = (
                                    ((цена - мин_цена) / мин_цена * 100)
                                    - комиссии
                                    #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                                    - фандинг
                                    + мин_фандинг
                                    #спред с учетом фандингов даже если основной спред это сам фандинг
                                    #- фандинг
                                    #+ мин_фандинг
                                )
                            # спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии - (фандинг - мин_фандинг)
                            спред_юсдт = ((volume * 2) / 100) * спред_проценты
                            if спред_юсдт >= 1:
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
                    f"Валютная пара: {beast.get('symbol')}\n\n"
                    f"Общий объем: {(beast.get('volume') * 2)} USDT\n\n"
                    f"Вход в сделку на {beast.get('volume')} USDT\n"
                    f"Лонг на: {beast.get('ex_long_id')}\nПо цене: {beast.get('ex_long'):.6f}\n"
                    f"Фандинг: {beast.get('funding_long'):.6f}%\nВремя: {beast.get('funding_long_time')}\n\n"
                    f"Возможности шорта:\n\n"
                )

                for data in возможности:
                    if data.get("volume") == beast.get("volume") and data.get(
                        "ex_long_id"
                    ) == beast.get("ex_long_id"):
                        msg += (
                            f"Вход в сделку на {beast.get('volume')} USDT\n"
                            f"Шорт на {data.get('ex_short_id')}\nПо цене: {data.get('ex_short'):.6f}\n"
                            f"Комиссии: {data.get('fees')}%\n"
                            f"Фандинг: {data.get('funding_short'):.6f}%\nВремя: {data.get('funding_short_time')}\n"
                            f"Спред: {data.get('spread'):.2f}% / {data.get('spread_usdt'):.2f}$ / {(data.get('volume') * 2)}$\n\n"
                        )

                try:


                    #print(f'редакт тг {symbol}')
                    
                    await bot.edit_message_text(
                        chat_id=чат_айди,
                        message_id=айди,
                        text=f"{msg}\n\nВремя жизни: {время_жизни}",
                    )
                    msg_for_edit = (
                        f"{msg}\n\nВремя жизни: {время_жизни}"
                    )
                    await asyncio.sleep(2)

                except Exception as e:
                    await asyncio.sleep(23)
                    print(f"Ошибка в функции арбитража: {e}")
                continue

            else:
                пустых_итераций += 1
                
                
                if пустых_итераций >= 1:
                    try:
                        if msg_for_edit is not None:
                            #print(f'Редакт в тг что спред потерялся {symbol}')
                            await bot.edit_message_text(
                                chat_id=чат_айди,
                                message_id=айди,
                                text=f"{msg_for_edit}\n\n❌ Спред потерялся",
                            )
                            msg_for_edit = None
                        else:
                            #print(f'Удаление тг если спред хуйню прожил {symbol}')
                            await bot.delete_message(
                                chat_id=чат_айди, message_id=айди
                            )

                    except Exception as e:
                        print(f"Ошибка при обработке потери спреда: {e}")
                    async with работающие_арбитражи_lock:
                        #print(работающие_арбитражи)
                        работающие_арбитражи.remove(symbol)
                    return
                else:
                    #print(f'спред пока что пропал {symbol}  {пустых_итераций}') 
                    continue

# async def арбитраж(пары, биржи, лимит, мин_обьем, макс_обьем, шаг, session):
#     айдишники = None
#     пустых_итераций = 0
#     msg_for_edit = None
#     async with semaphore:
#         start_time = time.time()
#         while True:
#             все_биржи_на_которых_есть_пара = defaultdict(list)
#             for symbol, exchanges in пары.items():
#                 # print(f'Монета: {symbol}')
#                 словарь_с_ценами = defaultdict(lambda: defaultdict(dict))
#                 for exhange, data in exchanges.items():
#                     for conn in биржи.values():
#                         if exhange == conn:
#                             try:
#                                 order_book = await safe_fetch_order_book(
#                                     conn, symbol, sessions=session
#                                 )
#                                 if order_book:
#                                     все_биржи_на_которых_есть_пара[symbol].append(
#                                         conn
#                                     )
#                                 for volume in range(мин_обьем, макс_обьем + 1, шаг):
#                                     # ==== VWAP покупка ====
#                                     asks = order_book.get("asks") or []
#                                     bids = order_book.get("bids") or []
#                                     if not asks or not bids:
#                                         continue

#                                     remaining_money = (
#                                         volume  # сколько USDT хотим потратить
#                                     )
#                                     total_spent = 0.0
#                                     total_coins = 0.0

#                                     for price in asks:
#                                         if remaining_money <= 0:
#                                             break
#                                         coins_can_buy = remaining_money / float(
#                                             price[0]
#                                         )
#                                         actual_coins = min(
#                                             float(price[1]), coins_can_buy
#                                         )
#                                         total_spent += actual_coins * float(
#                                             price[0]
#                                         )
#                                         total_coins += actual_coins
#                                         remaining_money -= actual_coins * float(
#                                             price[0]
#                                         )

#                                     # если ничего не купили — пропускаем
#                                     if total_coins == 0:
#                                         continue

#                                     buy_avg = (
#                                         total_spent / total_coins
#                                     )  # средняя цена покупки

#                                     # ==== VWAP продажа ====
#                                     remaining_coins = (
#                                         total_coins  # продаём то, что купили
#                                     )
#                                     total_revenue = 0.0
#                                     coins_sold = 0.0

#                                     for price in bids:
#                                         if remaining_coins <= 0:
#                                             break
#                                         actual_coins = min(
#                                             float(price[1]), remaining_coins
#                                         )
#                                         total_revenue += actual_coins * float(
#                                             price[0]
#                                         )
#                                         coins_sold += actual_coins
#                                         remaining_coins -= actual_coins

#                                     # если нечего продать — пропускаем
#                                     if coins_sold == 0:
#                                         continue

#                                     sell_avg = (
#                                         total_revenue / coins_sold
#                                     )  # средняя цена продажи

#                                     словарь_с_ценами[symbol][volume][conn] = {
#                                         "buy_avg": buy_avg,
#                                         "sell_avg": sell_avg,
#                                         "volume": volume,
#                                         "fee_maker": data.get("maker"),
#                                         "fee_taker": data.get("taker"),
#                                         "funding": data.get("funding"),
#                                         "get_funding": data.get("get_funding"),
#                                     }

#                             except Exception as e:
#                                 print(f"Ошибка тут: {e}")
                                
                
#                 if словарь_с_ценами:
#                     возможности = []
#                     #print(f"Прошла работа по монете: {symbol}")
#                     for symbol, volumes in словарь_с_ценами.items():
#                         for volume, exchanges in volumes.items():
#                             # ищем биржу с минимальной buy_avg
#                             min_exchange = min(
#                                 exchanges.items(), key=lambda x: x[1]["buy_avg"]
#                             )
#                             мин_биржа, мин_данные = min_exchange
#                             мин_цена = мин_данные.get("buy_avg")
#                             мин_тейкер = мин_данные.get("fee_taker")
#                             мин_мейкер = мин_данные.get("fee_maker")
#                             мин_фандинг = мин_данные.get("funding")
#                             мин_время_к_фандингy = мин_данные.get("get_funding")

#                             for биржа, дата in exchanges.items():
#                                 if мин_биржа == биржа:
#                                     continue
#                                 цена = дата.get("sell_avg")
#                                 тейкер = дата.get("fee_taker")
#                                 мейкер = дата.get("fee_maker")
#                                 фандинг = дата.get("funding")
#                                 время_к_фандингy = дата.get("get_funding")

#                                 комиссии = тейкер + мин_тейкер
#                                 # фандинг = захожу в лонг = фандинг платят шортистам
#                                 # мин_фандинг = захожу в шорт = фандинг платять лонгистам

#                                 if фандинг >= 0 and мин_фандинг >= 0:
#                                     спред_проценты = (
#                                         ((цена - мин_цена) / мин_цена * 100)
#                                         - комиссии
#                                         #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
#                                         - фандинг
#                                         #- мин_фандинг
#                                         #спред с учетом фандингов даже если основной спред это сам фандинг
#                                         #- фандинг
#                                         #+ мин_фандинг
#                                     )
#                                 if фандинг < 0 and мин_фандинг < 0:
#                                     спред_проценты = (
#                                         ((цена - мин_цена) / мин_цена * 100)
#                                         - комиссии
#                                         #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
#                                         #+ фандинг
#                                         + мин_фандинг
#                                         #спред с учетом фандингов даже если основной спред это сам фандинг
#                                         #- фандинг
#                                         #+ мин_фандинг
#                                     )
#                                 if фандинг < 0 and мин_фандинг > 0:
#                                     спред_проценты = (
#                                         ((цена - мин_цена) / мин_цена * 100)
#                                         - комиссии
#                                         #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
#                                         #+ фандинг
#                                         #- мин_фандинг
#                                         #спред с учетом фандингов даже если основной спред это сам фандинг
#                                         #- фандинг
#                                         #+ мин_фандинг
#                                     )
#                                 if фандинг > 0 and мин_фандинг < 0:
#                                     спред_проценты = (
#                                         ((цена - мин_цена) / мин_цена * 100)
#                                         - комиссии
#                                         #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
#                                         - фандинг
#                                         + мин_фандинг
#                                         #спред с учетом фандингов даже если основной спред это сам фандинг
#                                         #- фандинг
#                                         #+ мин_фандинг
#                                     )
#                                 # спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии - (фандинг - мин_фандинг)
#                                 спред_юсдт = ((volume * 2) / 100) * спред_проценты
#                                 if спред_юсдт >= 3.5:
#                                     возможности.append(
#                                         {
#                                             "symbol": symbol,
#                                             "ex_long": мин_цена,
#                                             "ex_long_id": мин_биржа,
#                                             "ex_short": цена,
#                                             "ex_short_id": биржа,
#                                             "fees": комиссии,
#                                             "spread": спред_проценты,
#                                             "spread_usdt": спред_юсдт,
#                                             "funding_long": мин_фандинг,
#                                             "funding_long_time": мин_время_к_фандингy,
#                                             "funding_short": фандинг,
#                                             "funding_short_time": время_к_фандингy,
#                                             "volume": volume,
#                                         }
#                                     )

#                 if возможности:  # Если спред найден
#                     пустых_итераций = 0  # сбрасываем счетчик

#                     for k in возможности:
#                         if k.get("symbol") in черный_список:
#                             return

#                     прошедшие_секунды = time.time() - start_time
#                     minutes = int(прошедшие_секунды // 60)
#                     sec = int(прошедшие_секунды % 60)
#                     время_жизни = f'{minutes} минут {sec} секунд'
#                     beast = max(возможности, key=lambda x: x["spread_usdt"])

#                     msg = (
#                         f"Валютная пара: {beast.get('symbol')}\n\n"
#                         f"Общий объем: {(beast.get('volume') * 2)} USDT\n\n"
#                         f"Вход в сделку на {beast.get('volume')} USDT\n"
#                         f"Лонг на: {beast.get('ex_long_id')}\nПо цене: {beast.get('ex_long'):.6f}\n"
#                         f"Фандинг: {beast.get('funding_long'):.6f}%\nВремя: {beast.get('funding_long_time')}\n\n"
#                         f"Возможности шорта:\n\n"
#                     )

#                     for data in возможности:
#                         if data.get("volume") == beast.get("volume") and data.get(
#                             "ex_long_id"
#                         ) == beast.get("ex_long_id"):
#                             msg += (
#                                 f"Вход в сделку на {beast.get('volume')} USDT\n"
#                                 f"Шорт на {data.get('ex_short_id')}\nПо цене: {data.get('ex_short'):.6f}\n"
#                                 f"Комиссии: {data.get('fees')}%\n"
#                                 f"Фандинг: {data.get('funding_short'):.6f}%\nВремя: {data.get('funding_short_time')}\n"
#                                 f"Спред: {data.get('spread'):.2f}% / {data.get('spread_usdt'):.2f}$ / {(data.get('volume') * 2)}$\n\n"
#                             )

#                     try:
#                         if айдишники is None:
#                             #print(f'отправка в тг {symbol}')
#                             айди_сообщения = await bot.send_message(
#                                 chat_id=чат_айди, text=msg
#                             )
#                             айдишники = айди_сообщения.message_id

#                         else:
#                             if minutes >= 90:
#                                 черный_список.append(symbol)
#                                 with open('черный_список.txt', 'a', encoding='utf-8') as w:
#                                     w.write(f'{symbol}, ')
#                                 await bot.edit_message_text(chat_id=чат_айди, message_id=айдишники, text=f'{msg}\n\nДобавилося в черный список')
#                                 return
#                             else:
#                                 #print(f'редакт тг {symbol}')
#                                 await asyncio.sleep(2)
#                                 await bot.edit_message_text(
#                                     chat_id=чат_айди,
#                                     message_id=айдишники,
#                                     text=f"{msg}\n\nВремя жизни: {время_жизни}",
#                                 )
#                                 msg_for_edit = (
#                                     f"{msg}\n\nВремя жизни: {время_жизни}"
#                                 )

#                     except Exception as e:
#                         await asyncio.sleep(23)
#                         print(f"Ошибка в функции арбитража: {e}")
#                     continue

#                 else:
#                     пустых_итераций += 1
                    
#                     if айдишники is None:
#                         return
                    
#                     if пустых_итераций >= 5:
#                         try:
#                             if msg_for_edit is not None:
#                                 #print(f'Редакт в тг что спред потерялся {symbol}')
#                                 await bot.edit_message_text(
#                                     chat_id=чат_айди,
#                                     message_id=айдишники,
#                                     text=f"{msg_for_edit}\n\n❌ Спред потерялся",
#                                 )
#                                 msg_for_edit = None
#                             else:
#                                 #print(f'Удаление тг если спред хуйню прожил {symbol}')
#                                 await bot.delete_message(
#                                     chat_id=чат_айди, message_id=айдишники
#                                 )

#                         except Exception as e:
#                             print(f"Ошибка при обработке потери спреда: {e}")
#                         return
#                     else:
#                         #print(f'спред пока что пропал {symbol}  {пустых_итераций}') 
#                         continue


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
            print("🚀 Запуск новых арбитражных задач...")
            tasks = []
            for пара in chunk_dict(текущий_словарь):
                task = asyncio.create_task(
                    арбитраж(пара, биржи, 100, 100, 300, 25, session)
                )
                tasks.append(task)
            await asyncio.gather(*tasks, return_exceptions=True)
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


async def телеграм():
    await dp.start_polling(bot)


async def main():
    session = await on_startup()

    try:
        await телеграм()
    finally:
        await on_shutdown(session)

if __name__ == "__main__":
    asyncio.run(main())
