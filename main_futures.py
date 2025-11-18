from web import send_message_to_site, app, delete_message_from_site
import uvicorn
from testik import apishechka
import asyncio
import aiohttp
from collections import defaultdict
import time
from filtr import get_time_until_funding

sem = asyncio.Semaphore(11)
мин_спред = 6
zz = set()
работающие_арбитражи = set()
работающие_арбитражи_lock = asyncio.Lock()
#черный_список = ['BTCUSDT']
черный_список = ['LIGHTUSDT', 'OMUSDT', 'KTAUSDT', 'MEGAUSDT', 'XNAPUSDT', 'NUMIUSDT', 'SVSAUSDT', 'IQUSDT', 'AMUSDT', 'SIGMAISDT', 'ALPHAUSDT', 'BNBHOLDERUSDT', 'UUSDT', 'ALLUSDT', 'ARCUSDT',  'MEMECOINUSDT', 'RICEUSDT', 'BDXNUSDT', 'AURAUSDT', 'METUSDT', 'MONUSDT']
светлофор_для_арбитража = asyncio.Semaphore(8)
светлофор = asyncio.Semaphore(10)
светлофор_для_фандинга = asyncio.Semaphore(10)

async def safe_fetch_fundings(exchange, symbol, sessions, retries=5, delay=0.7):
    #async with светлофор_для_фандинга:
        #await asyncio.sleep(2)
    for i in range(retries):
        try:
            session = sessions.get(exchange)
            if exchange == "bingx":
                #print('Попытка бингх')
                symboll = symbol.replace('USDT', '-USDT')
                async with session.get(
                    url=f"https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex?symbol={symboll}"
                ) as response:
                    fundings = await response.json()
                    #print(f'BINGX  {fundings}\n\n')
                    return float(
                        fundings["data"].get("lastFundingRate")
                    ) * 100, await get_time_until_funding(
                        exchange_name=exchange,
                        funding_timestamp=fundings["data"].get("nextFundingTime"),
                    )
            elif exchange == "binance":
                async with session.get(
                    url=f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}"
                ) as response:
                    fundings = await response.json()
                    return float(
                        fundings.get("lastFundingRate")
                    ) * 100, await get_time_until_funding(
                        exchange_name=exchange,
                        funding_timestamp=fundings.get("nextFundingTime"),
                    )
            elif exchange == "bybit":
                url = "https://api.bybit.com/v5/market/tickers"
                params = {
                    "category": "linear",  # "spot", "linear", "inverse"
                    "symbol": symbol,
                }
                async with session.get(url, params=params) as resp:
                    fundings = await resp.json()
                    return float(
                        fundings["result"]["list"][0].get("fundingRate")
                    ) * 100, await get_time_until_funding(
                        exchange_name=exchange,
                        funding_timestamp=int(
                            fundings["result"]["list"][0].get("nextFundingTime")
                        ),
                    )
            elif exchange == "bitget":
                async with session.get(
                    url=f"https://api.bitget.com/api/v2/mix/market/current-fund-rate?symbol={symbol}&productType=usdt-futures"
                ) as respone:
                    fundings = await respone.json()
                    return float(
                        fundings["data"][0].get("fundingRate")
                    ) * 100, await get_time_until_funding(
                        exchange_name=exchange,
                        funding_timestamp=int(
                            fundings["data"][0].get("nextUpdate")
                        ),
                    )
            elif exchange == "gateio":
                async with session.get(
                    url=f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{symbol.replace('USDT', '_USDT')}",
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                    },
                ) as respone:
                    fundings_and_fees_gate = await respone.json()
                    return float(
                        fundings_and_fees_gate.get("funding_rate")
                    ) * 100, await get_time_until_funding(
                        exchange_name=exchange,
                        funding_timestamp=int(
                            fundings_and_fees_gate.get("funding_next_apply")
                        ),
                    )
            elif exchange == "htx":
                async with session.get(
                    url=f"https://api.hbdm.com/linear-swap-api/v1/swap_funding_rate?contract_code={symbol.replace('USDT', '-USDT')}"
                ) as respone:
                    
                    try:
                        fundings = await respone.json(content_type=None)
                        
                    except Exception:
                        return 0, 'нет данных'
                    return float(
                        fundings["data"].get("funding_rate")
                    ) * 100, await get_time_until_funding(
                        exchange_name=exchange,
                        funding_timestamp=int(fundings["data"].get("funding_time")),
                    )
            elif exchange == "kucoin":
                async with session.get(
                    f"https://api-futures.kucoin.com/api/v1/contracts/{symbol.replace('USDT', 'USDTM')}"
                ) as respone:
                    fundings_and_fees_kucoin = await respone.json()
                    return float(
                        fundings_and_fees_kucoin["data"].get("fundingFeeRate")
                    ) * 100, await get_time_until_funding(
                        exchange_name=exchange,
                        funding_timestamp=int(
                            fundings_and_fees_kucoin["data"].get(
                                "nextFundingRateDateTime"
                            )
                        ),
                    )
            elif exchange == "okx":
                async with session.get(
                    url=f"https://www.okx.com/api/v5/public/funding-rate?instId={symbol.replace('USDT', '-USDT-SWAP')}"
                ) as respone:
                    fundings = await respone.json()
                    return float(
                        fundings["data"][0].get("fundingRate")
                    ) * 100, await get_time_until_funding(
                        exchange_name=exchange,
                        funding_timestamp=int(
                            fundings["data"][0].get("fundingTime")
                        ),
                    )
            elif exchange == "mexc":
                async with session.get(
                    url=f"https://contract.mexc.com/api/v1/contract/funding_rate/{symbol.replace('USDT', '_USDT')}"
                ) as respone:
                    fundings = await respone.json()
                    return fundings["data"].get(
                        "fundingRate"
                    ) * 100, await get_time_until_funding(
                        exchange_name=exchange,
                        funding_timestamp=fundings["data"].get("nextSettleTime"),
                    )
            elif exchange == "lbank":
                # async with session.get(
                #     url=f"https://lbkperp.lbank.com/cfd/openApi/v1/pub/marketData?productGroup=SwapU"
                # ) as respone:
                #     fundings = await respone.json()
                #     for k in fundings['data']:
                #         if k == symbol:
                #             return float(k.get('fundingRate')) * 100, await get_time_until_funding(exchange_name=exchange, funding_timestamp=k.get('nextFeeTime'))
                return 0, 'нет данных'
            else:
                print("хуйня")
        except Exception as e:
            #print(f"NetworkError на {exchange} {symbol}, попытка {i+1}/{retries}: {e}")
            await asyncio.sleep(delay)
    #raise Exception(f"Не удалось получить funding для {symbol} на {exchange}")
    return None


async def safe_fetch_order_book(
    exchange, pair, sessions, limit=100, retries=3, delay=0.5
):
    #async with светлофор:
        #await asyncio.sleep(2)
    for attempt in range(retries):
        try:
            session = sessions.get(exchange)
            if exchange == "binance":
                params = {"symbol": pair, "limit": limit}

                async with session.get(
                    url="https://fapi.binance.com/fapi/v1/depth", params=params
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book["asks"]
                    bids = order_book["bids"]
                    return {"asks": asks, "bids": bids}
            elif exchange == "kucoin":
                z = pair.replace("USDT", "USDTM")

                async with session.get(
                    f"https://api-futures.kucoin.com/api/v1/level2/depth{limit}?symbol={z}"
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book["data"]["asks"]
                    bids = order_book["data"]["bids"]
                    return {"asks": asks, "bids": bids}
            elif exchange == "mexc":
                params = {"limit": 50}
                async with session.get(
                    url=f"https://contract.mexc.com/api/v1/contract/depth/{pair.replace('USDT', '_USDT')}",
                    params=params,
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book["data"]["asks"]
                    bids = order_book["data"]["bids"]
                    return {"asks": asks, "bids": bids}
            elif exchange == "htx":
                params = {"contract_code": pair.replace("USDT", "-USDT"), "type": "step0"}
                async with session.get(
                    url="https://api.hbdm.com/linear-swap-ex/market/depth", params=params
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book["tick"]["asks"]
                    bids = order_book["tick"]["bids"]
                    return {"asks": asks, "bids": bids}
            elif exchange == "bybit":
                params = {"category": "linear", "symbol": pair, "limit": limit}
                async with session.get(
                    url="https://api.bybit.com/v5/market/orderbook", params=params
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book["result"]["a"]
                    bids = order_book["result"]["b"]
                    return {"asks": asks, "bids": bids}
            elif exchange == "bingx":
                params = {"symbol": pair.replace("USDT", "-USDT"), "limit": limit}
                async with session.get(
                    url="https://open-api.bingx.com/openApi/swap/v2/quote/depth", params=params
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book["data"]["asks"]
                    bids = order_book["data"]["bids"]
                    return {"asks": asks, "bids": bids}
            elif exchange == "bitget":
                params = {"symbol": pair, "productType": "USDT-FUTURES", "limit": limit}
                async with session.get(
                    url="https://api.bitget.com/api/v2/mix/market/merge-depth", params=params
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book["data"]["asks"]
                    bids = order_book["data"]["bids"]
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
                    
                    
                    return {"asks": asks, "bids": bids}
            elif exchange == "okx":
                params = {"instId": pair.replace("USDT", "-USDT-SWAP"), "sz": limit}
                async with session.get(
                    url="https://www.okx.com/api/v5/market/books", params=params
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book["data"][0]["asks"]
                    bids = order_book["data"][0]["bids"]
                    return {"asks": asks, "bids": bids}
            elif exchange == 'lbank':
                #print('работа')
                async with session.get(
                    url=f"https://lbkperp.lbank.com/cfd/openApi/v1/pub/marketOrder?depth=50&symbol={pair}"
                ) as respone:
                    data = await respone.json()
                    
                    asks = data['data']['asks']
                    bids = data['data']['bids']

                    formatted_asks = []
                    for ask in asks:
                        price = float(ask['price'])
                        volume = float(ask['volume'])
                        orders = int(ask['orders'])
                        formatted_asks.append([price, volume])
                        
                    formatted_bids = []
                    for bid in bids:
                        price = float(bid['price'])
                        volume = float(bid['volume'])
                        orders = int(bid['orders'])
                        formatted_bids.append([price, volume])
                    #print(f'\n\nasks: {formatted_asks}\nbids{formatted_bids}\n{pair}\n\n')
                    return {"asks": formatted_asks, "bids": formatted_bids}
            else:
                print(f"Говно какое-то")

            #return result
        except Exception as e:
            print(f"{exchange} Ошибка {pair}, попытка {attempt+1}/{retries}: {e}")
            await asyncio.sleep(delay)
    #raise Exception(f"Не удалось получить order book для {pair} на {exchange}")
    return None

async def fetch_orderbook_and_funding(exchange, symbol, session):
    """
    Получаем ордербук и фандинг для конкретной биржи и символа.
    Возвращаем кортеж: (exchange, order_book, funding_info)
    """
    # async with sem:
    #     await asyncio.sleep(2.2)
    try:
        order_book = await safe_fetch_order_book(exchange, symbol, session)
    except Exception as e:
        print(f"Ошибка при fetch order book на {exchange}: {e}")
        order_book = None
        
        
    try:
        funding_info = await safe_fetch_fundings(exchange, symbol, session)
    except Exception as e:
        print(f"Ошибка при fetch funding на {exchange}: {e}")
        funding_info = (0, "нет данных")
        
    if funding_info is None:
        funding_info = (0, "нет данных")
            
    return exchange, order_book, funding_info

async def арбитраж(пары, мин_обьем, макс_обьем, шаг, session):
    global работающие_арбитражи_lock
    global работающие_арбитражи
    for symbol, exchanges in пары.items():
             
        min_exchange = min(exchanges.items(), key=lambda x: x[1]["price"])

        min_exchange_name = min_exchange[0]
        min_exchange_price = min_exchange[1]["price"]
        
        for exchange, price in exchanges.items():
            if exchange == min_exchange_name:
                continue
            avg_price = price['price']
            
            if min_exchange_price != 0:
                spread = ((avg_price - min_exchange_price) / min_exchange_price * 100)
            else:
                spread = 0
                
                
            if spread >= 2:
                
                # if symbol not in работающие_арбитражи:
                #     async with работающие_арбитражи_lock:
                #         работающие_арбитражи.add(symbol)

                # #await send_message_to_site(text=f'Монета {symbol}<br><br>Спред {spread}<br><br>Лонг на {min_exchange_name} {min_exchange_price}<br><br>Шорт на {exchange} {avg_price}')
                #     asyncio.create_task(арбитраж_повтор({symbol: exchanges}, мин_обьем=мин_обьем, макс_обьем=макс_обьем, шаг=шаг, session=session))
                async with работающие_арбитражи_lock:
                    if symbol in работающие_арбитражи or symbol in черный_список:
                        break
                    работающие_арбитражи.add(symbol)
                #zz.add(symbol)
                #print(symbol)
                # теперь безопасно запускать фоновый арбитраж
                asyncio.create_task(
                    арбитраж_повтор({symbol: exchanges}, мин_обьем, макс_обьем, шаг, session)
                )
                #break
            else:
                continue
    #print(zz)
    #print(len(zz))
    #print(f'После арбитража {len(работающие_арбитражи)}')
                
                
        
            



async def арбитраж_повтор(пары, мин_обьем, макс_обьем, шаг, session):
    global работающие_арбитражи_lock
    global работающие_арбитражи
    id = None
    start_time = time.time()
    async with sem:
        try:
            while True:
                for symbol, exchanges in пары.items():
                    
                    словарь_с_ценами = defaultdict(lambda: defaultdict(dict))
                    tasks = []

                    for exchange, data in exchanges.items():
                        task = asyncio.create_task(
                            fetch_orderbook_and_funding(exchange, symbol, session)
                        )
                        tasks.append(task)


                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    #print(f'{results}\n\n')
                    valid_results = [
                        r for r in results 
                        if isinstance(r, tuple) 
                        and len(r) == 3 
                        and isinstance(r[1], dict)
                    ]

                    if len(valid_results) < 2:
                        #print(f"⚠️ {symbol}: только {len(valid_results)} бирж, нужно минимум 2")
                        #print(valid_results)
                        if id is not None:
                            await delete_message_from_site(message_id=id)
                            async with работающие_арбитражи_lock:
                                работающие_арбитражи.discard(symbol)
                            return
                        else:    
                            async with работающие_арбитражи_lock:
                                работающие_арбитражи.discard(symbol)
                            return
                    
                    for exchange, order_book, funding_info in valid_results:



                        asks = order_book.get("asks") or []
                        bids = order_book.get("bids") or []
                        if not asks or not bids:
                            continue

                        funding_rate, funding_time = (0, "нет данных")
                        if isinstance(funding_info, tuple):
                            funding_rate, funding_time = funding_info

                        # теперь считаем VWAP
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
                                "fee_maker": 0.002,
                                "fee_taker": 0.006,
                                "funding": funding_rate,
                                "get_funding": funding_time,
                            }
                            #print(словарь_с_ценами)
                    if not словарь_с_ценами:
                        async with работающие_арбитражи_lock:
                            работающие_арбитражи.discard(symbol)
                        if id != None:    
                            await delete_message_from_site(message_id=id)
                        #print(symbol)
                        await asyncio.sleep(2)
                        return
                                    
                    
                    if словарь_с_ценами:
                        возможности = []
                        #print(f"Прошла работа по монете: {symbol}")
                        for symbol, volumes in словарь_с_ценами.items():
                            for volume, exchanges in volumes.items():
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

                                    funding_spread = фандинг - мин_фандинг
                                    курсовой = ((цена - мин_цена) / мин_цена * 100)
                                    spread_total = ((цена - мин_цена) / мин_цена * 100) - комиссии + funding_spread
                                    спред_юсдт = ((volume * 2) / 100) * spread_total

                                    if spread_total >= 1.5:
                                        возможности.append(
                                            {
                                                "symbol": symbol,
                                                "ex_long": мин_цена,
                                                "ex_long_id": мин_биржа,
                                                "ex_short": цена,
                                                "ex_short_id": биржа,
                                                "fees": комиссии,
                                                "spread_total": spread_total,
                                                "spread_usdt": спред_юсдт,
                                                'funding_spread': funding_spread,
                                                'курсовой': курсовой,
                                                "funding_long": мин_фандинг,
                                                "funding_long_time": мин_время_к_фандингy,
                                                "funding_short": фандинг,
                                                "funding_short_time": время_к_фандингy,
                                                "volume": volume,
                                            }
                                        )
                                        

                        if возможности:


                            прошедшие_секунды = time.time() - start_time
                            minutes = int(прошедшие_секунды // 60)
                            sec = int(прошедшие_секунды % 60)
                            время_жизни = f'{minutes} минут {sec} секунд'
                            beast = max(возможности, key=lambda x: x["spread_usdt"])

                            msg = (
                                f"Валютная пара: {beast.get('symbol')}<br><br>"
                                f"Общий объем: {(beast.get('volume') * 2)} USDT<br><br>"
                                f"Лонг {beast.get('ex_long_id')} {beast.get('volume')} USDT<br>"
                                f"По цене: {beast.get('ex_long'):.6f}<br>"
                                f"Фандинг: {beast.get('funding_long'):.2f}%          Время: {beast.get('funding_long_time')}<br><br>"
                            )

                            for data in возможности:
                                if data.get("volume") == beast.get("volume") and data.get(
                                    "ex_long_id"
                                ) == beast.get("ex_long_id"):
                                    msg += (
                                        f"Шорт {data.get('ex_short_id')} {beast.get('volume')} USDT<br>"
                                        f"По цене: {data.get('ex_short'):.6f}<br>"
                                        f"Комиссии: {data.get('fees')}%<br>"
                                        f"Фандинг: {data.get('funding_short'):.2f}%          Время: {data.get('funding_short_time')}<br>"
                                        #f"Общий спред: {data.get('spread_total'):.2f}% / {data.get('spread_usdt'):.2f}$ / {(data.get('volume') * 2)}$<br><br>"
                                        f"Общий спред: {data.get('spread_total'):.2f}% / {(data.get('volume') * 2) / 100 * data.get('spread_total'):.2f}$          Курсовой: {data.get('курсовой'):.2f}% / {(data.get('volume') * 2) / 100 * data.get('курсовой'):.2f}$          Фандинговый: {data.get('funding_spread')}% / {(data.get('volume') * 2) / 100 * data.get('funding_spread'):.2f}$<br><br>"
                                    )

                            try:
                                if id is not None:
                                    await send_message_to_site(f"{msg}<br>Время жизни: {время_жизни}", message_id=id)

                                    
                                else:
                                    id = await send_message_to_site(f"{msg}")



                            except Exception as e:
                                await asyncio.sleep(2.2)
                                print(f"Ошибка в функции арбитража: {e}")
                            await asyncio.sleep(2.2)
                            continue

                        else:

                            try:
                                if id:

                                    await delete_message_from_site(message_id=id)
                                    #print(f'{symbol} {id}')
                                    async with работающие_арбитражи_lock:
                                        работающие_арбитражи.discard(symbol)
                                    await asyncio.sleep(2.2)
                                    return
                                else:
                                    
                                    async with работающие_арбитражи_lock:
                                        работающие_арбитражи.discard(symbol)
                                    await asyncio.sleep(2.2)
                                    return

                            except Exception as e:
                                print(f"Ошибка при обработке потери спреда: {e}")
                            # async with работающие_арбитражи_lock:
                            #     работающие_арбитражи.remove(symbol)
                            # await asyncio.sleep(3)
                            # return
        finally:
            async with работающие_арбитражи_lock:
                работающие_арбитражи.discard(symbol)
            if id:
                await delete_message_from_site(message_id=id)




async def арбитраж_бот(session):
    while True:
        #async with sem:
        data = await apishechka()
        await арбитраж(data, 100, 250, 25, session)
        #print('запустилась функция арбитража')
        await asyncio.sleep(5)




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
    lbank_session = aiohttp.ClientSession()

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
        'lbank': lbank_session,
    }

    print("✅ Сессии созданы")



    asyncio.create_task(арбитраж_бот(sessions))

    return sessions


async def on_shutdown(sessions):
    print("🛑 Закрываем aiohttp сессии...")
    for name, s in sessions.items():
        await s.close()
        print(f"🔒 {name} закрыта")


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