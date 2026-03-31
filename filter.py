from datetime import timedelta
import json
import aiohttp
import asyncio
from collections import defaultdict
import time

blacklist = [
    ('SYNUSDT', 'all', 'all'),
    ('FUNUSDT', 'all', 'all'),
    ('CLAWDUSDT', 'htx', 'all'),
    ('GASUSDT', 'mexc', 'all'),
    ('FUNUSDT', 'htx', 'all'),
    ('CVXUSDT', 'htx', 'futures'),
    # ('AIAUSDT', 'all', 'all'),
    # #('PTBUSDT', 'mexc', 'all'),
    # ('LMTSUSDT', 'bingx', 'all'),
    # ('XNLUSDT', 'bingx', 'futures'),
    ('MTLUSDT', 'htx', 'spot'),
    ('BDXUSDT', 'bingx', 'futures'),
    ('REALUSDT', 'gateio', 'spot'),
    ('REALUSDT', 'lbank', 'spot'),
    # ('CARDSUSDT', 'bingx', 'all'),
    # ('XNAPUSDT', 'bingx', 'all'),
    # ('ORACLEUSDT', 'bingx', 'all'),
    # ("MEGAUSDT", "mexc", "all"),
    ("ACNUSDT", "gateio", "spot"),
    # ("HOODUSDT", "lbank", "spot"),
    # ("METAUSDT", "lbank", "spot"),
    # ("XBTUSDT", "lbank", "spot"),
    ("CADUSDT", "gateio", "spot"),
    ("ALLUSDT", "lbank", "spot"),
    ("ALLUSDT", "gateio", "futures"),
    # ("1USDT", "lbank", "spot"),
    ("APPUSDT", "bitget", "futures"),
    ("GMEUSDT", "bitget", "futures"),
    ("GTCUSDT", "gateio", "spot"),
    # ("LIGHTUSDT", "bingx", "spot"),
    # ("LIGHTUSDT", "lbank", "futures"),
    # ("LIGHTUSDT", "lbank", "spot"),
    # ("DREAMSUSDT", "kucoin", "spot"),
    # ("DREAMSUSDT", "mexc", "spot"),
    # ("OPULUSDT", "htx", "futures"),
    # ("CULTUSDT", "gateio", "spot"),
    # ("FLOCKUSDT", "lbank", "spot"),
    ("MAUSDT", "mexc", "spot"),
    ("MAUSDT", "gateio", "spot"),
    # ("YZYUSDT", "lbank", "spot"),
    # ("BLOCKUSDT", "gateio", "spot"),
    # ("BLOCKUSDT", "kucoin", "spot"),
    # ("AINUSDT", "lbank", "spot"),
    ("PEPUSDT", "mexc", "spot"),
    # ("AURAUSDT", "gateio", "spot"),
    # ("AURAUSDT", "mexc", "spot"),
    # ("GARIUSDT", "mexc", "spot"),
    # ("GARIUSDT", "kucoin", "spot"),
    # ("GARIUSDT", "htx", "spot"),
    # ("GARIUSDT", "gateio", "spot"),
    ("ARIAIPUSDT", "bitget", "futures"),
    # ("CVXUSDT", "htx", "futures"),
    ("ARCUSDT", "mexc", "spot"),
    # ("PLUMEUSDT", "htx", "futures"),
    # ("BULLAUSDT", "htx", "all"),
    ("MONUSDT", "htx", "spot"),
    ("MONUSDT", "lbank", "spot"),
    ("BROCCOLIUSDT", "kucoin", "spot"),
    ("ACEUSDT", "kucoin", "spot"),
    ("RIFUSDT", "htx", "spot"),
    ("TRUMPUSDT", "bingx", "spot"),
    # ("DUSKUSDT", "htx", "futures"),
    # ("VICUSDT", "htx", "spot"),
    # ("XMRUSDT", "binance", "spot"),
    ("TSTUSDT", "gateio", "spot"),
    ("TSTUSDT", "kucoin", "spot"),
    # ("SCUSDT", "lbank", "spot"),
    # ("BLDUSDT", "gateio", "spot"),
    # ("MEGAUSDT", "lbank", "spot"),
    ("NUMIUSDT", "gateio", "spot"),
    
]
cached_mexc_data = {}
last_mexc_update = 0 


def get_time_until_funding(funding_timestamp: int, exchange_name) -> str:
    # Переводим миллисекунды → секунды и корректируем часовой пояс
    now = time.time()
    if exchange_name in ('bingx', 'okx'):
        funding_timestamp = funding_timestamp / 1000  # type: ignore # -2 часа
    # if exchange_name in ('bybit'):
    #     funding_timestamp = funding_timestamp / 1000 + 7200
    elif exchange_name == 'gateio':
        # Gate.io уже отдаёт timestamp в секундах, ничего не делаем
        pass
    else:
        funding_timestamp = funding_timestamp / 1000 # type: ignore

    seconds_left = funding_timestamp - now

    if seconds_left <= 0:
        return "Фандинг уже произошёл."

    td = timedelta(seconds=seconds_left)
    hours, remainder = divmod(int(td.total_seconds()), 3600)
    minutes, _ = divmod(remainder, 60)

    days = hours // 24
    hours = hours % 24

    if days > 0:
        return f"{days} дн. {hours} ч. {minutes} мин."
    return f"{hours} ч. {minutes} мин."


def apply_blacklist(data: dict, blacklist: list):
    """
    data — твой словарь монет/бирж
    blacklist — список кортежей:
        [
            ("PYTHUSDT", "mexc", "spot"),
            ("BTCUSDT", "binance", "futures"),
            ("ETHUSDT", "okx", "all"),  # удалить биржу полностью
        ]
    """

    for coin, exchange, market_type in blacklist:
        if exchange == 'all' and market_type == 'all' and coin in data:
            del data[coin]

        # монеты может не быть
        if coin not in data:
            continue

        # биржи может не быть
        if exchange not in data[coin]:
            continue

        # удалить все рынки — биржу полностью
        if market_type == "all":
            del data[coin][exchange]
            continue

        # удалить только spot или futures
        if market_type in data[coin][exchange]:
            del data[coin][exchange][market_type]

        # если после удаления рынков биржа пустая — удалить биржу
        if not data[coin][exchange]:
            del data[coin][exchange]

    return data


def фильтрованный_словарь(data, exchange, symbol, price=None, futures=None, spot=None, max_vol=None, funding=None, get_funding=None):
    if symbol not in data:
        data[symbol] = {}
    if exchange not in data[symbol]:
        data[symbol][exchange] = defaultdict(dict)

    if futures and price:
        data[symbol][exchange]["futures"]["price"] = price if price is not None else 0
        
        
    if funding is not None and get_funding is not None:
        data[symbol][exchange]['futures']["funding"] = funding * 100  # % 
        data[symbol][exchange]['futures']["get_funding"] = get_time_until_funding(get_funding, exchange)
        
    if spot and price:
        data[symbol][exchange]["spot"]["price"] = price
    
    if futures and max_vol:
        data[symbol][exchange]["futures"]["max_vol"] = max_vol

    if spot and max_vol:
        data[symbol][exchange]["spot"]["max_vol"] = max_vol


async def binance_spot(data, session):
    async def price():
        async with session.get(
            url="https://api.binance.com/api/v3/ticker/price"
        ) as response:
            price = await response.json()
            for key in price:
                try:
                    if key.get("symbol").endswith("USDT"):
                        symbol = key.get("symbol")
                        цена = float(key.get("price"))
                        фильтрованный_словарь(
                            data=data,
                            exchange="binance",
                            symbol=symbol,
                            price=цена,
                            spot=True,
                        )
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет спота price binance {e}")
                    continue
        return
    
    async def volume():
        async with session.get(
            url="https://api.binance.com/api/v3/exchangeInfo"
        ) as response:
            price = await response.json()
            for key in price['symbols']:
                try:
                    if key.get("symbol", "").endswith("USDT") and key.get("isSpotTradingAllowed") is True:
                        symbol = key["symbol"]

                        max_notional = None

                        # Перебираем фильтры
                        for f in key.get("filters", []):
                            if f.get("filterType") == "NOTIONAL":
                                max_notional = float(f.get("maxNotional", 0))
                                break

                        if max_notional is None:
                            max_notional = 0  # или continue

                        фильтрованный_словарь(
                            data=data,
                            exchange="binance",
                            symbol=symbol,
                            max_vol=max_notional,
                            spot=True
                        )
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет спота volume binance {e}")
                    continue
        return
    for i in range(1, 7):
        try:
            await asyncio.gather(price(), volume())
            break
        except Exception as e:
            print(f"Ошибка в binance в файле фильтра спота\n{e}")
            await asyncio.sleep(0.5)


async def bybit_spot(data, session):
    for i in range(1, 7):
        try:
            async with session.get(
                url="https://api.bybit.com/v5/market/tickers",
                params={"category": "spot"},
            ) as response:
                price = await response.json()
                
                for key in price["result"]["list"]:
                    try:
                        if key.get("symbol").endswith("USDT"):
                            symbol = key.get("symbol")
                            цена = float(key.get("lastPrice"))
                            фильтрованный_словарь(
                                data=data,
                                exchange="bybit",
                                symbol=symbol,
                                price=цена,
                                spot=True,
                            )
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет спота bybit {e}")
                        continue
                return
        except Exception as e:
            print(f"Ошибка в bybit в файле фильтра спота\n{e}")
            await asyncio.sleep(0.5)


async def bingx_spot(data, session):
    for i in range(1, 7):
        try:
            async with session.get(
                url="https://open-api.bingx.com/openApi/spot/v1/ticker/price"
            ) as response:
                price = await response.json()
                for key in price["data"]:
                    try:
                        if key.get("symbol").endswith("USDT"):
                            symbol = key.get("symbol").replace("_", "")
                            цена = float(key["trades"][0].get("price"))
                            фильтрованный_словарь(
                                data=data,
                                exchange="bingx",
                                symbol=symbol,
                                price=цена,
                                spot=True,
                            )
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет спота bingx {e}")
                        continue
                return
        except Exception as e:
            print(f"Ошибка в bingx в файле фильтра спота\n{e}")
            await asyncio.sleep(0.5)


async def mexc_spot(data, session):
    
    async def price():
        async with session.get(
            url="https://api.mexc.com/api/v3/ticker/price"
        ) as response:
            price = await response.json()
            for key in price:
                try:
                    if key.get("symbol").endswith("USDT"):
                        symbol = key.get("symbol")
                        цена = float(key.get("price"))
                        фильтрованный_словарь(
                            data=data,
                            exchange="mexc",
                            symbol=symbol,
                            price=цена,
                            spot=True,
                        )
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет спота price mexc {e}")
                    continue
        return
    
    async def max_volume():
        async with session.get(
            url="https://api.mexc.com/api/v3/exchangeInfo"
        ) as response:
            price = await response.json()
            for key in price['symbols']:
                try:
                    if key.get("symbol").endswith("USDT") and key.get('status') == '1':
                        symbol = key.get("symbol")
                        vol = float(key.get("maxQuoteAmountMarket"))
                        #print(vol)
                        фильтрованный_словарь(
                            data=data,
                            exchange="mexc",
                            symbol=symbol,
                            max_vol=vol,
                            spot=True,
                        )
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет спота volume mexc {e}")
                    continue
        return
    
    for i in range(1, 7):
        try:
            await asyncio.gather(price(), max_volume())
            break
        except Exception as e:
            print(f"Ошибка в mexc в файле фильтра спота\n{e}")
            await asyncio.sleep(0.5)


async def bitget_spot(data, session):
    for i in range(1, 7):
        try:
            async with session.get(
                url="https://api.bitget.com/api/v2/spot/market/tickers"
            ) as response:
                price = await response.json()
                for key in price["data"]:
                    try:
                        if key.get("symbol").endswith("USDT"):
                            symbol = key.get("symbol")
                            цена = float(key.get("lastPr"))
                            фильтрованный_словарь(
                                data=data,
                                exchange="bitget",
                                symbol=symbol,
                                price=цена,
                                spot=True,
                            )
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет спота bitget {e}")
                        continue
                return
        except Exception as e:
            print(f"Ошибка в bitget в файле фильтра спота\n{e}")
            await asyncio.sleep(0.5)


async def gateio_spot(data, session):
    for i in range(1, 7):
        try:
            async with session.get(
                url="https://api.gateio.ws/api/v4/spot/tickers",
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
            ) as response:
                price = await response.json()
                for key in price:
                    try:
                        if key.get("currency_pair").endswith("USDT"):
                            symbol = key.get("currency_pair").replace("_", "")
                            цена = float(key.get("last"))
                            фильтрованный_словарь(
                                data=data,
                                exchange="gateio",
                                symbol=symbol,
                                price=цена,
                                spot=True,
                            )
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет спота gateio {e}")
                        continue
                return
        except Exception as e:
            print(f"Ошибка в gateio в файле фильтра спота\n{e}")
            await asyncio.sleep(0.5)


async def okx_spot(data, session):
    for i in range(1, 7):
        try:
            # async with session.get(url="https://www.okx.com/api/v5/public/mark-price?instType=SWAP") as response:
            async with session.get(
                url="https://www.okx.com/api/v5/market/tickers?instType=SPOT"
            ) as response:
                price = await response.json()
                for key in price["data"]:
                    try:
                        if key.get("instId").endswith("USDT"):
                            symbol = key.get("instId").replace("-USDT", "USDT")
                            цена = float(key.get("last"))
                            фильтрованный_словарь(
                                data=data,
                                exchange="okx",
                                symbol=symbol,
                                price=цена,
                                spot=True,
                            )
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет спота okx {e}")
                        continue
                return
        except Exception as e:
            print(f"Ошибка в оkx в файле фильтра спота\n{e}")
            await asyncio.sleep(0.5)


async def kucoin_spot(data, session):
    for i in range(1, 7):
        try:
            async with session.get(
                url="https://api.kucoin.com/api/v1/market/allTickers"
            ) as response:
                price = await response.json()
                for key in price["data"]["ticker"]:
                    try:
                        if key.get("symbol").endswith("USDT"):
                            symbol = key.get("symbol").replace("-USDT", "USDT")
                            last_price = key.get("last")
                            if last_price in (None, "", "0"):
                                continue

                            цена = float(last_price)
                            фильтрованный_словарь(
                                data=data,
                                exchange="kucoin",
                                symbol=symbol,
                                price=цена,
                                spot=True,
                            )
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет спота kucoin {e}")
                        continue
                return
        except Exception as e:
            print(f"Ошибка в kucoin в файле фильтра спота\n{e}")
            await asyncio.sleep(0.5)


async def htx_spot(data, session):
    for i in range(1, 7):
        try:
            # async with session.get(url="https://api.hbdm.com/linear-swap-ex/market/detail/batch_merged?contract_type=swap") as response:
            async with session.get(
                url="https://api.huobi.pro/market/tickers"
            ) as response:
                price = await response.json()
                for key in price["data"]:
                    try:
                        if key.get("symbol").endswith("usdt"):
                            symbol = key.get("symbol").upper()
                            цена = key.get("close")
                            фильтрованный_словарь(
                                data=data,
                                exchange="htx",
                                symbol=symbol,
                                price=цена,
                                spot=True,
                            )
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет спота htx {e}")
                        continue
                return
        except Exception as e:
            print(f"Ошибка в htx в файле фильтра спота\n{e}")
            await asyncio.sleep(0.5)


async def lbank_spot(data, session):
    for i in range(1, 7):
        try:
            async with session.get(
                url="https://api.lbank.info/v2/supplement/ticker/price.do"
            ) as response:
                price = await response.json()
                for key in price["data"]:
                    try:
                        if key.get("symbol").endswith("_usdt"):
                            symbol = key.get("symbol").upper().replace("_USDT", "USDT")
                            цена = float(key.get("price"))
                            фильтрованный_словарь(
                                data=data,
                                exchange="lbank",
                                symbol=symbol,
                                price=цена,
                                spot=True,
                            )
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет спота lbank {e}")
                        continue
                return
        except Exception as e:
            print(f"Ошибка в lbank в файле фильтра спота\n{e}")
            await asyncio.sleep(0.5)


async def binance_futures(data, session):
    async def binance_funding():
        async with session.get(
            url="https://fapi.binance.com/fapi/v1/premiumIndex"
        ) as response:
            price = await response.json()
            if price is not None:
                for k in price:
                    try:
                        if k.get('symbol').endswith('USDT'):
                            symbol = k.get('symbol')
                            фандинг = float(k.get('lastFundingRate'))
                            начисление = k.get('nextFundingTime')
                            фильтрованный_словарь(data=data, symbol=symbol, exchange='binance', funding=фандинг, get_funding=начисление)
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет funding фьючей binance {e}")
                        continue
        return
        
    async def binance_price():
        async with session.get(
            url="https://fapi.binance.com/fapi/v2/ticker/price"
        ) as response:
            price = await response.json()
            for key in price:
                try:
                    if key.get("symbol").endswith("USDT"):
                        symbol = key.get("symbol")
                        цена = float(key.get("price"))
                        фильтрованный_словарь(
                            data=data,
                            exchange="binance",
                            symbol=symbol,
                            price=цена,
                            futures=True,
                        )
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет фьючей binance {e}")
                    continue
        return

        return
    for i in range(1, 7):
        try:
            await asyncio.gather(binance_price(), binance_funding())
            break
        except Exception as e:
            print(f"Ошибка в binance в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def bybit_futures(data, session):
    async def bybit_fundings():
        async with session.get(
            url="https://api.bybit.com/v5/market/tickers",
            params={"category": "linear"},
        ) as response:
            price = await response.json()
            if price is not None:
                for k in price['result']['list']:
                    try:
                        if k.get('deliveryTime') == '0' and k.get('predictedDeliveryPrice') == '' and k.get('symbol').endswith('USDT'):
                            symbol = k.get('symbol')
                            фандинг = float(k.get('fundingRate'))
                            начисление = float(k.get('nextFundingTime'))

                            фильтрованный_словарь(data=data, symbol=symbol, exchange='bybit', funding=фандинг, get_funding=начисление)
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет funding фьючей bybit {e}")
                        continue                        
        return
    
    async def price():
        async with session.get(
            url="https://api.bybit.com/v5/market/tickers",
            params={"category": "linear"},
        ) as response:
            price = await response.json()
            for key in price["result"]["list"]:
                try:
                    if key.get("symbol").endswith("USDT"):
                        symbol = key.get("symbol")
                        цена = float(key.get("lastPrice"))
                        фильтрованный_словарь(
                            data=data,
                            exchange="bybit",
                            symbol=symbol,
                            price=цена,
                            futures=True,
                        )
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет фьючей bybit {e}")
                    continue
        return

    for i in range(1, 7):
        try:
            await asyncio.gather(bybit_fundings(), price())
            break
        except Exception as e:
            print(f"Ошибка в bybit в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def bingx_futures(data, session):
    async def price():

        # async with session.get(url='https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex') as response:
        async with session.get(
            url="https://open-api.bingx.com/openApi/swap/v1/ticker/price"
        ) as response:
            price = await response.json()
            for key in price["data"]:
                try:
                    if key.get("symbol").endswith("USDT"):
                        symbol = key.get("symbol").replace("-", "")
                        цена = float(key.get("price"))
                        фильтрованный_словарь(
                            data=data,
                            exchange="bingx",
                            symbol=symbol,
                            price=цена,
                            futures=True,
                        )
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет фьючей price bingx {e}")
                    continue
        return
    async def funding():
        async with session.get(
            url="https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex"
        ) as response:
            price = await response.json()
            for k in price['data']:
                try:
                    symbol = k.get('symbol').replace('-', '')
                    фандинг = float(k.get('lastFundingRate'))
                    начисление = k.get('nextFundingTime')
                    фильтрованный_словарь(data=data, symbol=symbol, exchange='bingx', funding=фандинг, get_funding=начисление)
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет фьючей funding bingx {e}")
        return
    for i in range(1, 7):
        try:
            await asyncio.gather(price(), funding())
            break
        except Exception as e:
            print(f"Ошибка в bingx в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def mexc_futures(data, session):
    for i in range(1, 7):
        try:
            # Делаем ОДНОВРЕМЕННО два запроса
            req_ticker = session.get("https://contract.mexc.com/api/v1/contract/ticker")
            req_detail = session.get("https://contract.mexc.com/api/v1/contract/detail")

            resp_ticker, resp_detail = await asyncio.gather(req_ticker, req_detail)

            # Парсим JSON
            ticker_json = await resp_ticker.json()
            detail_json = await resp_detail.json()

            # ---------------------------
            # ОБРАБОТКА TICKER
            # ---------------------------
            for key in ticker_json.get("data", []):
                try:
                    if key.get("symbol", "").endswith("USDT"):
                        symbol = key["symbol"].replace("_", "")
                        цена = key.get("lastPrice")

                        фильтрованный_словарь(
                            data=data,
                            exchange="mexc",
                            symbol=symbol,
                            price=цена,
                            futures=True,
                        )
                except Exception as e:
                    print(f"[MEXC futures ticker] Ошибка при обработке пары: {e}")
                    continue

            # ---------------------------
            # ОБРАБОТКА DETAIL
            # ---------------------------
            for key in detail_json.get("data", []):
                try:
                    if key.get("symbol", "").endswith("USDT"):
                        symbol = key["symbol"].replace("_", "")
                        max_vol = key.get("maxVol") * key.get('contractSize')

                        фильтрованный_словарь(
                            data=data,
                            exchange="mexc",
                            symbol=symbol,
                            max_vol=max_vol,
                            futures=True,
                        )
                except Exception as e:
                    print(f"[MEXC futures detail] Ошибка при обработке пары: {e}")
                    continue

            return  # ← Оставлено, если действительно нужно выйти после первого успешного запроса

        except Exception as e:
            print(f"Ошибка в mexc в файле фильтра фьючей: {e}")
            await asyncio.sleep(0.5)


async def bitget_futures(data, session):
    async def price():
        async with session.get(
            url="https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"
        ) as response:
            price = await response.json()
            for key in price["data"]:
                try:
                    if key.get("symbol").endswith("USDT"):
                        symbol = key.get("symbol")
                        цена = float(key.get("lastPr"))
                        фильтрованный_словарь(
                            data=data,
                            exchange="bitget",
                            symbol=symbol,
                            price=цена,
                            futures=True,
                        )
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет фьючей bitget {e}")
                    continue

        return
    async def fundings():
        async with session.get(
            url="https://api.bitget.com/api/v2/mix/market/current-fund-rate?productType=usdt-futures"
        ) as response:
            price = await response.json()
            for k in price['data']:
                try:
                    symbol = k.get('symbol')
                    фандинг = float(k.get('fundingRate'))
                    начисление = float(k.get('nextUpdate'))
                    фильтрованный_словарь(data=data, symbol=symbol, exchange='bitget', funding=фандинг, get_funding=начисление)
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет фьючей fundings bitget {e}")
                    continue
        return
    
    for i in range(1, 7):
        try:
            await asyncio.gather(price(), fundings())
            break
        except Exception as e:
            print(f"Ошибка в bitget в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def gateio_futures(data, session):
    async def price():
        async with session.get(
            url="https://api.gateio.ws/api/v4/futures/usdt/contracts",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        ) as response:
            price = await response.json()
            for key in price:
                try:
                    if key.get("name").endswith("USDT"):
                        symbol = key.get("name").replace("_", "")
                        цена = float(key.get("last_price"))
                        фильтрованный_словарь(
                            data=data,
                            exchange="gateio",
                            symbol=symbol,
                            price=цена,
                            futures=True,
                        )
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет фьючей gateio {e}")
                    continue
        return
    
    async def fundings():
        async with session.get(
            url="https://api.gateio.ws/api/v4/futures/usdt/contracts", headers={'Accept': 'application/json', 'Content-Type': 'application/json'},
        ) as response:
            price = await response.json()
            for k in price:
                try:
                    if k.get('type') == 'direct':
                        symbol = k.get('name').replace('_', '')
                        фандинг = float(k.get('funding_rate'))
                        начисление = float(k.get('funding_next_apply'))
                        # maker = float(k.get('maker_fee_rate'))
                        # taker = float(k.get('taker_fee_rate'))
                        # индекс = float(k.get('index_price'))
                        фильтрованный_словарь(data=data, symbol=symbol, exchange='gateio', funding=фандинг, get_funding=начисление)
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет фьючей fund gateio {e}")
                    continue
        return
    for i in range(1, 7):
        try:
            await asyncio.gather(price(), fundings())
            break
        except Exception as e:
            print(f"Ошибка в gateio в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def okx_futures(data, session):
    async def price():
        # async with session.get(url="https://www.okx.com/api/v5/public/mark-price?instType=SWAP") as response:
        async with session.get(
            url="https://www.okx.com/api/v5/market/tickers?instType=SWAP"
        ) as response:
            price = await response.json()
            for key in price["data"]:
                try:
                    if key.get("instId").endswith("USDT-SWAP"):
                        symbol = key.get("instId").replace("-USDT-SWAP", "USDT")
                        last_price = key.get("last")
                        if last_price in (None, "", "0"):
                            continue

                        цена = float(last_price)
                        фильтрованный_словарь(
                            data=data,
                            exchange="okx",
                            symbol=symbol,
                            price=цена,
                            futures=True,
                        )
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет фьючей okx {e}")
                    continue
        return
    async def fundings():
        # async with session.get(url="https://www.okx.com/api/v5/public/mark-price?instType=SWAP") as response:
        async with session.get(
            url="https://www.okx.com/api/v5/public/funding-rate?instId=ANY"
        ) as response:
            price = await response.json()
            for k in price["data"]:
                try:
                    if k.get('formulaType') == 'withRate' and k.get('instType') == 'SWAP' and k.get('instId').replace('-SWAP', '').endswith('USDT'):
                        symbol = k.get('instId').replace('-', '').replace('SWAP', '')
                        фандинг = float(k.get('fundingRate'))
                        начисление = float(k.get('fundingTime'))
                        фильтрованный_словарь(data=data, symbol=symbol, exchange='okx', funding=фандинг, get_funding=начисление)
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет фьючей fund okx {e}")
                    continue
        return
    for i in range(1, 7):
        try:
            await asyncio.gather(price(), fundings())
            break
        except Exception as e:
            print(f"Ошибка в оkx в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def kucoin_futures(data, session):
    async def price():
        async with session.get(
            url="https://api-futures.kucoin.com/api/v1/allTickers"
        ) as response:
            price = await response.json()
            for key in price["data"]:
                try:
                    if key.get("symbol").endswith("USDTM"):
                        symbol = key.get("symbol").replace("USDTM", "USDT")
                        цена = float(key.get("price"))
                        фильтрованный_словарь(
                            data=data,
                            exchange="kucoin",
                            symbol=symbol,
                            price=цена,
                            futures=True,
                        )
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет фьючей kucoin {e}")
                    continue
        return
    async def fundings():
        async with session.get(
            url="https://api-futures.kucoin.com/api/v1/contracts/active"
        ) as response:
            price = await response.json()
            for k in price["data"]:
                try:
                    if k.get('quoteCurrency') == 'USDT' and k.get('expireDate') == None and k.get('symbol').endswith('M') and k.get('type') == 'FFWCSX' and k.get('status') == 'Open':
                        symbol = k.get('symbol').rstrip('M')

                        фандинг = float(k.get('fundingFeeRate'))
                        начисление = float(k.get('nextFundingRateDateTime'))
                    фильтрованный_словарь(data=data, symbol=symbol, exchange='kucoin', funding=фандинг, get_funding=начисление)
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет фьючей fund kucoin {e}")
                    continue
        return
    for i in range(1, 7):
        try:
            await asyncio.gather(price(), fundings())
            break
        except Exception as e:
            print(f"Ошибка в kucoin в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def htx_futures(data, session):
    async def price():
        # async with session.get(url="https://api.hbdm.com/linear-swap-ex/market/detail/batch_merged?contract_type=swap") as response:
        async with session.get(
            url="https://api.hbdm.com/linear-swap-ex/market/trade"
        ) as response:
            price = await response.json()
            for key in price["tick"]["data"]:
                try:
                    if key.get("contract_code").endswith("-USDT"):
                        symbol = key.get("contract_code").replace("-USDT", "USDT")
                        цена = float(key.get("price"))
                        фильтрованный_словарь(
                            data=data,
                            exchange="htx",
                            symbol=symbol,
                            price=цена,
                            futures=True,
                        )
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет фьючей htx {e}")
                    continue
        return
    async def fundings():
        # async with session.get(url="https://api.hbdm.com/linear-swap-ex/market/detail/batch_merged?contract_type=swap") as response:
        async with session.get(url="https://api.hbdm.com/linear-swap-api/v1/swap_batch_funding_rate?contract_code=", headers={'Accept': 'application/json', 'Content-Type': 'application/json'}) as response:
            price = await response.json()
            for k in price['data']:
                try:
                    if k.get('contract_code').endswith('USDT') and k.get('funding_rate') != None and k.get('funding_time') != None:
                        symbol = k.get('contract_code').replace('-', '')
                        фандинг = float(k.get('funding_rate'))
                        начисление = float(k.get('funding_time'))
                        фильтрованный_словарь(data=data, symbol=symbol, exchange='htx', funding=фандинг, get_funding=начисление)
                except Exception as e:
                    print(f"Ошибка в фильтре перебора монет фьючей fund htx {e}")
                    continue
        return  
    for i in range(1, 7):
        try:
            await asyncio.gather(price(), fundings())
            break
        except Exception as e:
            print(f"Ошибка в htx в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def lbank_futures(data, session):
    
    for i in range(1, 7):
        try:
            async with session.get(
                url="https://lbkperp.lbank.com/cfd/openApi/v1/pub/marketData?productGroup=SwapU"
            ) as response:
                price = await response.json()
                for key in price["data"]:
                    try:
                        if (
                            key.get("symbol").endswith("USDT")
                            and key.get("lastPrice")
                            and key.get("instrumentStatus") != "1"
                            and key.get("turnover") != "0"
                            and key.get("volume") != "0"
                        ):
                            symbol = key.get("symbol")
                            цена = float(key.get("lastPrice"))
                            funding = float(key.get('fundingRate', 0))
                            next = key.get('nextFeeTime')
                            фильтрованный_словарь(
                                data=data,
                                exchange="lbank",
                                symbol=symbol,
                                price=цена,
                                funding=funding,
                                get_funding=next,
                                futures=True,
                            )
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет фьючей lbank {e}\n{key}")
                        continue
                return
        except Exception as e:
            print(f"Ошибка в lbank в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)

###UTILS
async def binance_volume_futures(session, data):
    symbols = list(set(symbol for symbol, exchange in data.items() if 'binance' in exchange and 'futures' in exchange['binance']))
    async with session.get(
        url="https://fapi.binance.com/fapi/v1/exchangeInfo"
    ) as response:
        volume = await response.json()
        for k in volume['symbols']:
            try:
                if k.get('symbol').endswith('USDT') and k.get('symbol') in symbols and k.get('contractType') == 'PERPETUAL' and k.get('status') == 'TRADING':
                    symbol = k.get('symbol')
                    
                    max_market_qty = float(next((f['maxQty'] for f in k.get('filters') if f['filterType'] == 'MARKET_LOT_SIZE'), 0))
                    price = data[symbol]['binance']['futures'].get('price', 0)
                    if price == 0:
                        continue
                    data[symbol]['binance']['futures']['max_vol'] = max_market_qty * price
            except Exception as e:
                print(f"Ошибка в фильтре перебора монет volume фьючей binance {e}")
                continue
    return data
async def mexc_fundings(data, session):  # максимум 20 одновременных запросов

    async def fetch_one(symbol: str):
        try:
            url = f"https://contract.mexc.com/api/v1/contract/funding_rate/{symbol}"
            async with session.get(url) as response:
                fundings = await response.json()
                #print(fundings)
                if not fundings or 'data' not in fundings:
                    return

                funding = fundings['data'].get('fundingRate')
                get = fundings['data'].get('nextSettleTime')

                key = symbol.replace('_USDT', 'USDT')
                if key in data and 'mexc' in data[key] and 'futures' in data[key]['mexc']:
                    if funding is not None:
                        data[key]['mexc']['futures']['funding'] = funding * 100
                    if get:
                        data[key]['mexc']['futures']['get_funding'] = get_time_until_funding(get, 'mexc')

        except Exception as e:
            print(f'Ошибка MEXC {symbol}: {e}')

    symbols = [
        symbol.replace('USDT', '_USDT')
        for symbol, exchanges in data.items()
        if 'mexc' in exchanges and 'futures' in exchanges['mexc']
    ]
    for i in range(0, len(symbols), 20):
        batch = symbols[i:i + 20]
        tasks = [fetch_one(symbol) for symbol in batch]
        await asyncio.gather(*tasks)
        if i + 20 < len(symbols):
            await asyncio.sleep(2)  # пауза между батчами

    return data
async def get_mexc_fundings(subscriptions_config, session):
    global cached_mexc_data, last_mexc_update
    if time.time() - last_mexc_update >= 5 * 60 or not cached_mexc_data:
        #print('yfxfkj')
        subscriptions_config = await binance_volume_futures(session, subscriptions_config)
        #print(1212)
        cached_mexc_data = await mexc_fundings(subscriptions_config, session)
        last_mexc_update = time.time()
    return cached_mexc_data
    # Разбиваем на батчи по 20
###UTILS

async def apishechka():
    data = {}

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            binance_spot(data, session),
            bybit_spot(data, session),
            bingx_spot(data, session),
            mexc_spot(data, session),
            bitget_spot(data, session),
            gateio_spot(data, session),
            okx_spot(data, session),
            kucoin_spot(data, session),
            htx_spot(data, session),
            lbank_spot(data, session),
            binance_futures(data, session),
            bybit_futures(data, session),
            bingx_futures(data, session),
            mexc_futures(data, session),
            bitget_futures(data, session),
            gateio_futures(data, session),
            okx_futures(data, session),
            kucoin_futures(data, session),
            htx_futures(data, session),
            lbank_futures(data, session),
        )

    with_blacklist = apply_blacklist(data, blacklist)

    дата_минимум_2_биржи = {
        symbol: exchanges
        for symbol, exchanges in with_blacklist.items()
        if len(exchanges) >= 2
        and any(
            "futures" in ex_data for ex_data in exchanges.values()
        )  # хотя бы один futures
    }

    # with open("filtr.txt", "w", encoding="utf-8") as f:
    #     json.dump(дата_минимум_2_биржи, f, ensure_ascii=False, indent=4)

    # for k, v in дата_минимум_2_биржи.items():
    #     for kk, vv in v.items():
    #         if kk == 'lbank':
    #             print(f'{k}\n{vv}\n\n')

    return дата_минимум_2_биржи

async def арбитраж(пары, session):
    #print("Старт арбитража")

    """
    Генерирует словарь подписок по найденным арбитражным символам.
    """
    subscriptions_config = {}

    for symbol, exchanges in пары.items():
        # ---- 1. Находим минимальную цену ----
        min_exchange, min_market_type, min_price, funding, timer, all_data = min(
            (
                (exchange, market_type, all_data.get('price', 0), all_data.get('funding', 0), all_data.get('get_funding', 'spot'), all_data)
                for exchange, markets in exchanges.items()
                for market_type, all_data in markets.items()
            ),
            key=lambda x: x[2]
        )

        if min_price == 0:
            continue

        # ---- 2. Проверяем арбитраж ----
        for exchange, types in exchanges.items():
            for market_type, data in types.items():

                # Пропускаем минимальную цену и неарбитражные комбинации
                if (min_exchange == exchange or 
                    (min_market_type, market_type) in [('spot', 'spot'), ('futures', 'spot')]):
                    continue

                spread = ((data.get('price') - min_price) / min_price * 100)
                spred_funding = data.get('funding', 0) - funding

                # if (spread >= 4 or spred_funding + spread >= 1) and (exchange == 'mexc' or (data.get('funding') and data.get('get_funding'))):
                #     # Добавляем в словарь подписок
                #     if symbol not in subscriptions_config:
                #         subscriptions_config[symbol] = {}
                #     if exchange not in subscriptions_config[symbol]:
                #         subscriptions_config[symbol][exchange] = []

                #     if market_type not in subscriptions_config[symbol][exchange]:
                #         subscriptions_config[symbol][exchange].append(market_type)

                #     # Также добавляем биржу с минимальной ценой
                #     if min_exchange not in subscriptions_config[symbol]:
                #         subscriptions_config[symbol][min_exchange] = []
                #     if min_market_type not in subscriptions_config[symbol][min_exchange]:
                #         subscriptions_config[symbol][min_exchange].append(min_market_type)
                if (spread >= 2 or spred_funding + spread >= 2) and (exchange == 'mexc' or (data.get('funding') and data.get('get_funding'))):
                    # Добавляем в словарь подписок
                    if symbol not in subscriptions_config:
                        subscriptions_config[symbol] = {}
                    if exchange not in subscriptions_config[symbol]:
                        subscriptions_config[symbol][exchange] = {}

                    if market_type not in subscriptions_config[symbol][exchange]:
                        subscriptions_config[symbol][exchange][market_type] = data

                    # Также добавляем биржу с минимальной ценой
                    if min_exchange not in subscriptions_config[symbol]:
                        subscriptions_config[symbol][min_exchange] = {}
                    if min_market_type not in subscriptions_config[symbol][min_exchange]:
                        subscriptions_config[symbol][min_exchange][min_market_type] = all_data
                        
                        
    data_with_mexc = await get_mexc_fundings(subscriptions_config, session)


    # for k, v in data_with_mexc.items():
    #     for kk, vv in v.items():
    #         if kk == 'binance' and 'futures' in vv:
    #             print(f'{k}\n{vv}\n\n')

    # with open("filtr.txt", "w", encoding="utf-8") as f:
    #     json.dump(data_with_mexc, f, ensure_ascii=False, indent=4)

    return data_with_mexc

async def mainn():
    async with aiohttp.ClientSession() as session:
            data = await apishechka()
            zz = await арбитраж(data, session)
    return zz
            

if __name__ == "__main__":
    asyncio.run(mainn())
