import json
import aiohttp
import asyncio
from collections import defaultdict

blacklist = [
    ("MEGAUSDT", "mexc", "all"),
    ("ACNUSDT", "gateio", "spot"),
    ("HOODUSDT", "lbank", "spot"),
    ("METAUSDT", "lbank", "spot"),
    ("XBTUSDT", "lbank", "spot"),
    ("CADUSDT", "gateio", "spot"),
    ("ALLUSDT", "lbank", "spot"),
    ("ALLUSDT", "gateio", "futures"),
    ("1USDT", "lbank", "spot"),
    ("APPUSDT", "bitget", "futures"),
    ("GMEUSDT", "bitget", "futures"),
    ("GTCUSDT", "gateio", "spot"),
    ("LIGHTUSDT", "bingx", "spot"),
    ("LIGHTUSDT", "lbank", "futures"),
    ("LIGHTUSDT", "lbank", "spot"),
    ("DREAMSUSDT", "kucoin", "spot"),
    ("DREAMSUSDT", "mexc", "spot"),
    ("OPULUSDT", "htx", "futures"),
    ("CULTUSDT", "gateio", "spot"),
    ("FLOCKUSDT", "lbank", "spot"),
    ("MAUSDT", "mexc", "spot"),
    ("MAUSDT", "gateio", "spot"),
    ("YZYUSDT", "lbank", "spot"),
    ("BLOCKUSDT", "gateio", "spot"),
    ("BLOCKUSDT", "kucoin", "spot"),
    ("AINUSDT", "lbank", "spot"),
    ("PEPUSDT", "mexc", "spot"),
    ("AURAUSDT", "gateio", "spot"),
    ("AURAUSDT", "mexc", "spot"),
    ("GARIUSDT", "mexc", "spot"),
    ("GARIUSDT", "kucoin", "spot"),
    ("GARIUSDT", "htx", "spot"),
    ("GARIUSDT", "gateio", "spot"),
    ("ARIAIPUSDT", "bitget", "futures"),
    ("CVXUSDT", "htx", "futures"),
    ("ARCUSDT", "mexc", "spot"),
    ("PLUMEUSDT", "htx", "futures"),
    ("BULLAUSDT", "htx", "all"),
    ("MONUSDT", "htx", "spot"),
    ("MONUSDT", "lbank", "spot"),
    ("BROCCOLIUSDT", "kucoin", "spot"),
    ("ACEUSDT", "kucoin", "spot"),
    ("RIFUSDT", "htx", "spot"),
    ("TRUMPUSDT", "bingx", "spot"),
    ("DUSKUSDT", "htx", "futures"),
    ("VICUSDT", "htx", "spot"),
    ("XMRUSDT", "binance", "spot"),
    ("TSTUSDT", "gateio", "spot"),
    ("TSTUSDT", "kucoin", "spot"),
    ("SCUSDT", "lbank", "spot"),
    ("BLDUSDT", "gateio", "spot"),
    ("MEGAUSDT", "lbank", "spot"),
    ("NUMIUSDT", "gateio", "spot"),
    
]


async def apply_blacklist(data: dict, blacklist: list):
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


async def фильтрованный_словарь(data, exchange, symbol, price, futures=None, spot=None):
    if symbol not in data:
        data[symbol] = {}
    if exchange not in data[symbol]:
        data[symbol][exchange] = defaultdict(dict)

    if futures and price:
        data[symbol][exchange]["futures"]["price"] = price

    if spot and price:
        data[symbol][exchange]["spot"]["price"] = price


async def binance_spot(data, session):
    for i in range(1, 7):
        try:
            async with session.get(
                url="https://api.binance.com/api/v3/ticker/price"
            ) as response:
                price = await response.json()
                for key in price:
                    try:
                        if key.get("symbol").endswith("USDT"):
                            symbol = key.get("symbol")
                            цена = float(key.get("price"))
                            await фильтрованный_словарь(
                                data=data,
                                exchange="binance",
                                symbol=symbol,
                                price=цена,
                                spot=True,
                            )
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет спота binance {e}")
                        continue
                return
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
                            await фильтрованный_словарь(
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
                            await фильтрованный_словарь(
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
    for i in range(1, 7):
        try:
            async with session.get(
                url="https://api.mexc.com/api/v3/ticker/price"
            ) as response:
                price = await response.json()
                for key in price:
                    try:
                        if key.get("symbol").endswith("USDT"):
                            symbol = key.get("symbol")
                            цена = float(key.get("price"))
                            await фильтрованный_словарь(
                                data=data,
                                exchange="mexc",
                                symbol=symbol,
                                price=цена,
                                spot=True,
                            )
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет спота mexc {e}")
                        continue
                return
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
                            await фильтрованный_словарь(
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
                            await фильтрованный_словарь(
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
                            await фильтрованный_словарь(
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
                            цена = float(key.get("last"))
                            await фильтрованный_словарь(
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
                            await фильтрованный_словарь(
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
                            await фильтрованный_словарь(
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
    for i in range(1, 7):
        try:
            # async with session.get(url='https://fapi.binance.com/fapi/v1/premiumIndex') as response:
            async with session.get(
                url="https://fapi.binance.com/fapi/v2/ticker/price"
            ) as response:
                price = await response.json()
                for key in price:
                    try:
                        if key.get("symbol").endswith("USDT"):
                            symbol = key.get("symbol")
                            цена = float(key.get("price"))
                            await фильтрованный_словарь(
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
        except Exception as e:
            print(f"Ошибка в binance в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def bybit_futures(data, session):
    for i in range(1, 7):
        try:
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
                            await фильтрованный_словарь(
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
        except Exception as e:
            print(f"Ошибка в bybit в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def bingx_futures(data, session):
    for i in range(1, 7):
        try:
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
                            await фильтрованный_словарь(
                                data=data,
                                exchange="bingx",
                                symbol=symbol,
                                price=цена,
                                futures=True,
                            )
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет фьючей bingx {e}")
                        continue
                return
        except Exception as e:
            print(f"Ошибка в bingx в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def mexc_futures(data, session):
    for i in range(1, 7):
        try:
            async with session.get(
                url="https://contract.mexc.com/api/v1/contract/ticker"
            ) as response:
                price = await response.json()
                for key in price["data"]:
                    try:
                        if key.get("symbol").endswith("USDT"):
                            symbol = key.get("symbol").replace("_", "")
                            цена = key.get("lastPrice")
                            await фильтрованный_словарь(
                                data=data,
                                exchange="mexc",
                                symbol=symbol,
                                price=цена,
                                futures=True,
                            )
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет фьючей mexc {e}")
                        continue
                return
        except Exception as e:
            print(f"Ошибка в mexc в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def bitget_futures(data, session):
    for i in range(1, 7):
        try:
            async with session.get(
                url="https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"
            ) as response:
                price = await response.json()
                for key in price["data"]:
                    try:
                        if key.get("symbol").endswith("USDT"):
                            symbol = key.get("symbol")
                            цена = float(key.get("lastPr"))
                            await фильтрованный_словарь(
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
        except Exception as e:
            print(f"Ошибка в bitget в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def gateio_futures(data, session):
    for i in range(1, 7):
        try:
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
                            await фильтрованный_словарь(
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
        except Exception as e:
            print(f"Ошибка в gateio в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def okx_futures(data, session):
    for i in range(1, 7):
        try:
            # async with session.get(url="https://www.okx.com/api/v5/public/mark-price?instType=SWAP") as response:
            async with session.get(
                url="https://www.okx.com/api/v5/market/tickers?instType=SWAP"
            ) as response:
                price = await response.json()
                for key in price["data"]:
                    try:
                        if key.get("instId").endswith("USDT-SWAP"):
                            symbol = key.get("instId").replace("-USDT-SWAP", "USDT")
                            цена = float(key.get("last"))
                            await фильтрованный_словарь(
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
        except Exception as e:
            print(f"Ошибка в оkx в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def kucoin_futures(data, session):
    for i in range(1, 7):
        try:
            async with session.get(
                url="https://api-futures.kucoin.com/api/v1/allTickers"
            ) as response:
                price = await response.json()
                for key in price["data"]:
                    try:
                        if key.get("symbol").endswith("USDTM"):
                            symbol = key.get("symbol").replace("USDTM", "USDT")
                            цена = float(key.get("price"))
                            await фильтрованный_словарь(
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
        except Exception as e:
            print(f"Ошибка в kucoin в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


async def htx_futures(data, session):
    for i in range(1, 7):
        try:
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
                            await фильтрованный_словарь(
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
                            await фильтрованный_словарь(
                                data=data,
                                exchange="lbank",
                                symbol=symbol,
                                price=цена,
                                futures=True,
                            )
                    except Exception as e:
                        print(f"Ошибка в фильтре перебора монет фьючей lbank {e}")
                        continue
                return
        except Exception as e:
            print(f"Ошибка в lbank в файле фильтра фьючей\n{e}")
            await asyncio.sleep(0.5)


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

    with_blacklist = await apply_blacklist(data, blacklist)

    дата_минимум_2_биржи = {
        symbol: exchanges
        for symbol, exchanges in with_blacklist.items()
        if len(exchanges) >= 2
        and any(
            "futures" in ex_data for ex_data in exchanges.values()
        )  # хотя бы один futures
    }

    with open("filtr.txt", "w", encoding="utf-8") as f:
        json.dump(дата_минимум_2_биржи, f, ensure_ascii=False, indent=4)

    # print(len(дата_минимум_2_биржи.keys()))

    return дата_минимум_2_биржи


# if __name__ == "__main__":
#     asyncio.run(apishechka())
