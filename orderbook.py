import aiohttp
import asyncio


async def kucoin(pair, limit, session):
    z = pair.replace("USDT", "USDTM")

    async with session.get(
        f"https://api-futures.kucoin.com/api/v1/level2/depth{limit}?symbol={z}"
    ) as respone:
        order_book = await respone.json()
        asks = order_book["data"]["asks"]
        bids = order_book["data"]["bids"]
        await asyncio.sleep(0.2)
        return {"asks": asks, "bids": bids}


async def binance(pair, limit, session):
    params = {"symbol": pair, "limit": limit}

    async with session.get(
        url="https://fapi.binance.com/fapi/v1/depth", params=params
    ) as respone:
        order_book = await respone.json()
        asks = order_book["asks"]
        bids = order_book["bids"]
        await asyncio.sleep(0.2)
        return {"asks": asks, "bids": bids}


async def bybit(pair, limit, session):
    params = {"category": "linear", "symbol": pair, "limit": limit}
    async with session.get(
        url="https://api.bybit.com/v5/market/orderbook", params=params
    ) as respone:
        order_book = await respone.json()
        asks = order_book["result"]["a"]
        bids = order_book["result"]["b"]
        await asyncio.sleep(0.2)
        return {"asks": asks, "bids": bids}
        # print(order_book['result'])


async def bingx(pair, limit, session):
    params = {"symbol": pair.replace("USDT", "-USDT"), "limit": limit}
    async with session.get(
        url="https://open-api.bingx.com/openApi/swap/v2/quote/depth", params=params
    ) as respone:
        order_book = await respone.json()
        #print(order_book)
        asks = order_book["data"]["asks"]
        bids = order_book["data"]["bids"]
        await asyncio.sleep(0.2)
        return {"asks": asks, "bids": bids}


async def gate(pair, limit, session):
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
        await asyncio.sleep(0.2)
        return {"asks": asks, "bids": bids}


async def mexc(pair, limit, session):
    params = {"limit": limit}
    async with session.get(
        url=f"https://contract.mexc.com/api/v1/contract/depth/{pair.replace('USDT', '_USDT')}",
        params=params,
    ) as respone:
        order_book = await respone.json()
        asks = order_book["data"]["asks"]
        bids = order_book["data"]["bids"]
        await asyncio.sleep(0.2)
        return {"asks": asks, "bids": bids}


async def bitget(pair, limit, session):
    params = {"symbol": pair, "productType": "USDT-FUTURES", "limit": limit}
    async with session.get(
        url="https://api.bitget.com/api/v2/mix/market/merge-depth", params=params
    ) as respone:
        order_book = await respone.json()
        asks = order_book["data"]["asks"]
        bids = order_book["data"]["bids"]
        await asyncio.sleep(0.2)
        return {"asks": asks, "bids": bids}


async def okx(pair, limit, session):
    params = {"instId": pair.replace("USDT", "-USDT-SWAP"), "sz": limit}
    async with session.get(
        url="https://www.okx.com/api/v5/market/books", params=params
    ) as respone:
        order_book = await respone.json()
        #print(order_book)
        asks = order_book["data"][0]["asks"]
        bids = order_book["data"][0]["bids"]
        await asyncio.sleep(0.2)
        return {"asks": asks, "bids": bids}


async def htx(pair, limit, session):
    params = {"contract_code": pair.replace("USDT", "-USDT"), "type": "step0"}
    async with session.get(
        url="https://api.hbdm.com/linear-swap-ex/market/depth", params=params
    ) as respone:
        order_book = await respone.json()
        asks = order_book["tick"]["asks"]
        bids = order_book["tick"]["bids"]
        await asyncio.sleep(0.2)
        return {"asks": asks, "bids": bids}

# Ошибка тут: Не удалось получить order book для PINGPONGUSDT на bingx
# Ошибка тут: Не удалось получить order book для GOATEDUSDT на bingx
# Ошибка тут: Не удалось получить order book для QTOUSDT на bingx
# Ошибка тут: Не удалось получить order book для PINGPONGUSDT на bingx
# Ошибка тут: Не удалось получить order book для GOATEDUSDT на bingx
# Ошибка тут: Не удалось получить order book для QTOUSDT на bingx
# Ошибка тут: Не удалось получить order book для PINGPONGUSDT на bingx
# Ошибка тут: Не удалось получить order book для GOATEDUSDT на bingx
# Ошибка тут: Не удалось получить order book для QTOUSDT на bingx
# Ошибка тут: Не удалось получить order book для PINGPONGUSDT на bingx

#asyncio.run(bingx("FHEUSDT", 20))
#GORKUSDT на bingx
# tao hot
