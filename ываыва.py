import asyncio
import aiohttp
import time
import requests

# data = requests.get("https://api.huobi.pro/market/fullMbp?symbol=neiroethusdt").json()

# print(data)

async def safe_fetch_order_book_spot(exchange, symbol, sessions, retries=3, delay=0.5):
    for i in range(retries):
        try:
            #session = sessions.get(exchange)
            if exchange == "bingx":
                symboll = symbol.replace('USDT', '-USDT')
                async with sessions.get(
                    url=f"https://open-api.bingx.com/openApi/spot/v1/market/depth?symbol={symboll}&limit=100"
                ) as response:
                    order_book = await response.json()

                    asks = order_book['data']['asks']
                    bids = order_book['data']['bids']
                    
                    print({'asks': asks, 'bids': bids})
                    return {'asks': asks, 'bids': bids}
            elif exchange == "binance":
                async with sessions.get(
                    url=f"https://api.binance.com/api/v3/depth?symbol={symbol}"
                ) as response:
                    order_book = await response.json()
                    asks = order_book['asks']
                    bids = order_book['bids']
                    print({'asks': asks, 'bids': bids})
                    return {'asks': asks, 'bids': bids}
            elif exchange == "bybit":
                url = "https://api.bybit.com/v5/market/orderbook"
                params = {
                    "category": "spot",  # "spot", "linear", "inverse"
                    "symbol": symbol,
                    'limit': 100
                }
                async with sessions.get(url, params=params) as resp:
                    order_book = await resp.json()
                    asks = order_book['result']['a']
                    bids = order_book['result']['b']
                    print({'asks': asks, 'bids': bids})
                    return {'asks': asks, 'bids': bids}
            elif exchange == "bitget":
                async with sessions.get(
                    url=f"https://api.bitget.com/api/v2/spot/market/orderbook?symbol={symbol}&type=step0&limit=100"
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book['data']['asks']
                    bids = order_book['data']['bids']
                    print({'asks': asks, 'bids': bids})
                    return {'asks': asks, 'bids': bids}
            elif exchange == "gateio":
                async with sessions.get(
                    url=f"https://api.gateio.ws/api/v4/spot/order_book?currency_pair={symbol.replace('USDT', '_USDT')}&limit=100",
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                    },
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book['asks']
                    bids = order_book['bids']
                    print({'asks': asks, 'bids': bids})
                    return {'asks': asks, 'bids': bids}
            elif exchange == "htx":
                async with sessions.get(
                    url=f"https://api.huobi.pro/market/fullMbp?symbol={symbol.lower()}"
                ) as respone:
                    order_book = await respone.json()
                    #print(order_book)
                    asks = order_book['tick']['asks']
                    bids = order_book['tick']['bids']
                    
                    print({'asks': asks, 'bids': bids})
                    return {'asks': asks, 'bids': bids}
            elif exchange == "kucoin":
                async with sessions.get(
                    f"https://api.kucoin.com/api/v1/market/orderbook/level2_{100}?symbol={symbol.replace('USDT', '-USDT')}"
                ) as respone:
                    order_book = await respone.json()
                    
                    asks = order_book['data']['asks']
                    bids = order_book['data']['bids']
                    print({'asks': asks, 'bids': bids})
                    return {'asks': asks, 'bids': bids}                   
            elif exchange == "okx":
                async with sessions.get(
                    url=f"https://www.okx.com/api/v5/market/books?instId={symbol.replace('USDT', '-USDT')}&sz=100"
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book['data'][0]['asks']
                    bids = order_book['data'][0]['bids']
                    
                    print({'asks': asks, 'bids': bids})
                    return {'asks': asks, 'bids': bids}    
            elif exchange == "mexc":
                async with sessions.get(
                    url=f"https://api.mexc.com/api/v3/depth?symbol={symbol}"
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book['asks']
                    bids = order_book['bids']
                    print({'asks': asks, 'bids': bids})
                    return {'asks': asks, 'bids': bids}
            elif exchange == "lbank":
                async with sessions.get(
                    url=f"https://api.lbank.info/v2/depth.do?symbol={symbol.lower().replace('usdt', '_usdt')}&size=100"
                ) as respone:
                    order_book = await respone.json()
                    asks = order_book['data']['asks']
                    bids = order_book['data']['bids']
                    print({'asks': asks, 'bids': bids})
                    return {'asks': asks, 'bids': bids}

            else:
                print("хуйня")
        except Exception as e:
            print(e)
            await asyncio.sleep(delay)
    return None



async def safe_fetch_order_book(
    exchange, pair, sessions, limit=100, retries=3, delay=0.5
):
    #async with светлофор:
        #await asyncio.sleep(2)
    for attempt in range(retries):
        try:
            session = sessions
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
                params = {"limit": limit}
                async with session.get(
                    url=f"https://contract.mexc.com/api/v1/contract/depth/{pair.replace('USDT', '_USDT')}",
                    params=params,
                ) as respone:
                    order_book = await respone.json()
                    print(order_book)
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
                    print(order_book)
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
                    print(order_book)
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
            #print(f"{exchange} Ошибка {pair}, попытка {attempt+1}/{retries}: {e}")
            await asyncio.sleep(delay)
    #raise Exception(f"Не удалось получить order book для {pair} на {exchange}")
    return None

async def main():
    async with aiohttp.ClientSession() as session:
        await safe_fetch_order_book('mexc', 'SWARMSUSDT', session)
        
asyncio.run(main())