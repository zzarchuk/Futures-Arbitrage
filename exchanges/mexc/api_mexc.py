import asyncio
from exchanges.binance.api_binance import binance_volume_futures
from utils.exchange_api.filter_api import filtered_dict, get_time_until_funding
import time

cached_mexc_data = {}
last_mexc_update = 0 


async def mexc_futures(data, session):
    for i in range(1, 7):
        try:
            req_ticker = session.get("https://contract.mexc.com/api/v1/contract/ticker")
            req_detail = session.get("https://contract.mexc.com/api/v1/contract/detail")

            resp_ticker, resp_detail = await asyncio.gather(req_ticker, req_detail)


            ticker_json = await resp_ticker.json()
            detail_json = await resp_detail.json()


            for key in ticker_json.get("data", []):
                try:
                    if key.get("symbol", "").endswith("USDT"):
                        symbol = key["symbol"].replace("_", "")
                        цена = key.get("lastPrice")

                        filtered_dict(
                            data=data,
                            exchange="mexc",
                            symbol=symbol,
                            price=цена,
                            futures=True,
                        )
                except Exception as e:
                    print(f"[MEXC futures ticker] Ошибка при обработке пары: {e}")
                    continue


            for key in detail_json.get("data", []):
                try:
                    if key.get("symbol", "").endswith("USDT"):
                        symbol = key["symbol"].replace("_", "")
                        max_vol = key.get("maxVol") * key.get('contractSize')

                        filtered_dict(
                            data=data,
                            exchange="mexc",
                            symbol=symbol,
                            max_vol=max_vol,
                            futures=True,
                        )
                except Exception as e:
                    print(f"[MEXC futures detail] Ошибка при обработке пары: {e}")
                    continue

            return

        except Exception as e:
            print(f"Ошибка в mexc в файле фильтра фьючей: {e}")
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
                        filtered_dict(
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
                        
                        filtered_dict(
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
            await asyncio.sleep(2) 
    return data


async def get_mexc_fundings(subscriptions_config, session):
    global cached_mexc_data, last_mexc_update
    if time.time() - last_mexc_update >= 5 * 60 or not cached_mexc_data:

        subscriptions_config = await binance_volume_futures(session, subscriptions_config)

        cached_mexc_data = await mexc_fundings(subscriptions_config, session)
        last_mexc_update = time.time()
    return cached_mexc_data