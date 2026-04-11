import asyncio
from utils.exchange_api.filter_api import filtered_dict



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
                            filtered_dict(
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
                        filtered_dict(
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
                    filtered_dict(data=data, symbol=symbol, exchange='bitget', funding=фандинг, get_funding=начисление)
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