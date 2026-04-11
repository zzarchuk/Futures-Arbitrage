import asyncio
from utils.exchange_api.filter_api import filtered_dict




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
                            filtered_dict(
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
                        filtered_dict(
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
                        filtered_dict(data=data, symbol=symbol, exchange='okx', funding=фандинг, get_funding=начисление)
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