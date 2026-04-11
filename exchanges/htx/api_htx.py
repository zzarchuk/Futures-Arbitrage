import asyncio
from utils.exchange_api.filter_api import filtered_dict



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
                            filtered_dict(
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
                        filtered_dict(
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
                        filtered_dict(data=data, symbol=symbol, exchange='htx', funding=фандинг, get_funding=начисление)
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
