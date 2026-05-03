import asyncio
from utils.exchange_api.filter_api import filtered_dict
import logging

logger = logging.getLogger(__name__)

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

                            filtered_dict(data=data, symbol=symbol, exchange='bybit', funding=фандинг, get_funding=начисление)
                    except Exception as e:
                        logger.error(f"Bybit API futures: {e}", exc_info=True)
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
                        filtered_dict(
                            data=data,
                            exchange="bybit",
                            symbol=symbol,
                            price=цена,
                            futures=True,
                        )
                except Exception as e:
                    logger.error(f"Bybit API futures: {e}", exc_info=True)
                    continue
        return

    for i in range(1, 7):
        try:
            await asyncio.gather(bybit_fundings(), price())
            break
        except Exception as e:
            logger.error(f"Bybit API futures: {e}", exc_info=True)
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
                            filtered_dict(
                                data=data,
                                exchange="bybit",
                                symbol=symbol,
                                price=цена,
                                spot=True,
                            )
                    except Exception as e:
                        logger.error(f"Bybit API spot: {e}", exc_info=True)
                        continue
                return
        except Exception as e:
            logger.error(f"Bybit API spot: {e}", exc_info=True)
            await asyncio.sleep(0.5)
