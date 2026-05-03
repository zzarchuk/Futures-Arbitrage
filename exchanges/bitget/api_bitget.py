import asyncio
from utils.exchange_api.filter_api import filtered_dict
import logging

logger = logging.getLogger(__name__)


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
                        logger.error(f'Bitget API spot: {e}', exc_info=True)
                        continue
                return
        except Exception as e:
            logger.error(f'Bitget API spot: {e}', exc_info=True)
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
                    logger.error(f'Bitget API futures: {e}', exc_info=True)
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
                    logger.error(f'Bitget API futures: {e}', exc_info=True)
                    continue
        return
    
    for i in range(1, 7):
        try:
            await asyncio.gather(price(), fundings())
            break
        except Exception as e:
            logger.error(f'Bitget API futures: {e}', exc_info=True)
            await asyncio.sleep(0.5)