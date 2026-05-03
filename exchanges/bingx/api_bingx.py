import asyncio
from utils.exchange_api.filter_api import filtered_dict
import logging

logger = logging.getLogger(__name__)

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
                        filtered_dict(
                            data=data,
                            exchange="bingx",
                            symbol=symbol,
                            price=цена,
                            futures=True,
                        )
                except Exception as e:
                    logger.error(f"Bingx futures API: {e}", exc_info=True)
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
                    filtered_dict(data=data, symbol=symbol, exchange='bingx', funding=фандинг, get_funding=начисление)
                except Exception as e:
                    logger.error(f"Bingx futures API: {e}", exc_info=True)
        return
    for i in range(1, 7):
        try:
            await asyncio.gather(price(), funding())
            break
        except Exception as e:
            logger.error(f"Bingx futures API: {e}", exc_info=True)
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
                            filtered_dict(
                                data=data,
                                exchange="bingx",
                                symbol=symbol,
                                price=цена,
                                spot=True,
                            )
                    except Exception as e:
                        logger.error(f"Bingx spot API: {e}", exc_info=True)
                        continue
                return
        except Exception as e:
            logger.error(f"Bingx spot API: {e}", exc_info=True)
            await asyncio.sleep(0.5)