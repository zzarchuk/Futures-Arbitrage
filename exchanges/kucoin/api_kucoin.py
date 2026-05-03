import asyncio
from utils.exchange_api.filter_api import filtered_dict
import logging

logger = logging.getLogger(__name__)

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
                            last_price = key.get("last")
                            if last_price in (None, "", "0"):
                                continue

                            цена = float(last_price)
                            filtered_dict(
                                data=data,
                                exchange="kucoin",
                                symbol=symbol,
                                price=цена,
                                spot=True,
                            )
                    except Exception as e:
                        logger.error(f"Kucoin API spot: {e}", exc_info=True)
                        continue
                return
        except Exception as e:
            logger.error(f"Kucoin API spot: {e}", exc_info=True)
            await asyncio.sleep(0.5)
            

async def kucoin_futures(data, session):
    async def price():
        async with session.get(
            url="https://api-futures.kucoin.com/api/v1/allTickers"
        ) as response:
            price = await response.json()
            for key in price["data"]:
                try:
                    if key.get("symbol").endswith("USDTM"):
                        symbol = key.get("symbol").replace("USDTM", "USDT")
                        цена = float(key.get("price"))
                        filtered_dict(
                            data=data,
                            exchange="kucoin",
                            symbol=symbol,
                            price=цена,
                            futures=True,
                        )
                except Exception as e:
                    logger.error(f"Kucoin API futures: {e}", exc_info=True)
                    continue
        return
    async def fundings():
        async with session.get(
            url="https://api-futures.kucoin.com/api/v1/contracts/active"
        ) as response:
            price = await response.json()
            for k in price["data"]:
                try:
                    if k.get('quoteCurrency') == 'USDT' and k.get('expireDate') == None and k.get('symbol').endswith('M') and k.get('type') == 'FFWCSX' and k.get('status') == 'Open':
                        symbol = k.get('symbol').rstrip('M')

                        фандинг = float(k.get('fundingFeeRate'))
                        начисление = float(k.get('nextFundingRateDateTime'))
                    filtered_dict(data=data, symbol=symbol, exchange='kucoin', funding=фандинг, get_funding=начисление)
                except Exception as e:
                    logger.error(f"Kucoin API futures: {e}", exc_info=True)
                    continue
        return
    for i in range(1, 7):
        try:
            await asyncio.gather(price(), fundings())
            break
        except Exception as e:
            logger.error(f"Kucoin API futures: {e}", exc_info=True)
            await asyncio.sleep(0.5)