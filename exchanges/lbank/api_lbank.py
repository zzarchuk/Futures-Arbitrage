import asyncio
from utils.exchange_api.filter_api import filtered_dict
import logging

logger = logging.getLogger(__name__)

async def lbank_futures(data, session):
    
    for i in range(1, 7):
        try:
            async with session.get(
                url="https://lbkperp.lbank.com/cfd/openApi/v1/pub/marketData?productGroup=SwapU"
            ) as response:
                price = await response.json()
                for key in price["data"]:
                    try:
                        if (
                            key.get("symbol").endswith("USDT")
                            and key.get("lastPrice")
                            and key.get("instrumentStatus") != "1"
                            and key.get("turnover") != "0"
                            and key.get("volume") != "0"
                        ):
                            symbol = key.get("symbol")
                            цена = float(key.get("lastPrice"))
                            funding = float(key.get('fundingRate', 0))
                            next = key.get('nextFeeTime')
                            filtered_dict(
                                data=data,
                                exchange="lbank",
                                symbol=symbol,
                                price=цена,
                                funding=funding,
                                get_funding=next,
                                futures=True,
                            )
                    except Exception as e:
                        logging.error(f'Lbank API futures: {e}', exc_info=True)
                        continue
                return
        except Exception as e:
            logging.error(f'Lbank API futures: {e}', exc_info=True)
            await asyncio.sleep(0.5)



async def lbank_spot(data, session):
    for i in range(1, 7):
        try:
            async with session.get(
                url="https://api.lbank.info/v2/supplement/ticker/price.do"
            ) as response:
                price = await response.json()
                for key in price["data"]:
                    try:
                        if key.get("symbol").endswith("_usdt"):
                            symbol = key.get("symbol").upper().replace("_USDT", "USDT")
                            цена = float(key.get("price"))
                            filtered_dict(
                                data=data,
                                exchange="lbank",
                                symbol=symbol,
                                price=цена,
                                spot=True,
                            )
                    except Exception as e:
                        logging.error(f'Lbank API spot: {e}', exc_info=True)
                        continue
                return
        except Exception as e:
            logging.error(f'Lbank API spot: {e}', exc_info=True)
            await asyncio.sleep(0.5)