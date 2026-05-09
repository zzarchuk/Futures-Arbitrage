import asyncio
from utils.exchange_api.filter_api import filtered_dict
import logging

logger = logging.getLogger(__name__)

async def gateio_spot(data, session):
    for i in range(1, 7):
        try:
            async with session.get(
                url="https://api.gateio.ws/api/v4/spot/tickers",
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
            ) as response:
                price = await response.json()
                for key in price:
                    try:
                        if key.get("currency_pair").endswith("USDT"):
                            symbol = key.get("currency_pair").replace("_", "")
                            цена = float(key.get("last"))
                            filtered_dict(
                                data=data,
                                exchange="gateio",
                                symbol=symbol,
                                price=цена,
                                spot=True,
                            )
                    except Exception as e:
                        logger.error(f"Gate API spot: {e}", exc_info=True)
                        continue
                return
        except Exception as e:
            logger.error(f"Gate API spot: {e}", exc_info=True)
            await asyncio.sleep(0.5)



async def gateio_futures(data, session):
    async def price():
        async with session.get(
            url="https://api.gateio.ws/api/v4/futures/usdt/contracts",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        ) as response:
            price = await response.json()
            for key in price:
                try:
                    if key.get("name").endswith("USDT"):
                        symbol = key.get("name").replace("_", "")
                        цена = float(key.get("last_price"))
                        filtered_dict(
                            data=data,
                            exchange="gateio",
                            symbol=symbol,
                            price=цена,
                            futures=True,
                        )
                        
                except Exception as e:
                    logger.error(f"Gate API futures: {e}", exc_info=True)
                    continue
        return
    
    async def fundings():
        async with session.get(
            url="https://api.gateio.ws/api/v4/futures/usdt/contracts", headers={'Accept': 'application/json', 'Content-Type': 'application/json'},
        ) as response:
            price = await response.json()
            for k in price:
                try:
                    if k.get('type') == 'direct':
                        symbol = k.get('name').replace('_', '')
                        фандинг = float(k.get('funding_rate'))
                        начисление = float(k.get('funding_next_apply'))
                        # maker = float(k.get('maker_fee_rate'))
                        # taker = float(k.get('taker_fee_rate'))
                        # индекс = float(k.get('index_price'))
                        filtered_dict(data=data, symbol=symbol, exchange='gateio', funding=фандинг, get_funding=начисление)
                except Exception as e:
                    logger.error(f"Gate API futures: {e}", exc_info=True)
                    continue
        return
    for i in range(1, 7):
        try:
            await asyncio.gather(price(), fundings())
            break
        except Exception as e:
            logger.error(f"Gate API futures: {e}", exc_info=True)
            await asyncio.sleep(0.5)
            
