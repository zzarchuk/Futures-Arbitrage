import asyncio
from utils.exchange_api.filter_api import filtered_dict
import logging

logger = logging.getLogger(__name__)

async def binance_spot(data, session):
    async def price():
        async with session.get(
            url="https://api.binance.com/api/v3/ticker/price"
        ) as response:
            price = await response.json()
            for key in price:
                try:
                    if key.get("symbol").endswith("USDT"):
                        symbol = key.get("symbol")
                        цена = float(key.get("price"))
                        filtered_dict(
                            data=data,
                            exchange="binance",
                            symbol=symbol,
                            price=цена,
                            spot=True,
                        )
                except Exception as e:
                    logger.error(f"Binance spot API: {e}", exc_info=True)
                    continue
        return
    
    async def volume():
        async with session.get(
            url="https://api.binance.com/api/v3/exchangeInfo"
        ) as response:
            price = await response.json()
            for key in price['symbols']:
                try:
                    if key.get("symbol", "").endswith("USDT") and key.get("isSpotTradingAllowed") is True:
                        symbol = key["symbol"]

                        max_notional = None

                        # Перебираем фильтры
                        for f in key.get("filters", []):
                            if f.get("filterType") == "NOTIONAL":
                                max_notional = float(f.get("maxNotional", 0))
                                break

                        if max_notional is None:
                            max_notional = 0  # или continue

                        filtered_dict(
                            data=data,
                            exchange="binance",
                            symbol=symbol,
                            max_vol=max_notional,
                            spot=True
                        )
                except Exception as e:
                    logger.error(f"Binance spot API volume: {e}", exc_info=True)
                    continue
        return
    for i in range(1, 7):
        try:
            await asyncio.gather(price(), volume())
            break
        except Exception as e:
            logger.error(f"Binance spot API filter: {e}", exc_info=True)
            await asyncio.sleep(0.5)
            
            
async def binance_futures(data, session):
    async def binance_funding():
        async with session.get(
            url="https://fapi.binance.com/fapi/v1/premiumIndex"
        ) as response:
            price = await response.json()
            if price is not None:
                for k in price:
                    try:
                        if k.get('symbol').endswith('USDT'):
                            symbol = k.get('symbol')
                            фандинг = float(k.get('lastFundingRate'))
                            начисление = k.get('nextFundingTime')
                            filtered_dict(data=data, symbol=symbol, exchange='binance', funding=фандинг, get_funding=начисление)
                    except Exception as e:
                        logger.error(f"Binance futures API: {e}", exc_info=True)
                        continue
        return
        
    async def binance_price():
        async with session.get(
            url="https://fapi.binance.com/fapi/v2/ticker/price"
        ) as response:
            price = await response.json()
            for key in price:
                try:
                    if key.get("symbol").endswith("USDT"):
                        symbol = key.get("symbol")
                        цена = float(key.get("price"))
                        filtered_dict(
                            data=data,
                            exchange="binance",
                            symbol=symbol,
                            price=цена,
                            futures=True,
                        )
                except Exception as e:
                    logger.error(f"Binance futures API: {e}", exc_info=True)
                    continue
        return

        return
    for i in range(1, 7):
        try:
            await asyncio.gather(binance_price(), binance_funding())
            break
        except Exception as e:
            logger.error(f"Binance futures API: {e}", exc_info=True)
            await asyncio.sleep(0.5)

async def binance_volume_futures(session, data):
    symbols = list(set(symbol for symbol, exchange in data.items() if 'binance' in exchange and 'futures' in exchange['binance']))
    async with session.get(
        url="https://fapi.binance.com/fapi/v1/exchangeInfo"
    ) as response:
        volume = await response.json()
        for k in volume['symbols']:
            try:
                if k.get('symbol').endswith('USDT') and k.get('symbol') in symbols and k.get('contractType') == 'PERPETUAL' and k.get('status') == 'TRADING':
                    symbol = k.get('symbol')
                    
                    max_market_qty = float(next((f['maxQty'] for f in k.get('filters') if f['filterType'] == 'MARKET_LOT_SIZE'), 0))
                    price = data[symbol]['binance']['futures'].get('price', 0)
                    if price == 0:
                        continue
                    data[symbol]['binance']['futures']['max_vol'] = max_market_qty * price
            except Exception as e:
                logger.error(f"Binance futures API: {e}", exc_info=True)
                continue
    return data