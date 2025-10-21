# from pybit.unified_trading import HTTP
# session = HTTP()
# print(session.get_funding_rate_history(
#     category="linear",
#     #symbol="BTCUSDT",
#     limit=1,
# ))

import asyncio
import ccxt.async_support as ccxt


async def g():
    bybit = ccxt.bybit({
        'apiKey': "mKSVOO3FmriPpHZFat",
        'secret': "xpZPwQK1DB0Vxy1VGmIiKLTlBFT8Cc4Q9lVe",
        "options": {"defaultType": "future"},  # важно! иначе будет спот
        "enableRateLimit": True
    }) # type: ignore
    try:
        data = await bybit.fetch_funding_rates(symbols=['BTC/USDT:USDT', 'ETH/USDT:USDT'])  # можно без symbols или с
        #data = await bybit.fetch_trading_fees(params={'type': 'swap'})
        return data
    finally:
        await bybit.close()  # обязательно закрываем соединение

print(asyncio.run(g()))

