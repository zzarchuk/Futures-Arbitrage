import asyncio
import ccxt.async_support as ccxt
from ccxt.async_support import NetworkError, RequestTimeout
import datetime
import time
from datetime import timedelta
import aiohttp
import hmac
import base64
from datetime import datetime
import json
def чтобы_не_было_хуйни(data):
    required_keys = {"funding", "maker"}
    for symbol, exchanges in list(data.items()):
        for exchange, info in list(exchanges.items()):
            if not required_keys.issubset(info.keys()):
                del data[symbol][exchange]



# bybit = ccxt.bybit({"options": {"defaultType": "future"}, "enableRateLimit": True}),  # type: ignore
# okx = ccxt.okx({"options": {"defaultType": "future"}, "enableRateLimit": True}),  # type: ignore
# kucoin = ccxt.kucoin({"options": {"defaultType": "future"}, "enableRateLimit": True}),  # type: ignore
# gateio = ccxt.gateio({"options": {"defaultType": "future"}, "enableRateLimit": True}),  # type: ignore
# mexc = ccxt.mexc({"options": {"defaultType": "future"}, "enableRateLimit": True}),  # type: ignore
# # "coinbase": ccxt.coinbase({"options": {"defaultType": "spot"}}),
# bitget = ccxt.bitget({"options": {"defaultType": "future"}, "enableRateLimit": True}),  # type: ignore
# bingx = ccxt.bingx({"options": {"defaultType": "future"}, "enableRateLimit": True}),  # type: ignore

async def safe_fetch_funding_rates(exchange, retries=5, delay=4):
    for i in range(retries):
        try:
            if exchange.id == 'bitget':
                return await exchange.fetch_funding_rates(params={'subType': 'linear', 'productType': 'USDT-FUTURES'})
                # async with aiohttp.ClientSession() as session:
                #     async with session.get("https://api.bitget.com/api/v2/mix/market/current-fund-rate?productType=usdt-futures") as response:
                #         funding = await response.json()
                #         return funding
            if exchange.id == 'okx':

                apikey = "47813d19-df49-48cb-a148-2e7c2718fae7"
                secretkey = "4CC81EE3B2B14680ABA59CFB0875E7C8"
                passphrase = 'Zalupochka1!'
                
                timestamp = datetime.utcnow().isoformat()[:-3] + 'Z'
                method = 'GET'
                request_path = '/api/v5/public/funding-rate?instId=ANY'
                
                # Создание подписи
                message = timestamp + method + request_path
                mac = hmac.new(
                    bytes(secretkey, encoding='utf8'),
                    bytes(message, encoding='utf-8'),
                    digestmod='sha256'
                )
                signature = base64.b64encode(mac.digest()).decode()
                
                headers = {
                    'OK-ACCESS-KEY': apikey,
                    'OK-ACCESS-SIGN': signature,
                    'OK-ACCESS-TIMESTAMP': timestamp,
                    'OK-ACCESS-PASSPHRASE': passphrase,
                    'Content-Type': 'application/json'
                }
                
                url = 'https://www.okx.com' + request_path
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers) as response:
                        result = await response.json()
                        return result

                
            return await exchange.fetch_funding_rates()
        except (NetworkError, RequestTimeout) as e:
            print(f"NetworkError на {exchange.id}, попытка {i+1}/{retries}: {e}")
            await asyncio.sleep(delay)
    return None

async def safe_fetch_trading_fees(exchange, retries=5, delay=4):
    for i in range(retries):
        try:
            if exchange.id == 'bingx':
                return await exchange.fetch_markets()
            if exchange.id == 'okx':
                return await exchange.fetch_markets(params={'instType': 'SWAP'})


            
            return await exchange.fetch_trading_fees()
        except (NetworkError, RequestTimeout) as e:
            print(f"NetworkError на {exchange.id}, попытка {i+1}/{retries}: {e}")
            await asyncio.sleep(delay)
    return None


async def get_time_until_funding(funding_timestamp: int) -> str:
    # Определяем миллисекунды или секунды
    if funding_timestamp > 1e12:
        funding_timestamp /= 1000

    now = time.time()
    seconds_left = funding_timestamp - now

    if seconds_left <= 0:
        return "Фандинг уже произошёл."

    td = timedelta(seconds=seconds_left)
    hours, remainder = divmod(td.seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    days = td.days
    if days > 0:
        return f"До фандинга осталось {days} дн. {hours} ч. {minutes} мин."
    return f"До начисления фандинга осталось {hours} ч. {minutes} мин."


async def фильтрованный_словарь(data, symbol, exchange, maker=None, taker=None, funding=None, get_funding=None):
    if symbol not in data:
        data[symbol] = {}
    if exchange not in data[symbol]:
        data[symbol][exchange] = {}

    if maker is not None and taker is not None:
        data[symbol][exchange]["maker"] = maker
        data[symbol][exchange]["taker"] = taker

    if funding is not None and get_funding is not None:
        data[symbol][exchange]["funding"] = funding * 100  # % 
        data[symbol][exchange]["get_funding"] = await get_time_until_funding(get_funding)

async def binancee(data):
    binance = ccxt.binance(
        {
            "apiKey": "RlSkkX95SM6wEpc90ewL1Xe7e6lcmiuzJeMDRuOD2YYrmctuRBHGjuYAGLxoiYXA",
            "secret": "7aFzFhQUBInyWCXritxcidggFHjZpFW3Zj6d4F59fiAlwTZOSJ4PHrkVRGnBugpg",
            "options": {"defaultType": "future"},
            "enableRateLimit": True,
        } # type: ignore
    )  # type: ignore # <-- без запятой!
    exchange = binance.id
    try:
        funding = await safe_fetch_funding_rates(binance)
        fees = await safe_fetch_trading_fees(binance)
        #fees
        if fees is not None:
            for k, v in fees.items():
                if ':USDT' in k or k.endswith('/USDT:USDT'):
                    kk = k.replace("/", "")
                    symbol = kk.split(":")[0]
                    maker = v.get('maker')
                    taker = v.get('taker')
                    await фильтрованный_словарь(data, symbol, exchange, maker=maker, taker=taker)

        #funding
        if funding is not None:
            for k, v in funding.items():
                if ':USDT' in k or k.endswith('/USDT:USDT'):
                    kk = k.replace('/', '')
                    symbol = kk.split(':')[0]
                    фандинг = v.get('fundingRate')
                    начисление = v.get('fundingTimestamp')
                    await фильтрованный_словарь(data, symbol, exchange, funding=фандинг, get_funding=начисление)
                
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        await binance.close()


async def bingxx(data):
    bingx = ccxt.bingx({
        'apiKey': "qmjuI5SNJ066pEkzPfOrtEknDa22mIdrbwdT7Bjfj6OiiHq9s9w8IxralucNTYGgBDBz4njgjUwcx6URmGyFCA",
        'secret': 'i8pmzOUfKEpbLGidRNjNk0SEg6tiQJHxBNYUh6SaooegrkQV5d6bNVhmMECXidmLi2SIMBOV6sLptPMP7LaTg',
        "options": {"defaultType": "future"},
        "enableRateLimit": True

    })  # type: ignore # <-- без запятой!
    exchange = bingx.id
    try:
        funding = await safe_fetch_funding_rates(bingx)
        fees = await safe_fetch_trading_fees(bingx)

        #fees
        if fees is not None:
            for k in fees:
                if k.get('swap') is True and k.get('linear') is True and k.get('inverse') is False and k.get('settle') == 'USDT' and k.get('option') is False and k.get('future') is False and k.get('expiry') is None and k.get('active') is True:
                    
                    c = k.get('id')
                    symbol = c.replace('-', '')
                    maker = k.get('maker')
                    taker = k.get('taker')
                    await фильтрованный_словарь(data, symbol, exchange, maker=maker, taker=taker)


        #funding
        if funding is not None:
            #print(funding)
            for k, v in funding.items():
                if ':USDT' in k or k.endswith('/USDT:USDT'):
                    print(f'{k}\n\n')
                    kk = k.replace('/', '')
                    symbol = kk.split(':')[0]
                    фандинг = v.get('fundingRate')
                    начисление = v.get('nextFundingTimestamp')
                    await фильтрованный_словарь(data, symbol, exchange, funding=фандинг, get_funding=начисление)
    except Exception as e:
        print(f'Ошибка: {e}')
    finally:
        await bingx.close()
    
async def bitgett(data):
    bitget = ccxt.bitget({
        'apiKey': 'bg_b1ea92da7e883651b87fdfe2dbf033a6',
        'secret': '05566033c015b06766834cbcb7aaa6296f80312048852348b9210270a47ff709',
        "options": {"defaultType": "future"},
        "enableRateLimit": True

    })  # type: ignore # <-- без запятой!
    exchange = bitget.id
    try:
        # async with aiohttp.ClientSession() as session:
        #     async with session.get("https://api.bitget.com/api/v2/mix/market/current-fund-rate?symbol=BTCUSDT&productType=usdt-futures") as response:
        #         funding = await response.json()
        #         return funding
        fun = await safe_fetch_funding_rates(bitget)
        fees = await safe_fetch_trading_fees(bitget)

        #fees
        if fees is not None:
            for k, v in fees.items():
                #print(f'{k}\n{v}\n\n')
                if 'USDT' in k or k.endswith('/USDT:USDT'):
                    symbo = k.replace('/', '')
                    symbol = symbo.split(':')[0]
                    maker = v.get('maker')
                    taker = v.get('taker')
                    await фильтрованный_словарь(data, symbol, exchange, maker=maker, taker=taker)


        #funding                
            for k, v in fun.items():
                if ':USDT' in k or k.endswith('/USDT:USDT'):
                    #print(f'{k}\n{v}\n\n')
                    kk = k.replace('/', '')
                    symbol = kk.split(':')[0]
                    фандинг = v.get('fundingRate')
                    #начисление = v.get('fundingTimestamp')
                    начисление = 5
                    await фильтрованный_словарь(data, symbol, exchange, funding=фандинг, get_funding=начисление)
    except Exception as e:
        print(f'Ошибка: {e}')
    finally:
        await bitget.close()


async def bybitt(data):
    bybit = ccxt.bybit({
        'apiKey': "mKSVOO3FmriPpHZFat",
        'secret': "xpZPwQK1DB0Vxy1VGmIiKLTlBFT8Cc4Q9lVe",
        #"options": {"defaultType": "future"},  # важно! иначе будет спот
        "enableRateLimit": True
    }) # type: ignore
    exchange = bybit.id
    try:
        funding = await safe_fetch_funding_rates(bybit)
        fees = await safe_fetch_trading_fees(bybit)

        #fees
        if fees is not None:
            for k, v in fees.items():
                if ':USDT' in k or k.endswith('/USDT:USDT'):
                    symbo = k.replace('/', '')
                    symbol = symbo.split(':')[0]
                    maker = v.get('maker')
                    taker = v.get('taker')
                    await фильтрованный_словарь(data, symbol, exchange, maker=maker, taker=taker)


        #funding
        if funding is not None:
            for k, v in funding.items():
                if ':USDT' in k or k.endswith('/USDT:USDT'):
                    kk = k.replace('/', '')
                    symbol = kk.split(':')[0]
                    фандинг = v.get('fundingRate')
                    начисление = v.get('fundingTimestamp')
                    await фильтрованный_словарь(data, symbol, exchange, funding=фандинг, get_funding=начисление)
    except Exception as e:
        print(f'Ошибка: {e}')
    finally:
        await bybit.close()

async def gatee(data):
    gate = ccxt.gate({
        'apiKey': "0d462033d632b658b067a374303d0fd5",
        'secret': "dad81dc4be1cb652004ceecca375006d9108f36d5164f206ca05a2ea1ae0443f",
        "options": {"defaultType": "future"},  # важно! иначе будет спот
        "enableRateLimit": True
    }) # type: ignore
    exchange = gate.id
    try:
        funding = await safe_fetch_funding_rates(gate)
        fees = await safe_fetch_trading_fees(gate)

        #fees
        if fees is not None:
            for k, v in fees.items():
                if ':USDT' in k or k.endswith('/USDT:USDT'):
                    symbo = k.replace('/', '')
                    symbol = symbo.split(':')[0]
                    maker = v.get('maker')
                    taker = v.get('taker')
                    await фильтрованный_словарь(data, symbol, exchange, maker=maker, taker=taker)


        #funding
        if funding is not None:
            for k, v in funding.items():
                if ':USDT' in k or k.endswith('/USDT:USDT'):
                    kk = k.replace('/', '')
                    symbol = kk.split(':')[0]
                    фандинг = v.get('fundingRate')
                    начисление = v.get('fundingTimestamp')
                    await фильтрованный_словарь(data, symbol, exchange, funding=фандинг, get_funding=начисление)
    except Exception as e:
        print(f'Ошибка: {e}')
    finally:
        await gate.close()
        
async def okxx(data):
    okx = ccxt.okx({
        'apiKey': "47813d19-df49-48cb-a148-2e7c2718fae7",
        'secret': "4CC81EE3B2B14680ABA59CFB0875E7C8",
        'password': 'Zalupochka1!',
        "options": {"defaultType": "future"},  # важно! иначе будет спот
        "enableRateLimit": True
    }) # type: ignore
    exchange = okx.id
    try:
        funding = await safe_fetch_funding_rates(okx)
        fees = await safe_fetch_trading_fees(okx)

        #fees
        if fees is not None:
            for k in fees:
                #print(f'{k}\n\n')
                if k.get('swap') and k.get('linear') and k.get('active') and k.get('future') is False and k.get('option') is False and k.get('settle') == "USDT":
                    c = k.get('id')
                    symboll = c.replace('-', '')
                    symbol = symboll.replace('SWAP', '')
                    maker = k.get('maker')
                    taker = k.get('taker')
                    await фильтрованный_словарь(data, symbol, exchange, maker=maker, taker=taker)


        #funding
        if funding is not None:
            for k in funding['data']:
                #print(f'{k}\n\n')
                if 'USDT' in k.get('instId') and k.get('instType') == 'SWAP':
                    sy = k.get('instId')
                    sym = sy.replace('-', '')
                    symbol = sym.replace('SWAP', '')
                    фандинг = float(k.get('fundingRate'))
                    начисление = float(k.get('nextFundingTime'))
                    #print(начисление)
                    await фильтрованный_словарь(data, symbol, exchange, funding=фандинг, get_funding=начисление)
                    
    except Exception as e:
        print(f'Ошибка: {e}')
    finally:
        await okx.close()



async def main():
    data = {}
    #await asyncio.gather(binancee(data), bingxx(data), bitgett(data), bybitt(data), gatee(data), okxx(data))
    await asyncio.gather(bingxx(data=data), binancee(data))
    dataa = {k: v for k, v in data.items() if len(v) >= 2}
    with open('filtr.txt', 'w', encoding='utf-8') as f:
        json.dump(dataaa, f, ensure_ascii=False, indent=4)
    print(len(dataaa.keys()))


asyncio.run(main())

#okx bitget bingx