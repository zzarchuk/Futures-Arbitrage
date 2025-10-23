import time
import aiohttp
import hmac
from hashlib import sha256
import hashlib
import asyncio
import urllib.parse
import ccxt.async_support as ccxt
from ccxt.async_support import NetworkError, RequestTimeout
import datetime
import time
from datetime import datetime, timedelta
import aiohttp
import base64
from datetime import datetime
import json
from urllib.parse import urlencode

#пойми эту функцию
def чтобы_не_было_хуйни(data):
    required_keys = {"funding", "maker"}
    for symbol, exchanges in list(data.items()):
        for exchange, info in list(exchanges.items()):
            if not required_keys.issubset(info.keys()):
                del data[symbol][exchange]
        if not data[symbol]:
            del data[symbol]
    return data


async def safe_fetch_fees(exchange, retries=5, delay=4):
    for i in range(retries):
        try:
            if exchange.id == 'bingx':
                APIURL = "https://open-api.bingx.com"
                APIKEY = "qmjuI5SNJ066pEkzPfOrtEknDa22mIdrbwdT7Bjfj6OiiHq9s9w8IxralucNTYGgBDBz4njgjUwcx6URmGyFCA"
                SECRETKEY = 'i8pmzOUfKEpbLGidRNjNk0SEg6tiQJHxBNYUh6SaooegrkQV5d6bNVhmMECXidmLi2SIMBOV6sLptPMP7LaTg'

                # --- Формируем параметры ---
                params = {
                    "timestamp": str(int(time.time() * 1000))
                }

                # Сортируем и собираем в строку
                params_str = "&".join(f"{k}={v}" for k, v in sorted(params.items()))

                # Создаём HMAC SHA256 подпись
                signature = hmac.new(SECRETKEY.encode("utf-8"), params_str.encode("utf-8"), sha256).hexdigest()

                # Полный URL с подписью
                url = f"{APIURL}/openApi/swap/v2/quote/contracts?{params_str}&signature={signature}"

                # Заголовки
                headers = {
                    "X-BX-APIKEY": APIKEY
                }

                # --- Делаем запрос ---
                async with aiohttp.ClientSession() as session:
                    async with session.get(url=url, headers=headers) as response:
                        fees = await response.json()
                        return fees
            if exchange.id == 'binance':
                return await exchange.fetch_trading_fees(params={'subType': 'linear'})
            if exchange.id == 'bybit':
                return await exchange.fetch_trading_fees(params={'type': 'swap'})
            if exchange.id == 'bitget':
                async with aiohttp.ClientSession() as session:
                    async with session.get("https://api.bitget.com/api/v2/mix/market/contracts?productType=usdt-futures") as respone:
                        fees = await respone.json()
                        return fees
            if exchange.id == 'htx':
                API_KEY = "0d61a406-31e11796-bn2wed5t4y-92796"
                SECRET_KEY = "554474eb-dca5e37c-9d5db860-515f6"

                url = "https://api.hbdm.com/linear-swap-api/v1/swap_fee"
                host = urllib.parse.urlparse(url).hostname
                path = urllib.parse.urlparse(url).path

                # --- параметры для подписи (только служебные параметры) ---
                auth_params = {
                    "AccessKeyId": API_KEY,
                    "SignatureMethod": "HmacSHA256",
                    "SignatureVersion": "2",
                    "Timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
                }

                # --- формируем query string для подписи ---
                qs = '&'.join(f"{k}={urllib.parse.quote(str(auth_params[k]), safe='')}" 
                            for k in sorted(auth_params.keys()))

                # --- payload для HMAC ---
                payload = f"POST\n{host}\n{path}\n{qs}"

                # --- HMAC-SHA256 + Base64 ---
                signature = base64.b64encode(
                    hmac.new(SECRET_KEY.encode(), payload.encode(), hashlib.sha256).digest()
                ).decode()

                # --- добавляем подпись ---
                auth_params["Signature"] = signature

                # --- ВАЖНО: параметры авторизации идут в URL, а бизнес-параметры в body ---
                query_string = '&'.join(f"{k}={urllib.parse.quote(str(auth_params[k]), safe='')}" 
                                        for k in auth_params.keys())
                full_url = f"{url}?{query_string}"

                # --- бизнес-параметры в body (если нужны) ---
                body_params = {
                    "contract_type": "swap",
                    "business_type": "swap"
                }

                headers = {
                    "Content-Type": "application/json"
                }

                async with aiohttp.ClientSession() as session:
                    async with session.post(full_url, headers=headers, json=body_params) as resp:
                        text = await resp.text()  
                        try:
                            return json.loads(text)
                        except Exception as e:
                            print(f'Ошибка {e}')
            if exchange.id == 'okx':

                return await exchange.fetch_markets(params={'instType': 'SWAP'})
            if exchange.id == 'mexc':
                #print('поиск комисий начался')
                async with aiohttp.ClientSession() as session:
                    async with session.get("https://contract.mexc.com/api/v1/contract/detail") as respone:
                        fees = await respone.json()
                        return fees
        except Exception as e:
            print(f"NetworkError на {exchange.id}, попытка {i+1}/{retries}: {e}")
            await asyncio.sleep(delay)
    return None

async def safe_fetch_fundings(exchange, retries=5, delay=4, винйняток=None):
    for i in range(retries):
        try:
            if exchange.id == 'bingx':
                APIURL = "https://open-api.bingx.com"
                APIKEY = "qmjuI5SNJ066pEkzPfOrtEknDa22mIdrbwdT7Bjfj6OiiHq9s9w8IxralucNTYGgBDBz4njgjUwcx6URmGyFCA"
                SECRETKEY = 'i8pmzOUfKEpbLGidRNjNk0SEg6tiQJHxBNYUh6SaooegrkQV5d6bNVhmMECXidmLi2SIMBOV6sLptPMP7LaTg'

                # --- Формируем параметры ---
                params = {
                    "timestamp": str(int(time.time() * 1000))
                }

                # Сортируем и собираем в строку
                params_str = "&".join(f"{k}={v}" for k, v in sorted(params.items()))

                # Создаём HMAC SHA256 подпись
                signature = hmac.new(SECRETKEY.encode("utf-8"), params_str.encode("utf-8"), sha256).hexdigest()

                # Полный URL с подписью
                url = f"{APIURL}/openApi/swap/v2/quote/premiumIndex?{params_str}&signature={signature}"

                # Заголовки
                headers = {
                    "X-BX-APIKEY": APIKEY
                }

                # --- Делаем запрос ---
                async with aiohttp.ClientSession() as session:
                    async with session.get(url=url, headers=headers) as response:
                        fundings = await response.json()
                        return fundings
            if exchange.id == 'binance':
                api_key = "RlSkkX95SM6wEpc90ewL1Xe7e6lcmiuzJeMDRuOD2YYrmctuRBHGjuYAGLxoiYXA"
                api_secret = "7aFzFhQUBInyWCXritxcidggFHjZpFW3Zj6d4F59fiAlwTZOSJ4PHrkVRGnBugpg"

                timestamp = int(time.time() * 1000)
                query_string = f"timestamp={timestamp}"

                signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
                url = f"https://fapi.binance.com/fapi/v1/premiumIndex?{query_string}&signature={signature}"

                headers = {
                    "X-MBX-APIKEY": api_key
                }

                async with aiohttp.ClientSession() as session:
                    async with session.get(url=url, headers=headers) as response:
                        funding = await response.json()
                        return funding
            if exchange.id == 'bybit':
                url = "https://api.bybit.com/v5/market/tickers"
                params = {
                    "category": "linear",  # "spot", "linear", "inverse"
                }

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params) as resp:
                        fundings = await resp.json()
                        return fundings
            if exchange.id == 'bitget':
                async with aiohttp.ClientSession() as session:
                    async with session.get("https://api.bitget.com/api/v2/mix/market/current-fund-rate?productType=usdt-futures") as respone:
                        fundings = await respone.json()
                        return fundings
            if exchange.id == 'gateio':
            
                   
                #сразу фандинг и комса
                async with aiohttp.ClientSession() as session:
                    async with session.get(url="https://api.gateio.ws/api/v4/futures/usdt/contracts", headers={'Accept': 'application/json', 'Content-Type': 'application/json'}) as respone:
                        fundings_and_fees_gate = await respone.json()
                        return fundings_and_fees_gate
            if exchange.id == 'htx':
                async with aiohttp.ClientSession() as session:
                    async with session.get(url="https://api.hbdm.com/linear-swap-api/v1/swap_batch_funding_rate?contract_code=", headers={'Accept': 'application/json', 'Content-Type': 'application/json'}) as respone:
                        fundings = await respone.json()
                        return fundings
            if exchange.id == 'kucoin':
                async with aiohttp.ClientSession() as session:
                    async with session.get('https://api-futures.kucoin.com/api/v1/contracts/active') as respone:
                        fundings_and_fees_kucoin = await respone.json()
                        return fundings_and_fees_kucoin
            if exchange.id == 'okx':
                async with aiohttp.ClientSession() as session:
                    async with session.get("https://www.okx.com/api/v5/public/funding-rate?instId=ANY") as respone:
                        fundings = await respone.json()
                        return fundings
            if exchange.id == 'mexc':
                #print('поиск фандингов начался')
                result = []
                semaphore = asyncio.Semaphore(20)  # "светофор" - пропускает максимум 20 запросов одновременно
                
                async def fetch_one(symbol):
                    async with semaphore:  # ждём зелёного света (если уже 20 запросов идут - ждём)
                        async with aiohttp.ClientSession() as session:
                            async with session.get(f"https://contract.mexc.com/api/v1/contract/funding_rate/{symbol}") as response:
                                fundings = await response.json()
                                #print(f'пара добавилась {symbol}')
                                return fundings
                
                # Создаём задачи для ВСЕХ пар сразу
                tasks = [fetch_one(k) for k in винйняток] # type: ignore
                
                # Запускаем их батчами по 20 штук
                for i in range(0, len(tasks), 20):
                    batch = tasks[i:i+20]  # берём 20 задач
                    batch_results = await asyncio.gather(*batch)  # запускаем их одновременно
                    result.extend(batch_results)  # добавляем результаты
                    #print(f'Выполнено {i+20} из {len(tasks)} пар')
                    
                    if i + 20 < len(tasks):  # если ещё есть пары - ждём 2 секунды
                        await asyncio.sleep(2)
                
                return result
                # print('поиск фандингов начался')
                # result = []
                # for k in винйняток:
                #     async with aiohttp.ClientSession() as session:
                #         async with session.get(f"https://contract.mexc.com/api/v1/contract/funding_rate/{k}") as respone:
                #             fundings = await respone.json()
                #             result.append(fundings)
                #             print(f'пара добавилася {k}')
                #             #await asyncio.sleep(0.1)
                # return result

                            
        
        except Exception as e:
            print(f"NetworkError на {exchange.id}, попытка {i+1}/{retries}: {e}")
            await asyncio.sleep(delay)
    return None






async def get_time_until_funding(funding_timestamp: int, exchange_name) -> str:
    # Переводим миллисекунды → секунды и корректируем часовой пояс
    now = time.time()
    if exchange_name in ('bingx', 'okx'):
        funding_timestamp = funding_timestamp / 1000  # type: ignore # -2 часа
    # if exchange_name in ('bybit'):
    #     funding_timestamp = funding_timestamp / 1000 + 7200
    elif exchange_name == 'gateio':
        # Gate.io уже отдаёт timestamp в секундах, ничего не делаем
        pass
    else:
        funding_timestamp = funding_timestamp / 1000 # type: ignore

    seconds_left = funding_timestamp - now

    if seconds_left <= 0:
        return "Фандинг уже произошёл."

    td = timedelta(seconds=seconds_left)
    hours, remainder = divmod(int(td.total_seconds()), 3600)
    minutes, _ = divmod(remainder, 60)

    days = hours // 24
    hours = hours % 24

    if days > 0:
        return f"{days} дн. {hours} ч. {minutes} мин."
    return f"{hours} ч. {minutes} мин."




async def фильтрованный_словарь(data, symbol, exchange, maker=None, taker=None, funding=None, get_funding=None, index=None):
    if symbol not in data:
        data[symbol] = {}
    if exchange not in data[symbol]:
        data[symbol][exchange] = {}
        

    if maker is not None and taker is not None:
        data[symbol][exchange]["maker"] = maker
        data[symbol][exchange]["taker"] = taker

    if funding is not None and get_funding is not None:
        data[symbol][exchange]["funding"] = funding * 100  # % 
        data[symbol][exchange]["get_funding"] = await get_time_until_funding(get_funding, exchange)
    
    if index is not None:
        data[symbol][exchange]['index'] = index


async def bingxx(data):#есть
    bingx = ccxt.bingx({
        "options": {"defaultType": "future"},
        "enableRateLimit": True

    })  # type: ignore # <-- без запятой!
    exchange = bingx.id
    try:
        fees = await safe_fetch_fees(bingx)
        funding = await safe_fetch_fundings(bingx) 
        
        if fees is not None:
            for k in fees['data']:
                symbol = k.get('symbol').replace('-', '')
                maker = k.get('makerFeeRate')
                taker = k.get('takerFeeRate')
                await фильтрованный_словарь(data, symbol, exchange, maker=maker, taker=taker)

        if funding is not None:
            for k in funding['data']:
                symbol = k.get('symbol').replace('-', '')
                фандинг = float(k.get('lastFundingRate'))
                начисление = k.get('nextFundingTime')
                индекс = float(k.get('indexPrice'))
                await фильтрованный_словарь(data, symbol, exchange, funding=фандинг, get_funding=начисление, index=индекс)
                
            
            
        
    except Exception as e:
        print(f'Ошибка: {e}')
    finally:
        await bingx.close()

async def binancee(data):#есть
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
        funding = await safe_fetch_fundings(binance)
        fees = await safe_fetch_fees(binance)
        #fees
        if fees is not None:
            for k, v in fees.items():
                if k.endswith('USDT'):                    
                    kk = k.replace("/", "")
                    symbol = kk.split(":")[0]
                    maker = v.get('maker')
                    taker = v.get('taker')
                    await фильтрованный_словарь(data, symbol, exchange, maker=maker, taker=taker)

        #funding
        if funding is not None:
            for k in funding:
                if k.get('symbol').endswith('USDT'):
                    symbol = k.get('symbol')
                    фандинг = float(k.get('lastFundingRate'))
                    начисление = k.get('nextFundingTime')
                    индекс = float(k.get('indexPrice'))
                    await фильтрованный_словарь(data, symbol, exchange, funding=фандинг, get_funding=начисление, index=индекс)
                    
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        await binance.close()
    
async def bybitt(data):#есть
    bybit = ccxt.bybit(
        {
            "apiKey": "mKSVOO3FmriPpHZFat",
            "secret": "xpZPwQK1DB0Vxy1VGmIiKLTlBFT8Cc4Q9lVe",
            "options": {"defaultType": "future"},
            "enableRateLimit": True,
        } # type: ignore
    )  # type: ignore # <-- без запятой!
    exchange = bybit.id
    try:
        funding = await safe_fetch_fundings(bybit)
        fees = await safe_fetch_fees(bybit)
        #fees
        if fees is not None:
            for k, v in fees.items():
                if k.endswith('USDT:USDT'): 
                    kk = k.replace("/", "")
                    symbol = kk.split(":")[0]
                    maker = v.get('maker')
                    taker = v.get('taker')
                    await фильтрованный_словарь(data, symbol, exchange, maker=maker, taker=taker)

        #funding
        if funding is not None:
            for k in funding['result']['list']:
                if k.get('deliveryTime') == '0' and k.get('predictedDeliveryPrice') == '' and k.get('symbol').endswith('USDT'):
                    symbol = k.get('symbol')
                    фандинг = float(k.get('fundingRate'))
                    начисление = float(k.get('nextFundingTime'))
                    индекс = float(k.get('indexPrice'))
                    await фильтрованный_словарь(data, symbol, exchange, funding=фандинг, get_funding=начисление, index=индекс)
                    
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        await bybit.close()
    
async def bitgett(data):
    bitget = ccxt.bitget({'options': {'defaultType': 'future'}, 'enableRateLimit': True}) # type: ignore
    
    exchange = bitget.id
    try:
        fundings = await safe_fetch_fundings(bitget)
        fees = await safe_fetch_fees(bitget)
        
        #funding
        if fundings is not None:
            for k in fundings['data']:
                symbol = k.get('symbol')
                фандинг = float(k.get('fundingRate'))
                начисление = float(k.get('nextUpdate'))
                await фильтрованный_словарь(data, symbol, exchange, funding=фандинг, get_funding=начисление)
        
        #fees
        if fees is not None:
            for k in fees['data']:
                symbol = k.get('symbol')
                maker = float(k.get('makerFeeRate'))
                taker = float(k.get('takerFeeRate'))
                await фильтрованный_словарь(data, symbol, exchange, maker=maker, taker=taker)
                
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        await bitget.close()

async def gatee(data):#есть
    gate = ccxt.gateio({
        'apiKey': "0d462033d632b658b067a374303d0fd5",
        'secret': "dad81dc4be1cb652004ceecca375006d9108f36d5164f206ca05a2ea1ae0443f",
        "options": {"defaultType": "future"},  # важно! иначе будет спот
        "enableRateLimit": True
    }) # type: ignore
    exchange = gate.id
    try:
        fundings_and_fees = await safe_fetch_fundings(gate)
        
        if fundings_and_fees is not None:
            for k in fundings_and_fees:
                if k.get('type') == 'direct':
                    symbol = k.get('name').replace('_', '')
                    фандинг = float(k.get('funding_rate'))
                    начисление = float(k.get('funding_next_apply'))
                    maker = float(k.get('maker_fee_rate'))
                    taker = float(k.get('taker_fee_rate'))
                    индекс = float(k.get('index_price'))
                    await фильтрованный_словарь(data, symbol, exchange, maker=maker, taker=taker, funding=фандинг, get_funding=начисление, index=индекс)
                    
                    
                    
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        await gate.close()
   
async def htxx(data):
    htx = ccxt.htx({
        
        "options": {"defaultType": "future"},  # важно! иначе будет спот
        "enableRateLimit": True
    }) # type: ignore
    exchange = htx.id
    try:                    
        funding = await safe_fetch_fundings(htx)
        fees = await safe_fetch_fees(htx)
        
        
        if funding is not None:
            for k in funding['data']:
                if k.get('contract_code').endswith('USDT') and k.get('funding_rate') != None and k.get('funding_time') != None:
                    symbol = k.get('contract_code').replace('-', '')
                    фандинг = float(k.get('funding_rate'))
                    начисление = float(k.get('funding_time'))
                    await фильтрованный_словарь(data, symbol, exchange, funding=фандинг, get_funding=начисление)
                    
        if fees is not None:
            for k in fees['data']:
                symbol = k.get('contract_code').replace('-', '')
                maker = float(k.get('open_maker_fee'))
                taker = float(k.get('open_taker_fee'))
                await фильтрованный_словарь(data, symbol, exchange, maker=maker, taker=taker)
                
                
    
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        await htx.close()

async def kucoinn(data):
    kucoin = ccxt.kucoin({
        
        "options": {"defaultType": "future"},  # важно! иначе будет спот
        "enableRateLimit": True
    }) # type: ignore
    exchange = kucoin.id  
    try:
        fundings_and_fees = await safe_fetch_fundings(kucoin)
        
        if fundings_and_fees is not None:
            for k in fundings_and_fees['data']:
                if k.get('quoteCurrency') == 'USDT' and k.get('expireDate') == None and k.get('symbol').endswith('M') and k.get('type') == 'FFWCSX' and k.get('status') == 'Open':
                    symbol = k.get('symbol').rstrip('M')
                    maker = float(k.get('makerFeeRate'))
                    taker = float(k.get('takerFeeRate'))
                    фандинг = float(k.get('fundingFeeRate'))
                    начисление = float(k.get('nextFundingRateDateTime'))
                    await фильтрованный_словарь(data, symbol, exchange, maker=maker, taker=taker, funding=фандинг, get_funding=начисление)
                
                
                
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        await kucoin.close()

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
        fundings = await safe_fetch_fundings(okx)
        fees = await safe_fetch_fees(okx)
        
        if fundings is not None:
            for k in fundings['data']:
                if k.get('formulaType') == 'withRate' and k.get('instType') == 'SWAP' and k.get('instId').replace('-SWAP', '').endswith('USDT'):
                    symbol = k.get('instId').replace('-', '').replace('SWAP', '')
                    фандинг = float(k.get('fundingRate'))
                    начисление = float(k.get('fundingTime'))
                    await фильтрованный_словарь(data, symbol, exchange, funding=фандинг, get_funding=начисление)
        
        
        if fees is not None:
            for k in fees:
                if k.get('type') == 'swap' and k.get('linear') is True and k.get('settle') == 'USDT' and k.get('quote') == 'USDT' and k.get('active') is True:
                    symbol = k.get('id').replace('-', '').replace('SWAP', '')
                    taker = float(k.get('taker'))
                    maker = float(k.get('maker'))
                    await фильтрованный_словарь(data, symbol, exchange, maker=maker, taker=taker)
        

    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        await okx.close()

async def mexcc(data):
    вийняток = []
    mexc = ccxt.mexc({
        'apiKey': "mx0vgleZovkx65J1Xd",
        'secret': "765e00e08caa4506a7bbcecd7b55d5ea",
        "options": {"defaultType": "future"},  # важно! иначе будет спот
        "enableRateLimit": True
    }) # type: ignore
    exchange = mexc.id
    try:
        fees = await safe_fetch_fees(mexc)
        
        if fees is not None:
            for k in fees['data']:
                if k.get('futureType') == 1 and k.get('settleCoin') == 'USDT' and k.get('state') == 0 and k.get('isHidden') == False:
                    вийняток.append(k.get('symbol'))
                    symbol = k.get('symbol').replace('_', '')
                    maker = float(k.get('makerFeeRate'))
                    taker = float(k.get('takerFeeRate'))
                    await фильтрованный_словарь(data, symbol, exchange, maker=maker, taker=taker)
            #print('поиск комиссий окончился')
        
        fundings = await safe_fetch_fundings(mexc, винйняток=вийняток)
        
        if fundings is not None:
            for k in fundings:
                symbol = k['data'].get('symbol').replace('_', '')
                фандинг = k['data'].get('fundingRate')
                начисление = k['data'].get('nextSettleTime')
                await фильтрованный_словарь(data, symbol, exchange, funding=фандинг, get_funding=начисление)
                
                
                
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        await mexc.close()

#lbank xt bitmart 



async def api():
    data = {}
    await asyncio.gather(binancee(data=data), bybitt(data), bitgett(data), gatee(data), kucoinn(data), okxx(data), mexcc(data), bingxx(data), htxx(data))
    #await asyncio.gather(mexcc(data))


    # with open('filtr.txt', 'w', encoding='utf-8') as f:
    #     json.dump(data, f, ensure_ascii=False, indent=4)
    # print(len(data.keys()))
    
    дата_со_всеми_фандингами_и_тейкерами = чтобы_не_было_хуйни(data)

    дата_минимум_2_биржи = {k: v for k, v in дата_со_всеми_фандингами_и_тейкерами.items() if len(v) >= 2}
    with open('filtr.txt', 'w', encoding='utf-8') as f:
        json.dump(дата_минимум_2_биржи, f, ensure_ascii=False, indent=4)
    print(len(дата_минимум_2_биржи.keys()))
    return дата_минимум_2_биржи


#asyncio.run(api())