import aiohttp
import hmac
import hashlib
import time
import json
import asyncio

bybit_api = '8t3EVhQOBgow2YZf6B'
bybit_api_secret = 'k5f7qPNqxvoBuysXqMzXoZTGTfD521A0W9pm'

async def trading_long(symbol, ex, volume):
    for exchange, type in ex.items():
        if type == 'futures':
            if exchange == 'bybit':
                url = "https://api.bybit.com/v5/order/create"
                timestamp = str(int(time.time() * 1000))
                recv_window = "5000"

                body = {
                    "category": "linear",
                    "symbol": symbol,
                    "side": "Buy",
                    "orderType": "Market",
                    "qty": str(volume)
                }

                body_json = json.dumps(body, separators=(',', ':'))

                # ---- 2. Создаём строку для подписи ----
                sign_payload = f"{timestamp}{bybit_api}{recv_window}{body_json}"

                signature = hmac.new(
                    bybit_api_secret.encode(),
                    sign_payload.encode(),
                    hashlib.sha256
                ).hexdigest()

                headers = {
                    "Content-Type": "application/json",
                    "X-BAPI-API-KEY": bybit_api,
                    "X-BAPI-TIMESTAMP": timestamp,
                    "X-BAPI-RECV-WINDOW": recv_window,
                    "X-BAPI-SIGN": signature
                }

                # ---- 3. Делаем POST ----
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, data=body_json) as resp:
                        data = await resp.json()
                        print("BYBIT LONG ORDER:", data)
                        return data                        
        elif type == 'spot':
            if exchange == 'bybit':
                url = "https://api.bybit.com/v5/order/create"
                timestamp = str(int(time.time() * 1000))
                recv_window = "5000"

                body = {
                    "category": "spot",
                    "symbol": symbol,
                    "side": "Buy",
                    "orderType": "Market",
                    "qty": str(volume),
                    'marketUnit': 'baseCoin'
                }

                body_json = json.dumps(body, separators=(',', ':'))

                # ---- 2. Создаём строку для подписи ----
                sign_payload = f"{timestamp}{bybit_api}{recv_window}{body_json}"

                signature = hmac.new(
                    bybit_api_secret.encode(),
                    sign_payload.encode(),
                    hashlib.sha256
                ).hexdigest()

                headers = {
                    "Content-Type": "application/json",
                    "X-BAPI-API-KEY": bybit_api,
                    "X-BAPI-TIMESTAMP": timestamp,
                    "X-BAPI-RECV-WINDOW": recv_window,
                    "X-BAPI-SIGN": signature
                }

                # ---- 3. Делаем POST ----
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, data=body_json) as resp:
                        data = await resp.json()
                        print("BYBIT LONG ORDER:", data)
                        return data
            
async def main():
    await trading_long('GMTUSDT', {'bybit': 'spot'}, 300)
    
asyncio.run(main())