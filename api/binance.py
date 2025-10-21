

# import aiohttp
# import asyncio
# import json

# symbol = "BTCUSDT"
# url = f"https://fapi.binance.com/fapi/v1/premiumIndex"

# async def gett(url):
#     async with aiohttp.ClientSession() as session:
#         async with session.get(url) as response:
#             return await response.json()
        
# print(asyncio.run(gett(url)))
# response = requests.get(url).json()
# zz = []
# for k in response:
    
#     zz.append(k.get('symbol'))
#     #for key, value in k.items():
# print(len(zz))


import time
import hmac
import hashlib
import requests
import aiohttp
import asyncio
import json

api_key = "RlSkkX95SM6wEpc90ewL1Xe7e6lcmiuzJeMDRuOD2YYrmctuRBHGjuYAGLxoiYXA"
api_secret = "7aFzFhQUBInyWCXritxcidggFHjZpFW3Zj6d4F59fiAlwTZOSJ4PHrkVRGnBugpg"

symbol = ''
timestamp = int(time.time() * 1000)
query_string = f"symbol={symbol}&timestamp={timestamp}"

signature = hmac.new(
    api_secret.encode('utf-8'),
    query_string.encode('utf-8'),
    hashlib.sha256
).hexdigest()

url = f"https://fapi.binance.com/fapi/v1/commissionRate?{query_string}&signature={signature}"
headers = {"X-MBX-APIKEY": api_key}

async def get_fee(url, headers):
    async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                return await response.json()
            
print(asyncio.run(get_fee(url, headers)))
#response = requests.get(url, headers=headers).json()

