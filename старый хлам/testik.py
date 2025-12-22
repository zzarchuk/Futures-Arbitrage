import json
import aiohttp
import asyncio


async def фильтрованный_словарь(data, exchange, symbol, price):
    if symbol not in data:
        data[symbol] = {}
    if exchange not in data[symbol]:
        data[symbol][exchange] = {}
        
    if price is not None:
        data[symbol][exchange]['price'] = price 
    
        


async def binance(data, session):
    for i in range(1, 7):
        try:
            # async with session.get(url='https://fapi.binance.com/fapi/v1/premiumIndex') as response:
            async with session.get(url='https://fapi.binance.com/fapi/v2/ticker/price') as response:
                price = await response.json()
                for key in price:
                    if key.get('symbol').endswith('USDT'):
                        symbol = key.get('symbol')
                        цена = float(key.get('price'))
                        await фильтрованный_словарь(data=data, exchange='binance', symbol=symbol, price=цена)
                return
        except Exception as e:
            print(f'Ошибка в binance в файле фильтра\n{e}')
            await asyncio.sleep(0.5)
        
async def bybit(data, session):
    for i in range(1, 7):
        try:
            async with session.get(url='https://api.bybit.com/v5/market/tickers', params={"category": "linear"}) as response:
                price = await response.json()
                for key in price['result']['list']:
                    if key.get('symbol').endswith('USDT'):
                        symbol = key.get('symbol')
                        цена = float(key.get('lastPrice'))
                        await фильтрованный_словарь(data=data, exchange='bybit', symbol=symbol, price=цена)
                return
        except Exception as e:
            print(f'Ошибка в bybit в файле фильтра\n{e}')
            await asyncio.sleep(0.5)
   
async def bingx(data, session):
    for i in range(1, 7):
        try:
            # async with session.get(url='https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex') as response:
            async with session.get(url='https://open-api.bingx.com/openApi/swap/v1/ticker/price') as response:
                price = await response.json()
                for key in price['data']:
                    if key.get('symbol').endswith('USDT'):
                        symbol = key.get('symbol').replace('-', '')
                        цена = float(key.get('price'))
                        await фильтрованный_словарь(data=data, exchange='bingx', symbol=symbol, price=цена)
                return
        except Exception as e:
            print(f'Ошибка в bingx в файле фильтра\n{e}')
            await asyncio.sleep(0.5)
        
async def mexc(data, session):
    for i in range(1, 7):
        try:
            async with session.get(url="https://contract.mexc.com/api/v1/contract/ticker") as response:
                price = await response.json()
                for key in price['data']:
                    if key.get('symbol').endswith('USDT'):
                        symbol = key.get('symbol').replace('_', '')
                        цена = key.get('lastPrice')
                        await фильтрованный_словарь(data=data, exchange='mexc', symbol=symbol, price=цена)
                return
        except Exception as e:
            print(f'Ошибка в mexc в файле фильтра\n{e}')
            await asyncio.sleep(0.5)
        
async def bitget(data, session):
    for i in range(1, 7):
        try:
            async with session.get(url="https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES") as response:
                price = await response.json()
                for key in price['data']:
                    if key.get('symbol').endswith('USDT'):
                        symbol = key.get('symbol')
                        цена = float(key.get('lastPr'))
                        await фильтрованный_словарь(data=data, exchange='bitget', symbol=symbol, price=цена)
                return
        except Exception as e:
            print(f'Ошибка в bitget в файле фильтра\n{e}')
            await asyncio.sleep(0.5)
              
async def gateio(data, session):
    for i in range(1, 7):
        try:
            async with session.get(url="https://api.gateio.ws/api/v4/futures/usdt/contracts", headers={'Accept': 'application/json', 'Content-Type': 'application/json'}) as response:
                price = await response.json()
                for key in price:
                    if key.get('name').endswith('USDT'):
                        symbol = key.get('name').replace('_', '')
                        цена = float(key.get('last_price'))
                        await фильтрованный_словарь(data=data, exchange='gateio', symbol=symbol, price=цена)
                return
        except Exception as e:
            print(f'Ошибка в gateio в файле фильтра\n{e}')
            await asyncio.sleep(0.5)        
  
async def okx(data, session):
    for i in range(1, 7):
        try:
            # async with session.get(url="https://www.okx.com/api/v5/public/mark-price?instType=SWAP") as response:
            async with session.get(url="https://www.okx.com/api/v5/market/tickers?instType=SWAP") as response:
                price = await response.json()
                for key in price['data']:
                    if key.get('instId').endswith('USDT-SWAP'):
                        symbol = key.get('instId').replace('-USDT-SWAP', 'USDT')
                        цена = float(key.get('last'))
                        await фильтрованный_словарь(data=data, exchange='okx', symbol=symbol, price=цена)
                return
        except Exception as e:
            print(f'Ошибка в оkx в файле фильтра\n{e}')
            await asyncio.sleep(0.5)  
        
async def kucoin(data, session):
    for i in range(1, 7):
        try:
            async with session.get(url="https://api-futures.kucoin.com/api/v1/allTickers") as response:
                price = await response.json()
                for key in price['data']:
                    if key.get('symbol').endswith('USDTM'):
                        symbol = key.get('symbol').replace('USDTM', 'USDT')
                        цена = float(key.get('price'))
                        await фильтрованный_словарь(data=data, exchange='kucoin', symbol=symbol, price=цена)
                return
        except Exception as e:
            print(f'Ошибка в kucoin в файле фильтра\n{e}')
            await asyncio.sleep(0.5)  

async def htx(data, session):
    for i in range(1, 7):
        try:
            # async with session.get(url="https://api.hbdm.com/linear-swap-ex/market/detail/batch_merged?contract_type=swap") as response:
            async with session.get(url="https://api.hbdm.com/linear-swap-ex/market/trade") as response:
                price = await response.json()
                for key in price['tick']['data']:
                    if key.get('contract_code').endswith('-USDT'):
                        symbol = key.get('contract_code').replace('-USDT', 'USDT')
                        цена = float(key.get('price'))
                        await фильтрованный_словарь(data=data, exchange='htx', symbol=symbol, price=цена)
                return
        except Exception as e:
            print(f'Ошибка в htx в файле фильтра\n{e}')
            await asyncio.sleep(0.5)  

async def lbank(data, session):
    for i in range(1, 7):
        try:
            async with session.get(url="https://lbkperp.lbank.com/cfd/openApi/v1/pub/marketData?productGroup=SwapU") as response:
                price = await response.json()
                for key in price['data']:
                    if key.get('symbol').endswith('USDT') and key.get('lastPrice') and key.get('instrumentStatus') != '1' and key.get('turnover') != '0' and key.get('volume') != '0':
                        symbol = key.get('symbol')                      
                        цена = float(key.get('lastPrice'))
                        await фильтрованный_словарь(data=data, exchange='lbank', symbol=symbol, price=цена)
                return
        except Exception as e:
            print(f'Ошибка в lbank в файле фильтра\n{e}')
            await asyncio.sleep(0.5)  
        
async def apishechka():
    data = {}
    
    
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(binance(data, session=session), bybit(data=data, session=session), bingx(data=data, session=session), mexc(data=data, session=session), bitget(data=data, session=session), gateio(data=data, session=session), okx(data=data, session=session), kucoin(data=data, session=session), htx(data=data, session=session), lbank(data=data, session=session))
    
    
    
    дата_минимум_2_биржи = {k: v for k, v in data.items() if len(v) >= 2}
    
    
    
    with open('filtr.txt', 'w', encoding='utf-8') as f:
        json.dump(дата_минимум_2_биржи, f, ensure_ascii=False, indent=4)
        
        
        
    #print(len(дата_минимум_2_биржи.keys()))
    
    
    
    return дата_минимум_2_биржи

# if __name__ == "__main__":
#     asyncio.run(apishechka())
