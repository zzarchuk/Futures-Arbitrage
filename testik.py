from collections import defaultdict
import requests


словарь_с_ценами = {
    "BTC/USDT": {
        1000: {
            "binance": {
                "buy_avg": 67450.23,
                "sell_avg": 67520.12,
                "volume": 1000,
                "fee_maker": 0.0005,
                "fee_taker": 0.0007,
                "funding": 0.0003,
                "get_funding": -0.0002
            },
            "bybit": {
                "buy_avg": 67470.55,
                "sell_avg": 67540.60,
                "volume": 1000,
                "fee_maker": 0.0004,
                "fee_taker": 0.0006,
                "funding": 0.0001,
                "get_funding": -0.0001
            }
        },
        2000: {
            "binance": {
                "buy_avg": 67460.00,
                "sell_avg": 67530.00,
                "volume": 2000,
                "fee_maker": 0.0005,
                "fee_taker": 0.0007,
                "funding": 0.00032,
                "get_funding": -0.00018
            },
            "bybit": {
                "buy_avg": 67480.10,
                "sell_avg": 67550.20,
                "volume": 2000,
                "fee_maker": 0.0004,
                "fee_taker": 0.0006,
                "funding": 0.00012,
                "get_funding": -0.00009
            }
        }
    },
    "ETH/USDT": {
        500: {
            "binance": {
                "buy_avg": 2543.80,
                "sell_avg": 2551.20,
                "volume": 500,
                "fee_maker": 0.0005,
                "fee_taker": 0.0007,
                "funding": 0.0002,
                "get_funding": -0.0001
            },
            "bybit": {
                "buy_avg": 2545.60,
                "sell_avg": 2553.70,
                "volume": 500,
                "fee_maker": 0.00045,
                "fee_taker": 0.00065,
                "funding": 0.00025,
                "get_funding": 0.0
            }
        },
        1000: {
            "binance": {
                "buy_avg": 254.00,
                "sell_avg": 2552.50,
                "volume": 1000,
                "fee_maker": 0.0005,
                "fee_taker": 0.0007,
                "funding": 0.00021,
                "get_funding": -0.00011
            },
            "bybit": {
                "buy_avg": 2500.30,
                "sell_avg": 2554.10,
                "volume": 1000,
                "fee_maker": 0.00045,
                "fee_taker": 0.00065,
                "funding": 0.00028,
                "get_funding": 0.00002
            }
        }
    }
}

# возможности = []
# for symbol, volumes in словарь_с_ценами.items():
#     #print(f"\n🔹 {symbol}")
#     for volume, exchanges in volumes.items():
#         # ищем биржу с минимальной buy_avg
#         min_exchange = min(exchanges.items(), key=lambda x: x[1]['buy_avg'])
#         мин_биржа, мин_данные = min_exchange
#         мин_цена = мин_данные.get('buy_avg')
#         мин_тейкер = мин_данные.get('fee_taker')
#         мин_мейкер = мин_данные.get('fee_maker')
#         мин_фандинг = мин_данные.get('funding')
#         мин_время_к_фандингy = мин_данные.get('get_funding')
        
        
#         for биржа, дата in exchanges.items():
#             if мин_биржа == биржа:
#                 continue
#             цена = дата.get('sell_avg')
#             тейкер = дата.get('fee_taker')
#             мейкер = дата.get('fee_maker')
#             фандинг = дата.get('funding')
#             время_к_фандингy = дата.get('get_funding')
            
#             комиссии = тейкер + мин_тейкер
#             спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии - фандинг - мин_фандинг
#             спред_юсдт = (volume / 100) * спред_проценты
#             if спред_юсдт >= 100:
#                 print(
#                     f"📊 Арбитражная возможность для {symbol}:\n"
#                     f"💰 Лонг: {мин_цена} на {мин_биржа} "
#                     f"(фандинг: {мин_фандинг}%, время до начисления: {мин_время_к_фандингy})\n"
#                     f"💸 Шорт: {цена} на {биржа} "
#                     f"(фандинг: {фандинг}%, время до начисления: {время_к_фандингy})\n"
#                     f"📈 Объём: {volume} USDT\n"
#                     f"⚖️ Спред: {спред_проценты:.4f}% | ~{спред_юсдт:.2f} USDT\n"
#                     f"💵 Комиссии суммарно: {комиссии:.4f}%\n"
#                     "────────────────────────────"
#                 )
#                 возможности.append({
#                     'symbol': symbol,
#                     "ex_long": мин_цена,
#                     "ex_long_id": мин_биржа,
#                     'ex_short': цена,
#                     'ex_short_id': биржа,
#                     'fees': комиссии,
#                     'spread': спред_проценты,
#                     'spread_usdt': спред_юсдт,
#                     'funding_long': мин_фандинг,
#                     'funding_long_time': мин_время_к_фандингy,
#                     'funding_short': фандинг,
#                     'funding_short_time': время_к_фандингy,
#                     'volume': volume,
#                 })
            #print(f'Цена  покупки: {мин_цена}\nНа бирже: {мин_биржа}\nЦена продажи: {цена}\nНа бирже: {биржа}\nСпред: {спред_проценты}% / {спред_юсдт} USDT\nОбьем: {volume}\n\n\n')


resp = requests.get("https://contract.mexc.com/api/v1/contract/detail").json()

for k in resp['data']:
    if k.get('symbol') == 'FLOCK_USDT':
        print(k)