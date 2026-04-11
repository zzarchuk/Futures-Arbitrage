import asyncio
from collections import defaultdict
import copy
import time
import traceback
from exchanges.mexc.api_mexc import get_mexc_fundings
from webb import message_to_site
#from config.config import orderbook, lock
from config.config import state_arbitrage


async def valid_spread(пары, session):

    subscriptions_config = {}

    for symbol, exchanges in пары.items():
        min_exchange, min_market_type, min_price, funding, timer, all_data = min(
            (
                (exchange, market_type, all_data.get('price', 0), all_data.get('funding', 0), all_data.get('get_funding', 'spot'), all_data)
                for exchange, markets in exchanges.items()
                for market_type, all_data in markets.items()
            ),
            key=lambda x: x[2]
        )

        if min_price == 0:
            continue

        for exchange, types in exchanges.items():
            for market_type, data in types.items():

                if (min_exchange == exchange or 
                    (min_market_type, market_type) in [('spot', 'spot'), ('futures', 'spot')]):
                    continue

                spread = ((data.get('price') - min_price) / min_price * 100)
                spred_funding = data.get('funding', 0) - funding

                # if (spread >= 4 or spred_funding + spread >= 1) and (exchange == 'mexc' or (data.get('funding') and data.get('get_funding'))):
                #     if symbol not in subscriptions_config:
                #         subscriptions_config[symbol] = {}
                #     if exchange not in subscriptions_config[symbol]:
                #         subscriptions_config[symbol][exchange] = []

                #     if market_type not in subscriptions_config[symbol][exchange]:
                #         subscriptions_config[symbol][exchange].append(market_type)

                #     if min_exchange not in subscriptions_config[symbol]:
                #         subscriptions_config[symbol][min_exchange] = []
                #     if min_market_type not in subscriptions_config[symbol][min_exchange]:
                #         subscriptions_config[symbol][min_exchange].append(min_market_type)
                
                if (spread >= 2 or spred_funding + spread >= 2) and (exchange == 'mexc' or (data.get('funding') and data.get('get_funding'))):
                    if symbol not in subscriptions_config:
                        subscriptions_config[symbol] = {}
                    if exchange not in subscriptions_config[symbol]:
                        subscriptions_config[symbol][exchange] = {}

                    if market_type not in subscriptions_config[symbol][exchange]:
                        subscriptions_config[symbol][exchange][market_type] = data

                    if min_exchange not in subscriptions_config[symbol]:
                        subscriptions_config[symbol][min_exchange] = {}
                    if min_market_type not in subscriptions_config[symbol][min_exchange]:
                        subscriptions_config[symbol][min_exchange][min_market_type] = all_data
                        
                        
    data_with_mexc = await get_mexc_fundings(subscriptions_config, session)




    return data_with_mexc


async def арбитраж_повтор(мин_обьем, макс_обьем, шаг):
    for_delete = set()
    time_of_life = {}
    current_keys = set() 
    last_seen = {}




    try:
        while True:
            current_keys.clear()
            for_send = set()
            await asyncio.sleep(0.2)  # УВЕЛИЧИЛ с 0.2 до 1 секунды
            
            if not state_arbitrage.orderbook_arbitrage.keys():
                await asyncio.sleep(1)
                continue
            else:
                async with state_arbitrage.lock_arbitrage:

                    data = copy.deepcopy(state_arbitrage.orderbook_arbitrage)
            # Собираем ВСЕ возможности со всех символов
            все_возможности = defaultdict(list)
            current_time = time.time()
            
            for symbol, exchanges in data.items():
                словарь_с_ценами = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
                
                for exchange, types in exchanges.items():
                    for type, order_book in types.items():
                        asks = order_book.get("asks") or []
                        bids = order_book.get("bids") or []
                        # best_ask = order_book['asks'][0][0]
                        # best_bid = order_book['bids'][0][0]
                        best_ask = 0
                        best_bid = 0
                        if not asks or not bids:
                            continue


                        funding_rate = order_book.get('funding', 0)
                        funding_time = order_book.get('get_funding', 'нет данных')

                        for volume in range(мин_обьем, макс_обьем + 1, шаг):
                            remaining_money = volume
                            total_spent = 0.0
                            total_coins = 0.0

                            for price in asks:
                                if remaining_money <= 0:
                                    break
                                coins_can_buy = remaining_money / float(price[0])
                                actual_coins = min(float(price[1]), coins_can_buy)
                                total_spent += actual_coins * float(price[0])
                                total_coins += actual_coins
                                remaining_money -= actual_coins * float(price[0])

                            if total_coins == 0:
                                continue

                            buy_avg = total_spent / total_coins

                            remaining_coins = total_coins
                            total_revenue = 0.0
                            coins_sold = 0.0

                            for price in bids:
                                if remaining_coins <= 0:
                                    break
                                actual_coins = min(float(price[1]), remaining_coins)
                                total_revenue += actual_coins * float(price[0])
                                coins_sold += actual_coins
                                remaining_coins -= actual_coins

                            if coins_sold == 0:
                                continue

                            sell_avg = total_revenue / coins_sold
                            
                            if type == 'futures':
                                словарь_с_ценами[symbol][volume][exchange][type] = {
                                    'best_ask': best_ask,
                                    'best_bid': best_bid,
                                    "buy_avg": buy_avg,
                                    "sell_avg": sell_avg,
                                    "volume": volume,
                                    "fee_maker": 0.002,
                                    "fee_taker": 0.006,
                                    "funding": funding_rate,
                                    "get_funding": funding_time,
                                }
                            elif type == 'spot':
                                словарь_с_ценами[symbol][volume][exchange][type] = {
                                    'best_ask': best_ask,
                                    'best_bid': best_bid,
                                    "buy_avg": buy_avg,
                                    "sell_avg": sell_avg,
                                    "volume": volume,
                                    "fee_maker": 0.002,
                                    "fee_taker": 0.006,
                                }
                
                # Анализируем возможности для текущего символа
                if словарь_с_ценами:
                    
                    for symbol, volumes in словарь_с_ценами.items():
                        for volume, exchanges in volumes.items():
                            all_positions = []
                            for exchange, markets in exchanges.items():
                                for market_type, data in markets.items():
                                    funding = data.get("funding", 0) if market_type == "futures" else 0
                                    funding_time = data.get("get_funding") if market_type == "futures" else "нет данных"
                                    all_positions.append({
                                        "exchange": exchange,
                                        "market_type": market_type,
                                        "buy_avg": data["buy_avg"],
                                        "sell_avg": data["sell_avg"],
                                        "fee_maker": data["fee_maker"],
                                        "fee_taker": data["fee_taker"],
                                        'best_ask': data['best_ask'],
                                        'best_bid': data['best_bid'],
                                        "funding": funding,
                                        "funding_time": funding_time,
                                    })

                            for i, pos_sell in enumerate(all_positions):
                                for j, pos_buy in enumerate(all_positions):
                                    if i == j or ((pos_buy['market_type'], pos_sell['market_type']) in [('spot', 'spot'), ('futures', 'spot')]):
                                        continue
                                    

                                    exit_long = ((pos_buy["sell_avg"] - pos_buy["buy_avg"]) / pos_buy["buy_avg"]) * 100
                                    exit_short = ((pos_sell["sell_avg"] - pos_sell["buy_avg"]) / pos_sell["sell_avg"]) * 100
                                    
                                    if exit_long > 0 or exit_short > 0:
                                        print('sdf')
                                    total = exit_long + exit_short
                                    #total = 0



                                    комиссии = pos_sell["fee_taker"] + pos_buy["fee_taker"]
                                    funding_spread = pos_sell["funding"] - pos_buy["funding"]
                                    
                                    
                                    курсовой = ((pos_sell["sell_avg"] - pos_buy["buy_avg"]) / pos_buy["buy_avg"]) * 100

                                            
                                    spread_total = курсовой - комиссии + funding_spread + total
                                    spred_without_fund = курсовой - комиссии + total
                                    спред_юсдт = ((volume * 2) / 100) * spread_total
                                
                                    #if spread_total >= 9:
                                    if spred_without_fund >= 6: #or spread_total >= 2: 
                                        
                                        for_send.add(symbol)
                                        
                                        key = f'{symbol}_{pos_buy["exchange"]}_{pos_buy["market_type"]}_{pos_sell["exchange"]}_{pos_sell["market_type"]}_{volume}'
                                        
                                        current_keys.add(key)
                                        
                                        # Если видим первый раз - запоминаем время начала
                                        if key not in time_of_life:
                                            time_of_life[key] = current_time
                                        
                                        # Обновляем время последнего обнаружения
                                        last_seen[key] = current_time
                                        
                                        # Вычисляем время жизни (сколько секунд существует)
                                        время_жизни = current_time - time_of_life[key]


                                        все_возможности[symbol].append({
                                            'slippage_for_long': exit_long, 
                                            "long_price": round(pos_buy["buy_avg"], 2), #
                                            'ex_long_exit': pos_buy['sell_avg'],
                                            "exchange_long": pos_buy["exchange"], #
                                            "long_type": pos_buy["market_type"], #
                                            "short_price": round(pos_sell["sell_avg"], 2),#
                                            'ex_short_exit': pos_buy['buy_avg'],
                                            "exchange_short": pos_sell["exchange"],#
                                            "short_type": pos_sell["market_type"],#
                                            'slippage_for_short': exit_short,
                                            "fees": комиссии,
                                            "spread_total": spread_total,
                                            "spread_usdt": спред_юсдт,
                                            "funding_spread": round(funding_spread, 2),#
                                            "курсовой": round(курсовой, 2),#
                                            'курсовой_с_тоталом': round(spred_without_fund, 2),#
                                            "funding_long": pos_buy["funding"],
                                            "funding_long_time": pos_buy["funding_time"],
                                            "funding_short": pos_sell["funding"],
                                            "funding_short_time": pos_sell["funding_time"],
                                            "volume": round((volume / pos_buy["buy_avg"]), 4),#
                                            'total_slippage': total,
                                            'lifetime': round(время_жизни)#
                                        })
            #print(set(все_возможности.keys()))
            keys_to_delete = set(time_of_life.keys()) - current_keys
            for key in keys_to_delete:
                del time_of_life[key]
                if key in last_seen:
                    del last_seen[key]
                    
                    
            delete = for_delete - for_send
            for_delete = for_send.copy()
            
            try:
                for symbol_for_send, data_for_send in все_возможности.items():
                    await message_to_site(symbol_for_send, data_for_send)
                for symbol_for_delete in delete:
                    await message_to_site(symbol_for_delete)
            except Exception as e:
                print(f'Ошибка в отправление/удаления сообщения в функции арбитража{e}')
            
            
            
    except Exception as e:
        print(f'Ошибка в функции арбитража {e}')
        traceback.print_exc()