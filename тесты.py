from collections import defaultdict

async def арбитраж_повтор(мин_обьем, макс_обьем, шаг):
    for_delete = set()

    try:
        while True:
            for_send = set()
            await asyncio.sleep(0.2)  # УВЕЛИЧИЛ с 0.2 до 1 секунды
            
            if not orderbook.keys():
                await asyncio.sleep(1)
                continue
            else:
                #print('до лока')
                async with lock:
                    #print('вошли в лок')
                    #data = {k: v.copy() for k, v in orderbook.items()}
                    data = copy.deepcopy(orderbook)
                    #print('вышли с лока')
            #print('начало')
            # Собираем ВСЕ возможности со всех символов
            все_возможности = defaultdict(list)
            
            for symbol, exchanges in data.items():
                словарь_с_ценами = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
                
                for exchange, types in exchanges.items():
                    for type, order_book in types.items():
                        asks = order_book.get("asks") or []
                        bids = order_book.get("bids") or []
                        best_ask = order_book['asks'][0][0]
                        best_bid = order_book['bids'][0][0]
                        if not asks or not bids:
                            continue

                        #funding_rate, funding_time = (0, "нет данных")
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
                                    if spred_without_fund >= 2: #or spread_total >= 2: 
                                        for_send.add(symbol)
                                        


                                        все_возможности[symbol].append({
                                            'slippage_for_long': exit_long,
                                            "ex_long": pos_buy["buy_avg"],
                                            'ex_long_exit': pos_buy['sell_avg'],
                                            "ex_long_id": pos_buy["exchange"],
                                            "ex_long_type": pos_buy["market_type"],
                                            "ex_short": pos_sell["sell_avg"],
                                            'ex_short_exit': pos_buy['buy_avg'],
                                            "ex_short_id": pos_sell["exchange"],
                                            "ex_short_type": pos_sell["market_type"],
                                            'slippage_for_short': exit_short,
                                            "fees": комиссии,
                                            "spread_total": spread_total,
                                            "spread_usdt": спред_юсдт,
                                            "funding_spread": funding_spread,
                                            "курсовой": курсовой,
                                            'курсовой_с_тоталом': spred_without_fund,
                                            "funding_long": pos_buy["funding"],
                                            "funding_long_time": pos_buy["funding_time"],
                                            "funding_short": pos_sell["funding"],
                                            "funding_short_time": pos_sell["funding_time"],
                                            "volume": volume,
                                            'total_slippage': total
                                        })
            delete = for_delete - for_send
            for_delete = for_send.copy()
            
            try:
                for symbol_for_send, data_for_send in все_возможности.items():
                    await send_message(symbol_for_send, data_for_send)
                for symbol_for_delete in delete:
                    await send_message(symbol_for_delete)
            except Exception as e:
                print(f'Ошибка в отправление/удаления сообщения в функции арбитража{e}')
            
            
            
    except Exception as e:
        print(f'Ошибка в функции арбитража {e}')
            
            
                    
