#from config.config import orderbook, lock
from config.config import state_arbitrage
import asyncio
from collections import defaultdict
import copy
from typing import Dict


def filter_two_exchanges(
    data: Dict,
    symbol: str,
    exchanges_with_type: Dict[str, str]  # пример: {"bingx": "futures", "bybit": "spot"}
) -> Dict:
    """
    Возвращает словарь с конкретным символом и двумя биржами с указанным типом рынка.
    Если какой-то биржи нет или нет нужного рынка — возвращается пустой словарь.
    """
    if symbol not in data:
        return {}

    filtered_exchanges = {}
    for exchange, market_type in exchanges_with_type.items():
        if exchange in data[symbol] and market_type in data[symbol][exchange]:
            filtered_exchanges[exchange] = {market_type: data[symbol][exchange][market_type]}

    if len(filtered_exchanges) == len(exchanges_with_type):
        return {symbol: filtered_exchanges}
    return {}

async def схождения(exchange_long, exchange_short, symbol_param, volume, long_price, short_price, spread):
    from webb import update_spread # type: ignore
    id_msg = None
    try:
        while True:
            print('=== Начало итерации ===')
            await asyncio.sleep(2)
            
            if not state_arbitrage.orderbook_arbitrage.keys():
                print('Orderbook пустой, ждем...')
                await asyncio.sleep(1)
                continue
            else:
                async with state_arbitrage.lock_arbitrage:
                    data = copy.deepcopy(state_arbitrage.orderbook_arbitrage)
                    exs = exchange_long | exchange_short
                    print(f'Ищем символ: {symbol_param}')
                    print(f'Биржи для поиска: {exs}')
                    print(f'Доступные символы в orderbook: {list(data.keys())}')
                    
                    data = filter_two_exchanges(data, symbol_param, exs)
                    print(f'После фильтрации: {data}')
                    
                    if not data:
                        print('⚠️ filter_two_exchanges вернул пустой словарь!')
                        continue

            for symbol, exchanges in data.items():
                print(f'Обрабатываем символ: {symbol}')
                словарь_с_ценами = defaultdict(lambda: defaultdict(dict))
                
                for exchange, types in exchanges.items():
                    print(f'  Биржа: {exchange}, типы: {list(types.keys())}')
                    for type, order_book in types.items():
                        asks = order_book.get("asks") or []
                        bids = order_book.get("bids") or []
                        if not asks or not bids:
                            print(f'    ⚠️ Нет asks или bids для {exchange}/{type}')
                            continue

                        funding_rate = order_book.get('funding', 0)
                        funding_time = order_book.get('get_funding', 'нет данных')

                        # Покупка
                        remaining_coins_to_buy = volume
                        total_spent = 0.0

                        for price in asks:
                            if remaining_coins_to_buy <= 0:
                                break
                            actual_coins = min(float(price[1]), remaining_coins_to_buy)
                            total_spent += actual_coins * float(price[0])
                            remaining_coins_to_buy -= actual_coins

                        if remaining_coins_to_buy > 0:
                            print(f'    ⚠️ Недостаточно объема в asks для {exchange}/{type}')
                            continue

                        buy_avg = total_spent / volume

                        # Продажа
                        remaining_coins_to_sell = volume
                        total_revenue = 0.0

                        for price in bids:
                            if remaining_coins_to_sell <= 0:
                                break
                            actual_coins = min(float(price[1]), remaining_coins_to_sell)
                            total_revenue += actual_coins * float(price[0])
                            remaining_coins_to_sell -= actual_coins

                        if remaining_coins_to_sell > 0:
                            print(f'    ⚠️ Недостаточно объема в bids для {exchange}/{type}')
                            continue

                        sell_avg = total_revenue / volume
                        
                        print(f'    ✓ {exchange}/{type}: buy={buy_avg:.4f}, sell={sell_avg:.4f}')
                            
                        if type == 'futures':
                            словарь_с_ценами[symbol][exchange][type] = {
                                "buy_avg": buy_avg,
                                "sell_avg": sell_avg,
                                "volume": volume,
                                "fee_maker": 0.002,
                                "fee_taker": 0.006,
                                "funding": funding_rate,
                                "get_funding": funding_time,
                            }
                        elif type == 'spot':
                            словарь_с_ценами[symbol][exchange][type] = {
                                "buy_avg": buy_avg,
                                "sell_avg": sell_avg,
                                "volume": volume,
                                "fee_maker": 0.002,
                                "fee_taker": 0.006,
                            }

            print(f'Словарь с ценами: {dict(словарь_с_ценами)}')

            for symbol, exchanges in словарь_с_ценами.items():
                позиции = []

                for exchange, types in exchanges.items():
                    for market_type, data_item in types.items():

                        # если это биржа для продажи (long)
                        if next(iter(exchange_long)) == exchange:
                            позиции.append({
                                'side': 'sell',
                                'ex_for_sell': exchange,
                                'price_sell': data_item.get('sell_avg'),
                                'funding_sell': data_item.get('funding', 0),
                                'get_funding_sell': data_item.get('get_funding', 0),
                                'type_sell': market_type,
                                'fee_sell': data_item.get('fee_taker')
                            })
                        else:
                            позиции.append({
                                'side': 'buy',
                                'ex_for_buy': exchange,
                                'price_buy': data_item.get('buy_avg'),
                                'funding_buy': data_item.get('funding', 0),
                                'get_funding_buy': data_item.get('get_funding', 0),
                                'type_buy': market_type,
                                'fee_buy': data_item.get('fee_taker')
                            })

                print(f'Позиции: {позиции}')

                sell_data = None
                buy_data = None

                for p in позиции:
                    if p['side'] == 'sell':
                        sell_data = p
                    elif p['side'] == 'buy':
                        buy_data = p

                if not sell_data or not buy_data:
                    print('⚠️ Нет пары sell/buy')
                    continue

                # цены выхода
                exit_long_price = sell_data['price_sell']
                exit_short_price = buy_data['price_buy']

                # комиссии
                fee_sell = sell_data.get('fee_sell', 0)
                fee_buy = buy_data.get('fee_buy', 0)
                funding_spread = sell_data.get('funding_sell', 0) - buy_data.get('funding_buy', 0)
                
                if sell_data['type_sell'] == 'spot':
                    pnl_long = (exit_long_price - long_price) * volume
                elif sell_data['type_sell'] == 'futures':
                    pnl_long = (exit_long_price - long_price) * volume

                if buy_data['type_buy'] == 'spot':
                    pnl_short = (short_price - exit_short_price) * volume
                elif buy_data['type_buy'] == 'futures':
                    pnl_short = (short_price - exit_short_price) * volume
                
                total_pnl = pnl_long + pnl_short # type: ignore

                # комиссии
                fee_cost = ((exit_long_price * fee_sell) + (exit_short_price * fee_buy)) * volume

                # чистый PNL
                net_pnl = total_pnl - fee_cost + funding_spread
                
                pnl_percent_all = (net_pnl / ((long_price + short_price) * volume)) * 100
                now = spread + pnl_percent_all
                
                msg = (
                    f'Схождения {symbol}\n\n'
                    f'Закрываем лонг на бирже {sell_data["ex_for_sell"]} по цене {sell_data["price_sell"]:.4f}\n'
                    f'PNL: {pnl_long:.2f} USDT\n\n'
                    f'Закрываем шорт на бирже {buy_data["ex_for_buy"]} по цене {buy_data["price_buy"]:.4f}\n'
                    f'PNL: {pnl_short:.2f} USDT\n\n'
                    #f'Чистый PNL с учетом фандингов {net_pnl:.2f} USDT / {pnl_percent_all:.2f}%\n'
                    f'Изначальный спред при входе {spread:.2f}% сейчас {now:.2f}%'
                )
                
                print(f'📨 Отправляем сообщение:\n{msg}\n')
                
                if id_msg == None:
                    id_msg = await update_spread(msg)
                    print(f'✓ Создано сообщение с ID: {id_msg}')
                else:
                    await update_spread(msg, id_msg)
                    print(f'✓ Обновлено сообщение ID: {id_msg}')
                    
    except Exception as e:
        print(f'❌ Ошибка схождения: {e}')
        import traceback
        traceback.print_exc()
                    
                    # курсовой = ((all_data["sell_avg"] - all_data["buy_avg"]) /all_data["buy_avg"]) * 100
                    # spread_total = курсовой - комиссии + funding_spread
                    # спред_usdt = volume * spread_total / 100  # если нужно в USDT

    #                 if spread_total >= 0:
    #                     все_возможности.append({
    #                         "symbol": symbol,
    #                         "ex_long": pos_buy["buy_avg"],
    #                         "ex_long_id": pos_buy["exchange"],
    #                         "ex_long_type": pos_buy["market_type"],
    #                         "ex_short": pos_sell["sell_avg"],
    #                         "ex_short_id": pos_sell["exchange"],
    #                         "ex_short_type": pos_sell["market_type"],
    #                         "fees": комиссии,
    #                         "spread_total": spread_total,
    #                         "spread_usdt": спред_usdt,
    #                         "funding_spread": funding_spread,
    #                         "курсовой": курсовой,
    #                         "funding_long": pos_buy.get("funding", 0),
    #                         "funding_long_time": pos_buy.get("get_funding", "нет данных"),
    #                         "funding_short": pos_sell.get("funding", 0),
    #                         "funding_short_time": pos_sell.get("get_funding", "нет данных"),
    #                         "volume": volume
    #                     })

            
    #         # Обрабатываем все возможности ОДИН РАЗ за итерацию
            
    #         current_keys = set()
            
    #         for воз in все_возможности:
    #             key = f"{воз['symbol']}_{воз['ex_long_id']}_{воз['ex_long_type']}_{воз['ex_short_id']}_{воз['ex_short_type']}_{воз['volume']}"
    #             current_keys.add(key)
    #             монеты = воз['volume'] / воз['ex_long']
                
    #             now = time.time()
                
    #             # Формируем сообщение
    #             if воз.get('ex_long_type') == 'futures' and воз.get('ex_short_type') == 'futures':
    #                 msg = (
    #                     f"Валютная пара: {воз['symbol']}\n\n"
    #                     f"Лонг {воз['ex_long_id']} ({воз['ex_long_type']}) {воз['volume']} USDT {монеты:.4f}\n"
    #                     f"По цене: {воз['ex_long']:.6f}\n"
    #                     f"Фандинг: {воз['funding_long']:.2f}% Время: {воз['funding_long_time']}\n\n"
    #                     f"Шорт {воз['ex_short_id']} ({воз['ex_short_type']}) {воз['volume']} USDT {монеты:.4f}\n"
    #                     f"По цене: {воз['ex_short']:.6f}\n"
    #                     f"Фандинг: {воз['funding_short']:.2f}% Время: {воз['funding_short_time']}\n"
    #                     f"Общий спред: {воз['spread_total']:.2f}% / {воз['spread_usdt']:.2f}$ "
    #                     f"Курсовой: {воз.get('курсовой'):.2f}% / {(воз.get('volume') * 2) / 100 * воз.get('курсовой'):.2f}$ "
    #                     f"Фандинговый: {воз.get('funding_spread'):.2f}% / {(воз.get('volume') * 2) / 100 * воз.get('funding_spread'):.2f}$\n"

    #                 )
    #             else:
    #                 msg = (
    #                     f"Валютная пара: {воз['symbol']}\n\n"
    #                     f"Лонг {воз['ex_long_id']} ({воз['ex_long_type']}) {воз['volume']} USDT {монеты:.4f}\n"
    #                     f"По цене: {воз['ex_long']:.6f}\n\n"
    #                     f"Шорт {воз['ex_short_id']} ({воз['ex_short_type']}) {воз['volume']} USDT {монеты:.4f}\n"
    #                     f"По цене: {воз['ex_short']:.6f}\n"
    #                     f"Фандинг: {воз['funding_short']:.2f}% Время: {воз['funding_short_time']}\n"
    #                     f"Общий спред: {воз['spread_total']:.2f}% / {воз['spread_usdt']:.2f}$ "
    #                     f"Курсовой: {воз.get('курсовой'):.2f}% / {(воз.get('volume') * 2) / 100 * воз.get('курсовой'):.2f}$ "
    #                     f"Фандинговый: {воз.get('funding_spread'):.2f}% / {(воз.get('volume') * 2) / 100 * воз.get('funding_spread'):.2f}$\n"
    #                 )
                
    #             try:
    #                 # Если сообщение существует И прошло достаточно времени - обновляем
    #                 if key in id_map:
    #                     if now - last_update_time.get(key, 0) >= update_interval:
    #                         await send_message_to_site(
    #                             msg, 
    #                             long_price=воз['ex_long'],
    #                             short_price=воз['ex_short'],
    #                             spread=воз['курсовой'],
    #                             message_id=id_map[key],
    #                             exchange_long=воз['ex_long_id'],
    #                             exchange_short=воз['ex_short_id'],
    #                             symbol=воз['symbol'],
    #                             volume=монеты,
    #                             exchange_short_type=воз['ex_short_type'],
    #                             exchange_long_type=воз['ex_long_type']
    #                         )
    #                         last_update_time[key] = now
    #                 else:
    #                     # Создаём новое сообщение
    #                     new_id = await send_message_to_site(
    #                         msg,
    #                         long_price=воз['ex_long'],
    #                         short_price=воз['ex_short'],
    #                         spread=воз['курсовой'],
    #                         exchange_long=воз['ex_long_id'],
    #                         exchange_short=воз['ex_short_id'],
    #                         symbol=воз['symbol'],
    #                         volume=монеты,
    #                         exchange_short_type=воз['ex_short_type'],
    #                         exchange_long_type=воз['ex_long_type']
    #                     )
    #                     id_map[key] = new_id
    #                     last_update_time[key] = now
                        
    #             except Exception as e:
    #                 print(f"Ошибка в функции арбитража: {e}")
            
    #         # Удаляем исчезнувшие возможности
    #         keys_to_remove = set(id_map.keys()) - current_keys
    #         for key in keys_to_remove:
    #             try:
    #                 await delete_message_from_site(message_id=id_map[key])
    #                 del id_map[key]
    #                 if key in last_update_time:
    #                     del last_update_time[key]
    #             except Exception as e:
    #                 print(f"Ошибка при удалении сообщения: {e}")
                    
    # finally:
    #     # Очистка при завершении
    #     for msg_id in id_map.values():
    #         try:
    #             await delete_message_from_site(message_id=msg_id)
    #         except:
    #             pass
    