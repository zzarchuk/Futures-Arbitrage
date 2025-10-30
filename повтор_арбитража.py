from filtr import api
import asyncio
import aiohttp
from aiogram import Bot, Dispatcher
from orderbook import binance, bybit, bingx, bitget, htx, kucoin, okx, gate, mexc
from collections import defaultdict
import time




async def арбитраж_повтор(пары, биржи, мин_обьем, макс_обьем, шаг, session, функция_ордербук, чат_айди, bot, черный_список, работающие_арбитражи):
    global работающие_арбитражи
    айдишники = None
    пустых_итераций = 0
    msg_for_edit = None
    start_time = time.time()
    while True:
        все_биржи_на_которых_есть_пара = defaultdict(list)
        for symbol, exchanges in пары.items():
            # print(f'Монета: {symbol}')
            словарь_с_ценами = defaultdict(lambda: defaultdict(dict))
            for exhange, data in exchanges.items():
                for conn in биржи.values():
                    if exhange == conn:
                        try:
                            order_book = await функция_ордербук(
                                conn, symbol, sessions=session
                            )
                            if order_book:
                                все_биржи_на_которых_есть_пара[symbol].append(
                                    conn
                                )
                            for volume in range(мин_обьем, макс_обьем + 1, шаг):
                                # ==== VWAP покупка ====
                                asks = order_book.get("asks") or []
                                bids = order_book.get("bids") or []
                                if not asks or not bids:
                                    continue

                                remaining_money = (
                                    volume  # сколько USDT хотим потратить
                                )
                                total_spent = 0.0
                                total_coins = 0.0

                                for price in asks:
                                    if remaining_money <= 0:
                                        break
                                    coins_can_buy = remaining_money / float(
                                        price[0]
                                    )
                                    actual_coins = min(
                                        float(price[1]), coins_can_buy
                                    )
                                    total_spent += actual_coins * float(
                                        price[0]
                                    )
                                    total_coins += actual_coins
                                    remaining_money -= actual_coins * float(
                                        price[0]
                                    )

                                # если ничего не купили — пропускаем
                                if total_coins == 0:
                                    continue

                                buy_avg = (
                                    total_spent / total_coins
                                )  # средняя цена покупки

                                # ==== VWAP продажа ====
                                remaining_coins = (
                                    total_coins  # продаём то, что купили
                                )
                                total_revenue = 0.0
                                coins_sold = 0.0

                                for price in bids:
                                    if remaining_coins <= 0:
                                        break
                                    actual_coins = min(
                                        float(price[1]), remaining_coins
                                    )
                                    total_revenue += actual_coins * float(
                                        price[0]
                                    )
                                    coins_sold += actual_coins
                                    remaining_coins -= actual_coins

                                # если нечего продать — пропускаем
                                if coins_sold == 0:
                                    continue

                                sell_avg = (
                                    total_revenue / coins_sold
                                )  # средняя цена продажи

                                словарь_с_ценами[symbol][volume][conn] = {
                                    "buy_avg": buy_avg,
                                    "sell_avg": sell_avg,
                                    "volume": volume,
                                    "fee_maker": data.get("maker"),
                                    "fee_taker": data.get("taker"),
                                    "funding": data.get("funding"),
                                    "get_funding": data.get("get_funding"),
                                }

                        except Exception as e:
                            print(f"Ошибка тут: {e}")
                            
            
            if словарь_с_ценами:
                возможности = []
                #print(f"Прошла работа по монете: {symbol}")
                for symbol, volumes in словарь_с_ценами.items():
                    for volume, exchanges in volumes.items():
                        # ищем биржу с минимальной buy_avg
                        min_exchange = min(
                            exchanges.items(), key=lambda x: x[1]["buy_avg"]
                        )
                        мин_биржа, мин_данные = min_exchange
                        мин_цена = мин_данные.get("buy_avg")
                        мин_тейкер = мин_данные.get("fee_taker")
                        мин_мейкер = мин_данные.get("fee_maker")
                        мин_фандинг = мин_данные.get("funding")
                        мин_время_к_фандингy = мин_данные.get("get_funding")

                        for биржа, дата in exchanges.items():
                            if мин_биржа == биржа:
                                continue
                            цена = дата.get("sell_avg")
                            тейкер = дата.get("fee_taker")
                            мейкер = дата.get("fee_maker")
                            фандинг = дата.get("funding")
                            время_к_фандингy = дата.get("get_funding")

                            комиссии = тейкер + мин_тейкер
                            # фандинг = захожу в лонг = фандинг платят шортистам
                            # мин_фандинг = захожу в шорт = фандинг платять лонгистам

                            if фандинг >= 0 and мин_фандинг >= 0:
                                спред_проценты = (
                                    ((цена - мин_цена) / мин_цена * 100)
                                    - комиссии
                                    #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                                    - фандинг
                                    #- мин_фандинг
                                    #спред с учетом фандингов даже если основной спред это сам фандинг
                                    #- фандинг
                                    #+ мин_фандинг
                                )
                            if фандинг < 0 and мин_фандинг < 0:
                                спред_проценты = (
                                    ((цена - мин_цена) / мин_цена * 100)
                                    - комиссии
                                    #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                                    #+ фандинг
                                    + мин_фандинг
                                    #спред с учетом фандингов даже если основной спред это сам фандинг
                                    #- фандинг
                                    #+ мин_фандинг
                                )
                            if фандинг < 0 and мин_фандинг > 0:
                                спред_проценты = (
                                    ((цена - мин_цена) / мин_цена * 100)
                                    - комиссии
                                    #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                                    #+ фандинг
                                    #- мин_фандинг
                                    #спред с учетом фандингов даже если основной спред это сам фандинг
                                    #- фандинг
                                    #+ мин_фандинг
                                )
                            if фандинг > 0 and мин_фандинг < 0:
                                спред_проценты = (
                                    ((цена - мин_цена) / мин_цена * 100)
                                    - комиссии
                                    #будет считаться спред отнимая любые фандинги чтобы спред был чисто курсовой и было похуй на фандинги
                                    - фандинг
                                    + мин_фандинг
                                    #спред с учетом фандингов даже если основной спред это сам фандинг
                                    #- фандинг
                                    #+ мин_фандинг
                                )
                            # спред_проценты = ((цена - мин_цена) / мин_цена * 100) - комиссии - (фандинг - мин_фандинг)
                            спред_юсдт = ((volume * 2) / 100) * спред_проценты
                            if спред_юсдт >= 3.5:
                                возможности.append(
                                    {
                                        "symbol": symbol,
                                        "ex_long": мин_цена,
                                        "ex_long_id": мин_биржа,
                                        "ex_short": цена,
                                        "ex_short_id": биржа,
                                        "fees": комиссии,
                                        "spread": спред_проценты,
                                        "spread_usdt": спред_юсдт,
                                        "funding_long": мин_фандинг,
                                        "funding_long_time": мин_время_к_фандингy,
                                        "funding_short": фандинг,
                                        "funding_short_time": время_к_фандингy,
                                        "volume": volume,
                                    }
                                )

            if возможности:  # Если спред найден
                пустых_итераций = 0  # сбрасываем счетчик

                for k in возможности:
                    if k.get("symbol") in черный_список:
                        работающие_арбитражи.remive(symbol)
                        return

                прошедшие_секунды = time.time() - start_time
                minutes = int(прошедшие_секунды // 60)
                sec = int(прошедшие_секунды % 60)
                время_жизни = f'{minutes} минут {sec} секунд'
                beast = max(возможности, key=lambda x: x["spread_usdt"])

                msg = (
                    f"Валютная пара: {beast.get('symbol')}\n\n"
                    f"Общий объем: {(beast.get('volume') * 2)} USDT\n\n"
                    f"Вход в сделку на {beast.get('volume')} USDT\n"
                    f"Лонг на: {beast.get('ex_long_id')}\nПо цене: {beast.get('ex_long'):.6f}\n"
                    f"Фандинг: {beast.get('funding_long'):.6f}%\nВремя: {beast.get('funding_long_time')}\n\n"
                    f"Возможности шорта:\n\n"
                )

                for data in возможности:
                    if data.get("volume") == beast.get("volume") and data.get(
                        "ex_long_id"
                    ) == beast.get("ex_long_id"):
                        msg += (
                            f"Вход в сделку на {beast.get('volume')} USDT\n"
                            f"Шорт на {data.get('ex_short_id')}\nПо цене: {data.get('ex_short'):.6f}\n"
                            f"Комиссии: {data.get('fees')}%\n"
                            f"Фандинг: {data.get('funding_short'):.6f}%\nВремя: {data.get('funding_short_time')}\n"
                            f"Спред: {data.get('spread'):.2f}% / {data.get('spread_usdt'):.2f}$ / {(data.get('volume') * 2)}$\n\n"
                        )

                try:
                    if айдишники is None:
                        #print(f'отправка в тг {symbol}')
                        айди_сообщения = await bot.send_message(
                            chat_id=чат_айди, text=msg
                        )
                        айдишники = айди_сообщения.message_id

                    else:
                        if minutes >= 90:
                            черный_список.append(symbol)
                            with open('черный_список.txt', 'a', encoding='utf-8') as w:
                                w.write(f'{symbol}, ')
                            await bot.edit_message_text(chat_id=чат_айди, message_id=айдишники, text=f'{msg}\n\nДобавилося в черный список')
                            работающие_арбитражи.remove(symbol)
                            return
                        else:
                            #print(f'редакт тг {symbol}')
                            await asyncio.sleep(2)
                            await bot.edit_message_text(
                                chat_id=чат_айди,
                                message_id=айдишники,
                                text=f"{msg}\n\nВремя жизни: {время_жизни}",
                            )
                            msg_for_edit = (
                                f"{msg}\n\nВремя жизни: {время_жизни}"
                            )

                except Exception as e:
                    await asyncio.sleep(23)
                    print(f"Ошибка в функции арбитража: {e}")
                continue

            else:
                пустых_итераций += 1
                
                if айдишники is None:
                    работающие_арбитражи.remove(symbol)
                    return
                
                if пустых_итераций >= 5:
                    try:
                        if msg_for_edit is not None:
                            #print(f'Редакт в тг что спред потерялся {symbol}')
                            await bot.edit_message_text(
                                chat_id=чат_айди,
                                message_id=айдишники,
                                text=f"{msg_for_edit}\n\n❌ Спред потерялся",
                            )
                            msg_for_edit = None
                        else:
                            #print(f'Удаление тг если спред хуйню прожил {symbol}')
                            await bot.delete_message(
                                chat_id=чат_айди, message_id=айдишники
                            )

                    except Exception as e:
                        print(f"Ошибка при обработке потери спреда: {e}")
                    работающие_арбитражи.remove(symbol)
                    return
                else:
                    #print(f'спред пока что пропал {symbol}  {пустых_итераций}') 
                    continue
