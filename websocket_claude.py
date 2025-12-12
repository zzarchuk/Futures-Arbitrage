import random
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "websocket_proto"))

import tracemalloc
from state import orderbook, lock
import asyncio
import aiohttp
import websockets
import json
import gzip
from collections import defaultdict
import time
from typing import Dict, Set, List
import copy
from binascii import crc32
from decimal import Decimal
import ctypes
#from dict_with_spread import арбитраж_бот
from filter_with_spot import mainn
from web import send_message_to_site, app, delete_message_from_site
import uvicorn

import PushDataV3ApiWrapper_pb2  # type: ignore
import PublicLimitDepthsV3Api_pb2  # type: ignore

tracemalloc.start()

async def monitor_memory():
    """Периодически проверяет использование памяти"""
    while True:
        await asyncio.sleep(10)  # Каждую минуту
        
        current, peak = tracemalloc.get_traced_memory()
        print(f"💾 Memory: {current / 1024 / 1024:.2f} MB | Peak: {peak / 1024 / 1024:.2f} MB")
        
        # Топ-10 потребителей памяти
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        
        print("[ Top 10 memory consumers ]")
        for stat in top_stats[:10]:
            print(stat)
            
            
#lock = asyncio.Lock()
# orderbook = defaultdict(
#     lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
# )


subscriptions_lock = asyncio.Lock()
#subscriptions_config = {'BTCUSDT': {'okx': ['spot', 'futures']}, 'ETHUSDT': {'okx': ['spot', 'futures']}, 'BNBUSDT': {'okx': ['spot', 'futures']}}
subscriptions_config = {}




class ChecksumMismatch(Exception):
    pass


class DynamicSubscriptionManager:
    """Менеджер для отслеживания изменений в конфигурации подписок"""

    def __init__(self):
        self.previous_config = {}
        self.change_event = asyncio.Event()

    async def monitor_changes(self, config_dict: dict):
        """Отслеживает изменения в словаре конфигурации"""
        while True:
            await asyncio.sleep(1)  # Проверяем каждую секунду

            async with subscriptions_lock:
                current = set(config_dict.keys())

            if current != set(self.previous_config.keys()):
                #print(f"🔄 Detected configuration change!")
                self.previous_config = copy.deepcopy(config_dict)
                self.change_event.set()  # Сигнал всем классам обновиться

    def get_symbols_for_exchange(self, exchange: str, market: str) -> Set[str]:
        """Возвращает список символов для конкретной биржи и рынка"""
        symbols = set()

        for coin, exchanges in self.previous_config.items():
            if exchange in exchanges:
                markets = exchanges[exchange]
                if market in markets:
                    symbols.add(coin)

        return symbols
    
    def find_data(self, ex, market, symbol):

        if market == 'futures':
            try:
                funding = self.previous_config[symbol][ex][market]['funding']
                get_funding = self.previous_config[symbol][ex][market]['get_funding']
                #max_vol = self.previous_config[symbol][ex][market]['max_vol']
                return funding, get_funding
            except Exception:
                return None, None
        else:
            try:
                max_vol = self.previous_config[symbol][ex][market]['max_vol']
                return max_vol
            except Exception:
                return None


class BaseDynamicWSClient:
    """Базовый класс для динамических WebSocket подключений"""

    def __init__(self, exchange_name: str, manager: DynamicSubscriptionManager):
        self.exchange_name = exchange_name
        self.manager = manager
        self.active_connections = (
            {}
        )  # {conn_id: {'ws': ws, 'symbols': set(), 'task': task}}
        self.is_running = True

    async def _reconnect_wrapper(self, coro_func, conn_id: str):
        """Wrapper с переподключениями"""
        retry_count = 0
        while self.is_running:
            try:
                await coro_func(conn_id)
                retry_count = 0
            except Exception as e:
                retry_count += 1
                wait_time = min(2**retry_count, 60)
                print(
                    f"❌ {self.exchange_name} conn#{conn_id} error: {e}. Reconnect in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)

    def stop(self):
        """Остановка всех соединений"""
        self.is_running = False


class BybitDynamicWS(BaseDynamicWSClient):
    """Bybit с динамическими подписками"""

    def __init__(self, manager: DynamicSubscriptionManager, market="spot", depth=50):
        super().__init__(f"Bybit-{market}", manager)
        self.market = market
        self.depth = depth
        self.base_url = {
            "spot": "wss://stream.bybit.com/v5/public/spot",
            "linear": "wss://stream.bybit.com/v5/public/linear",
        }[market]
        self.max_per_subscription = 10 if market == "spot" else 50

    async def _handle_connection(self, conn_id: str):
        """Соединение с динамическими подписками"""
        current_subscribed = set()

        async with websockets.connect(
            self.base_url, ping_interval=20, ping_timeout=60
        ) as ws:
            #print(f"✅ Bybit {self.market.upper()} conn#{conn_id} connected")

            receive_task = asyncio.create_task(self._process_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "bybit", self.market
                    )

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    # Подписываемся (батчами по max_per_subscription)
                    if to_subscribe:
                        subscribe_list = list(to_subscribe)
                        for i in range(
                            0, len(subscribe_list), self.max_per_subscription
                        ):
                            batch = subscribe_list[i : i + self.max_per_subscription]
                            args = [f"orderbook.{self.depth}.{s}" for s in batch]

                            try:
                                sub = {
                                    "op": "subscribe",
                                    "args": args,
                                    "req_id": str(time.time()),
                                }
                                await ws.send(json.dumps(sub))
                                current_subscribed.update(batch)
                                #print(
                                #    f"➕ Bybit {self.market.upper()} subscribed: {len(batch)} symbols"
                                #)
                                await asyncio.sleep(0.1)
                            except Exception as e:
                                print(f"❌ Bybit {self.market} subscribe error: {e}")

                    # Отписываемся
                    if to_unsubscribe:
                        unsubscribe_list = list(to_unsubscribe)
                        for i in range(
                            0, len(unsubscribe_list), self.max_per_subscription
                        ):
                            batch = unsubscribe_list[i : i + self.max_per_subscription]
                            args = [f"orderbook.{self.depth}.{s}" for s in batch]

                            try:
                                unsub = {
                                    "op": "unsubscribe",
                                    "args": args,
                                    "req_id": str(time.time()),
                                }
                                await ws.send(json.dumps(unsub))
                                current_subscribed.difference_update(batch)
                                #print(
                                #    f"➖ Bybit {self.market.upper()} unsubscribed: {len(batch)} symbols"
                                #)

                                # Чистим orderbook
                                #async with lock:
                                for symbol in batch:
                                    if symbol in orderbook:
                                        if "bybit" in orderbook[symbol]:
                                            if (
                                                self.market
                                                in orderbook[symbol]["bybit"]
                                            ):
                                                del orderbook[symbol]["bybit"][
                                                    self.market
                                                ]
                                await asyncio.sleep(0.1)
                            except Exception as e:
                                print(f"❌ Bybit {self.market} unsubscribe error: {e}")

                    try:
                        await asyncio.wait_for(
                            self.manager.change_event.wait(), timeout=5
                        )
                        self.manager.change_event.clear()
                    except asyncio.TimeoutError:
                        pass
            finally:
                receive_task.cancel()

    async def _process_messages(self, ws):
        """Обработка сообщений"""
        async for raw in ws:
            try:
                msg = json.loads(raw)

                # Heartbeat
                if msg.get("op") == "ping":
                    await ws.send(
                        json.dumps({"op": "pong", "req_id": msg.get("req_id")})
                    )
                    continue

                topic = msg.get("topic")
                tp = msg.get("type")
                data = msg.get("data")

                if not topic or not data:
                    continue

                symbol = topic.split(".")[-1]

                is_snapshot = tp == "snapshot" or data.get("u") == 1

                if is_snapshot:
                    bids = [[float(p), float(q)] for p, q in data.get("b", [])]
                    asks = [[float(p), float(q)] for p, q in data.get("a", [])]
                    if self.market == 'spot':
                        
                        orderbook[symbol]["bybit"][self.market]["bids"] = bids
                        orderbook[symbol]["bybit"][self.market]["asks"] = asks # type: ignore
                    else:
                        fund, get = self.manager.find_data('bybit', 'futures', symbol) # type: ignore
                        if fund != None and get != None:
                            orderbook[symbol]["bybit"][self.market]["bids"] = bids
                            orderbook[symbol]["bybit"][self.market]["asks"] = asks
                            #orderbook[symbol]["bybit"][self.market]["max_volume"] = vol
                            orderbook[symbol]["bybit"][self.market]["funding"] = fund # type: ignore
                            orderbook[symbol]["bybit"][self.market]["get_funding"] = get # type: ignore
                        else:
                            orderbook[symbol]["bybit"][self.market]["bids"] = bids
                            orderbook[symbol]["bybit"][self.market]["asks"] = asks

                else:
                    # Delta updates
                    #async with lock:
                    for side_key, side_name in [("b", "bids"), ("a", "asks")]:
                        for price_str, qty_str in data.get(side_key, []):
                            price = float(price_str)
                            qty = float(qty_str)

                            lst = orderbook[symbol]["bybit"][self.market][side_name]
                            idx = next(
                                (i for i, lv in enumerate(lst) if lv[0] == price),
                                None,
                            )

                            if qty == 0:
                                if idx is not None:
                                    lst.pop(idx)
                            else:
                                if idx is not None:
                                    lst[idx][1] = qty
                                else:
                                    lst.append([price, qty])

                    # Сортировка
                    if self.market == 'futures':
                        fund, get = self.manager.find_data('bybit', 'futures', symbol) # type: ignore
                        if fund != None and get != None:
                            orderbook[symbol]['bybit'][self.market if self.market == 'futures' else 'spot']['funding'] = fund
                            orderbook[symbol]['bybit'][self.market if self.market == 'futures' else 'spot']['get_funding'] = get
                    orderbook[symbol]["bybit"][
                        self.market if self.market == "spot" else "futures"
                    ]["bids"].sort(key=lambda x: -x[0])
                    orderbook[symbol]["bybit"][
                        self.market if self.market == "spot" else "futures"
                    ]["asks"].sort(key=lambda x: x[0])

            except Exception as e:
                print(f"❌ Bybit {self.market} parse error: {e}")

    async def run(self):
        await self._reconnect_wrapper(self._handle_connection, "main")

        #await asyncio.gather(*tasks)

class BinanceDynamicWS(BaseDynamicWSClient):
    """Binance с динамическими подписками (Spot + Futures)"""

    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("Binance", manager)
        self.spot_url_base = "wss://stream.binance.com:9443/stream?streams="
        self.futures_url_base = "wss://fstream.binance.com/stream?streams="

    async def _handle_spot_connection(self, conn_id: str):
        """Spot соединение с динамическими подписками"""
        current_subscribed = set()
        ws = None

        while self.is_running:
            try:
                # Получаем символы
                target_symbols = self.manager.get_symbols_for_exchange(
                    "binance", "spot"
                )
                if not target_symbols:
                    await asyncio.sleep(1)
                    continue

                # Строим URL комбинированного потока
                streams = "/".join(
                    [f"{s.lower()}@depth20@100ms" for s in target_symbols]
                )
                url = f"{self.spot_url_base}{streams}"

                async with websockets.connect(
                    url, ping_interval=60, ping_timeout=180
                ) as ws:
                    #print(
                    #    f"✅ Binance SPOT connected with {len(target_symbols)} symbols"
                    #)

                    async for raw_message in ws:
                        try:
                            msg = json.loads(raw_message)

                            # Пропускаем сообщения без данных
                            if "stream" not in msg or "data" not in msg:
                                continue

                            symbol = msg["stream"].split("@")[0].upper()
                            data = msg["data"]

                            asks = [[float(p), float(q)] for p, q in data["asks"]]
                            bids = [[float(p), float(q)] for p, q in data["bids"]]

                            #async with lock:
                            orderbook[symbol]["binance"]["spot"]["asks"] = asks
                            orderbook[symbol]["binance"]["spot"]["bids"] = bids

                        except Exception as e:
                            print(f"❌ Binance SPOT parse error: {e}")

            except Exception as e:
                print(f"❌ Binance SPOT connection error: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)

    async def _handle_futures_connection(self, conn_id: str):
        """Futures соединение (оставляем как есть)"""
        current_subscribed = set()

        while self.is_running:
            try:
                target_symbols = self.manager.get_symbols_for_exchange(
                    "binance", "futures"
                )
                if not target_symbols:
                    await asyncio.sleep(1)
                    continue

                streams = "/".join(
                    [f"{s.lower()}@depth20@100ms" for s in target_symbols]
                )
                url = f"{self.futures_url_base}{streams}"

                async with websockets.connect(
                    url, ping_interval=60, ping_timeout=180
                ) as ws:
                    #print(
                    #    f"✅ Binance FUTURES connected with {len(target_symbols)} symbols"
                    #)

                    async for raw_message in ws:
                        try:
                            msg = json.loads(raw_message)

                            if "stream" not in msg or "data" not in msg:
                                continue

                            symbol = msg["stream"].split("@")[0].upper()
                            data = msg["data"]

                            asks = [[float(p), float(q)] for p, q in data["a"]]
                            bids = [[float(p), float(q)] for p, q in data["b"]]
                            fund, get = self.manager.find_data('binance', 'futures', symbol) # type: ignore
                            if fund and get:
                            #async with lock:
                                orderbook[symbol]["binance"]["futures"]["asks"] = asks
                                orderbook[symbol]["binance"]["futures"]["bids"] = bids
                                orderbook[symbol]["binance"]["futures"]['funding'] = fund
                                orderbook[symbol]["binance"]["futures"]['get_funding'] = get
                                #orderbook[symbol]["mexc"]["futures"]['max_volume'] = vol
                            else:
                                orderbook[symbol]["binance"]["futures"]["asks"] = asks
                                orderbook[symbol]["binance"]["futures"]["bids"] = bids
                            #async with lock:
                            # orderbook[symbol]["binance"]["futures"]["asks"] = asks
                            # orderbook[symbol]["binance"]["futures"]["bids"] = bids

                        except Exception as e:
                            print(f"❌ Binance FUTURES parse error: {e}")

            except Exception as e:
                print(
                    f"❌ Binance FUTURES connection error: {e}. Reconnecting in 5s..."
                )
                await asyncio.sleep(5)

    async def run_spot(self):
        await self._reconnect_wrapper(self._handle_spot_connection, "spot")

    async def run_futures(self):
        await self._reconnect_wrapper(self._handle_futures_connection, "futures")



class BingxDynamicWS(BaseDynamicWSClient):
    """BingX с динамическими подписками"""

    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("BingX", manager)
        self.max_per_connection = 100
        self.ws = None
        self.ws_futures = None
        # Очереди для накопления отписок
        self.unsub = defaultdict(set)
        self.lock = asyncio.Lock()


    async def _batch_unsubscribe_worker(self):
        """Воркер для batch-отписок раз в 5 минут"""
        while self.is_running:
            #print('gkgkk')
            await asyncio.sleep(90)  # 5 минут
            #print(self.ws)
            
            async with self.lock:
                #print(self.unsub)
                queue = self.unsub.copy()
                self.unsub.clear()

            if queue:
                print(f"🗑️ BingX processing {len(queue)} unsubscribes")
                
                # Отправляем unsub для каждого символа
                for market, data in queue.items():
                    for symbol in data:
                        try:
                            if market == "spot":
                                unsub = {
                                    "id": f"{symbol}_spot",
                                    "reqType": "unsub",
                                    "dataType": f"{symbol.replace('USDT', '-USDT')}@depth50",
                                }
                                await self.ws.send(json.dumps(unsub))
                            else:
                                unsub = {
                                    "id": f"{symbol}_futures",
                                    "reqType": "unsub",
                                    "dataType": f"{symbol.replace('USDT', '-USDT')}@depth50@500ms",
                                }
                                await self.ws_futures.send(json.dumps(unsub))
                            
                            #print(f"➖ BingX batch unsubscribed: {symbol}")
                            await asyncio.sleep(0.05)  # Небольшая задержка между unsub
                            
                        except Exception as e:
                            print(f"❌ Batch bingx unsubscribe error {symbol}: {e}")
                
                # Очистка orderbook для накопленных символов
                #async with lock:
                for type, symbols in queue.items():
                    for symbol in symbols:
                        if symbol in orderbook:
                            if "bingx" in orderbook[symbol]:
                                if type in orderbook[symbol]["bingx"]:
                                    del orderbook[symbol]["bingx"][type]
                                    
                                    if not orderbook[symbol]['bingx']:
                                        del orderbook[symbol]['bingx']
                                        if not orderbook[symbol]:
                                            del orderbook[symbol]


    async def _handle_spot_connection(self, conn_id: str):
        """Одно spot соединение с динамическими подписками"""
        url = "wss://open-api-ws.bingx.com/market"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
            self.ws = ws
            #print(f"✅ BingX SPOT conn#{conn_id} connected")

            # Создаём задачу для обработки сообщений
            receive_task = asyncio.create_task(self._process_spot_messages(ws))

            try:
                while self.is_running:
                    # Получаем актуальный список символов
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "bingx", "spot"
                    )

                    # Определяем что нужно добавить/удалить
                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    # Подписываемся на новые
                    for symbol in to_subscribe:
                        try:
                            sub = {
                                "id": f"{symbol}_spot",
                                "reqType": "sub",
                                "dataType": f"{symbol.replace('USDT', '-USDT')}@depth50",
                            }
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            #print(f"➕ BingX SPOT subscribed: {symbol}")
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            print(f"❌ Subscribe error {symbol}: {e}")

                    # Отписываемся от удалённых
                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['spot'].add(symbol)
                        #print(f"📋 BingX SPOT queued for unsubscribe: {symbol}")



                    # Ждём следующего обновления или таймаут
                    try:
                        await asyncio.wait_for(
                            self.manager.change_event.wait(), timeout=5
                        )
                        self.manager.change_event.clear()
                    except asyncio.TimeoutError:
                        pass
            finally:
                receive_task.cancel()

    async def _process_spot_messages(self, ws):
        """Обработка входящих сообщений"""
        async for message in ws:
            try:
                data = gzip.decompress(message).decode("utf-8")

                if data in ("ping", "Ping"):
                    await ws.send("Pong")
                    continue

                orderboo = json.loads(data)

                if "data" not in orderboo or orderboo["data"] is None:
                    continue
                if "asks" not in orderboo["data"] or "bids" not in orderboo["data"]:
                    continue

                symbol = orderboo["dataType"].split("@")[0]
                asks = orderboo["data"]["asks"]
                bids = orderboo["data"]["bids"]

                #async with lock:
                base = symbol.replace("-USDT", "USDT")
                orderbook[base]["bingx"]["spot"]["asks"] = [[float(price), float(size)] for price, size in asks[::-1]]
                orderbook[base]["bingx"]["spot"]["bids"] = [[float(price), float(size)] for price, size in bids]

            except Exception as e:
                print(f"❌ BingX SPOT parse error: {e}")

    async def _handle_futures_connection(self, conn_id: str):
        """Одно futures соединение с динамическими подписками"""
        url = "wss://open-api-swap.bingx.com/swap-market"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
            self.ws_futures = ws
            #print(f"✅ BingX FUTURES conn#{conn_id} connected")

            receive_task = asyncio.create_task(self._process_futures_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "bingx", "futures"
                    )

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    for symbol in to_subscribe:
                        try:
                            sub = {
                                "id": f"{symbol}_futures",
                                "reqType": "sub",
                                "dataType": f"{symbol.replace('USDT', '-USDT')}@depth50@500ms",
                            }
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            #print(f"➕ BingX FUTURES subscribed: {symbol}")
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            print(f"❌ Subscribe error {symbol}: {e}")

                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['futures'].add(symbol)
                        #print(f"📋 BingX futures queued for unsubscribe: {symbol}")


                    try:
                        await asyncio.wait_for(
                            self.manager.change_event.wait(), timeout=5
                        )
                        self.manager.change_event.clear()
                    except asyncio.TimeoutError:
                        pass
            finally:
                receive_task.cancel()

    async def _process_futures_messages(self, ws):
        """Обработка futures сообщений"""
        async for message in ws:
            try:
                data = gzip.decompress(message).decode("utf-8")

                if data == "Ping":
                    await ws.send("Pong")
                    continue

                orderboo = json.loads(data)

                if "data" not in orderboo or orderboo["data"] is None:
                    continue
                if "asks" not in orderboo["data"] or "bids" not in orderboo["data"]:
                    continue

                symbol = orderboo["dataType"].split("@")[0]
                asks = orderboo["data"]["asks"]
                bids = orderboo["data"]["bids"]

                #async with lock:
                base = symbol.replace("-USDT", "USDT")
                fund, get = self.manager.find_data('bingx', 'futures', base) # type: ignore
                if fund != None and get != None:
                    orderbook[base]["bingx"]["futures"]["funding"] = fund
                    orderbook[base]["bingx"]["futures"]["get_funding"] = get
                    
                orderbook[base]["bingx"]["futures"]["asks"] = [[float(price), float(size)] for price, size in asks[::-1]]
                orderbook[base]["bingx"]["futures"]["bids"] = [[float(price), float(size)] for price, size in bids]

            except Exception as e:
                print(f"❌ BingX FUTURES parse error: {e}")

    async def run_spot(self):
        """Запуск spot соединения"""
        await self._reconnect_wrapper(self._handle_spot_connection, "spot_0")

    async def run_futures(self):
        """Запуск futures соединения"""
        await self._reconnect_wrapper(self._handle_futures_connection, "futures_0")



class MexcDynamicWS(BaseDynamicWSClient):
    """MEXC с динамическими подписками (лимит 30 на соединение!)"""

    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("MEXC", manager)
        self.max_per_connection = 30  # ЖЁСТКИЙ ЛИМИТ
        self.spot_unsubscribe_queue = set()
        self.futures_unsubscribe_queue = set()
        self.unsubscribe_lock = asyncio.Lock()
        self.ws = None
        self.ws_futures = None
        # Очереди для накопления отписок
        self.unsub = defaultdict(set)
        self.lock = asyncio.Lock()
        
    
    async def ping(self):
        while True:
            await asyncio.sleep(15)
            await self.ws.send(json.dumps({'method': 'ping'}))
            await self.ws_futures.send(json.dumps({'method': 'ping'}))
        
    async def _batch_unsubscribe_worker(self):
        """Воркер для batch-отписок раз в 5 минут"""
        while self.is_running:
            await asyncio.sleep(90)  # 5 минут
            
            async with self.lock:
                queue = self.unsub.copy()
                self.unsub.clear()

            if queue:
                print(f"🗑️ mexc processing {len(queue)} unsubscribes")
                
                # Отправляем unsub для каждого символа
                for market, data in queue.items():
                    for symbol in data:
                        try:
                            if market == "spot":
                                unsub = {
                                    "method": "UNSUBSCRIPTION",
                                    "params": [
                                        f"spot@public.limit.depth.v3.api.pb@{symbol}@20"
                                    ],
                                }
                                await self.ws.send(json.dumps(unsub))
                            else:
                                unsub = {
                                    "method": "unsub.depth.step",
                                    "param": {"symbol": symbol.replace('USDT','_USDT')},
                                }
                                await self.ws_futures.send(json.dumps(unsub))
                            
                            #print(f"➖ mexc batch unsubscribed: {symbol}")
                            await asyncio.sleep(0.05)  # Небольшая задержка между unsub
                            
                        except Exception as e:
                            print(f"❌ Batch mexc unsubscribe error {symbol}: {e}")
                
                # Очистка orderbook для накопленных символов
                #async with lock:
                for type, symbols in queue.items():
                    for symbol in symbols:
                        if symbol in orderbook:
                            if "mexc" in orderbook[symbol]:
                                if type in orderbook[symbol]["mexc"]:
                                    del orderbook[symbol]["mexc"][type]
                                    if not orderbook[symbol]['mexc']:
                                        del orderbook[symbol]['mexc']
                                        if not orderbook[symbol]:
                                            del orderbook[symbol]

    async def _handle_spot_connection(self, conn_id: str):
        """Spot соединение с динамическими подписками"""
        url = "wss://wbs-api.mexc.com/ws"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=10, ping_timeout=60) as ws:
            #print(f"✅ MEXC SPOT conn#{conn_id} connected")
            self.ws = ws

            receive_task = asyncio.create_task(self._process_spot_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "mexc", "spot"
                    )

                    # MEXC: лимит 30 подписок на соединение!
                    # Берём только первые 30 для этого соединения
                    target_symbols = set(list(target_symbols)[:30])

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    # Подписываемся
                    for symbol in to_subscribe:
                        try:
                            sub = {
                                "method": "SUBSCRIPTION",
                                "params": [
                                    f"spot@public.limit.depth.v3.api.pb@{symbol}@20"
                                ],
                            }
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            #print(f"➕ MEXC SPOT subscribed: {symbol}")
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            print(f"❌ MEXC SPOT subscribe error {symbol}: {e}")

                    # Отписываемся
                    # for symbol in to_unsubscribe:
                    #     try:
                    #         unsub = {
                    #             "method": "UNSUBSCRIPTION",
                    #             "params": [
                    #                 f"spot@public.limit.depth.v3.api.pb@{symbol}@20"
                    #             ],
                    #         }
                    #         await ws.send(json.dumps(unsub))
                    #         current_subscribed.remove(symbol)
                    #         print(f"➖ MEXC SPOT unsubscribed: {symbol}")

                    #         async with lock:
                    #             if symbol in orderbook:
                    #                 if "mexc" in orderbook[symbol]:
                    #                     if "spot" in orderbook[symbol]["mexc"]:
                    #                         del orderbook[symbol]["mexc"]["spot"]
                    #     except Exception as e:
                    #         print(f"❌ MEXC SPOT unsubscribe error {symbol}: {e}")
                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['spot'].add(symbol)
                        #print(f"📋 mexc SPOT queued for unsubscribe: {symbol}")


                    try:
                        await asyncio.wait_for(
                            self.manager.change_event.wait(), timeout=5
                        )
                        self.manager.change_event.clear()
                    except asyncio.TimeoutError:
                        pass
            finally:
                receive_task.cancel()

    async def _process_spot_messages(self, ws):
        """Обработка spot сообщений (protobuf)"""
        async for raw_message in ws:
            try:
                if isinstance(raw_message, str):
                    continue

                wrapper = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
                wrapper.ParseFromString(raw_message)

                if wrapper.HasField("publicLimitDepths"):
                    depth = wrapper.publicLimitDepths
                    asks = [[float(l.price), float(l.quantity)] for l in depth.asks]
                    bids = [[float(l.price), float(l.quantity)] for l in depth.bids]
                    symbol = wrapper.symbol
                    
                    vol = self.manager.find_data('mexc', 'spot', symbol)
                    if vol:

                    #async with lock:
                        orderbook[symbol]["mexc"]["spot"]["asks"] = asks
                        orderbook[symbol]["mexc"]["spot"]["bids"] = bids

                        orderbook[symbol]["mexc"]["spot"]['max_volume'] = vol # type: ignore
                    else:
                        orderbook[symbol]["mexc"]["spot"]["asks"] = asks
                        orderbook[symbol]["mexc"]["spot"]["bids"] = bids
            except Exception as e:
                print(f"❌ MEXC SPOT parse error: {e}")

    async def _handle_futures_connection(self, conn_id: str):
        """Futures соединение с динамическими подписками"""
        url = "wss://contract.mexc.com/edge"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=10, ping_timeout=60) as ws:
            #print(f"✅ MEXC FUTURES conn#{conn_id} connected")
            self.ws_futures = ws
            receive_task = asyncio.create_task(self._process_futures_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "mexc", "futures"
                    )

                    # Лимит 30 на соединение
                    target_symbols = set(list(target_symbols)[:30])

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    # Подписываемся
                    for symbol in to_subscribe:
                        try:
                            mexc_symbol = symbol.replace("USDT", "_USDT")
                            sub = {
                                "method": "sub.depth.step",
                                "param": {"symbol": mexc_symbol},
                            }
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            #print(f"➕ MEXC FUTURES subscribed: {symbol}")
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            print(f"❌ MEXC FUTURES subscribe error {symbol}: {e}")

                    # Отписываемся
                    # for symbol in to_unsubscribe:
                    #     try:
                    #         mexc_symbol = symbol.replace("USDT", "_USDT")
                    #         unsub = {
                    #             "method": "unsub.depth.step",
                    #             "param": {"symbol": mexc_symbol},
                    #         }
                    #         await ws.send(json.dumps(unsub))
                    #         current_subscribed.remove(symbol)
                    #         print(f"➖ MEXC FUTURES unsubscribed: {symbol}")

                    #         async with lock:
                    #             if symbol in orderbook:
                    #                 if "mexc" in orderbook[symbol]:
                    #                     if "futures" in orderbook[symbol]["mexc"]:
                    #                         del orderbook[symbol]["mexc"]["futures"]
                    #     except Exception as e:
                    #         print(f"❌ MEXC FUTURES unsubscribe error {symbol}: {e}")
                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['futures'].add(symbol)
                        #print(f"📋 mexc futures queued for unsubscribe: {symbol}")


                    try:
                        await asyncio.wait_for(
                            self.manager.change_event.wait(), timeout=5
                        )
                        self.manager.change_event.clear()
                    except asyncio.TimeoutError:
                        pass
            finally:
                receive_task.cancel()

    async def _process_futures_messages(self, ws):
        """Обработка futures сообщений"""
        async for raw_message in ws:
            try:
                msg = json.loads(raw_message)

                if "data" not in msg or not msg.get("symbol"):
                    continue

                symbol = msg["symbol"].replace("_USDT", "USDT")
                
                fund, get = self.manager.find_data('mexc', 'futures', symbol)
                if fund and get:
                #async with lock:
                    orderbook[symbol]["mexc"]["futures"]["asks"] = msg["data"]["asks"]
                    orderbook[symbol]["mexc"]["futures"]["bids"] = msg["data"]["bids"]
                    orderbook[symbol]["mexc"]["futures"]['funding'] = fund
                    orderbook[symbol]["mexc"]["futures"]['get_funding'] = get
                    #orderbook[symbol]["mexc"]["futures"]['max_volume'] = vol
                else:
                    orderbook[symbol]["mexc"]["futures"]["asks"] = msg["data"]["asks"]
                    orderbook[symbol]["mexc"]["futures"]["bids"] = msg["data"]["bids"]
            except Exception as e:
                print(f"❌ MEXC FUTURES parse error: {e}")

    async def run_spot(self):
        """Запуск spot соединений (может быть несколько!)"""
        target_symbols = self.manager.get_symbols_for_exchange("mexc", "spot")
        connections_needed = (len(target_symbols) + 29) // 30

        tasks = [
            asyncio.create_task(
                self._reconnect_wrapper(self._handle_spot_connection, f"spot_{i}")
            )
            for i in range(max(1, connections_needed))
        ]
        await asyncio.gather(*tasks)

    async def run_futures(self):
        """Запуск futures соединений (может быть несколько!)"""
        target_symbols = self.manager.get_symbols_for_exchange("mexc", "futures")
        connections_needed = (len(target_symbols) + 29) // 30

        tasks = [
            asyncio.create_task(
                self._reconnect_wrapper(self._handle_futures_connection, f"futures_{i}")
            )
            for i in range(max(1, connections_needed))
        ]
        
        


class BitgetDynamicWS(BaseDynamicWSClient):
    """MEXC с динамическими подписками (лимит 30 на соединение!)"""

    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("Bitget", manager)
        self.ws = None
        self.ws_futures = None
        # Очереди для накопления отписок
        self.unsub = defaultdict(set)
        self.lock = asyncio.Lock()
        
    async def send_ping(self):
        while True:
            try:

                await asyncio.sleep(30)  # каждые 30 секунд
                await self.ws.send("ping")  # текстовое сообщение
                await self.ws_futures.send("ping")
            except Exception:
                continue       
    async def _batch_unsubscribe_worker(self):
        """Воркер для batch-отписок раз в 5 минут"""
        while self.is_running:
            await asyncio.sleep(90)  # 5 минут
            
            async with self.lock:
                queue = self.unsub.copy()
                self.unsub.clear()

            if queue:
                print(f"🗑️ bitget processing {len(queue)} unsubscribes")
                
                # Отправляем unsub для каждого символа
                for market, data in queue.items():
                    for symbol in data:
                        try:
                            if market == "spot":
                                unsub = {
                                    "op": "unsubscribe",
                                    "args": [
                                        {
                                            "instType": "SPOT",
                                            "channel": "books15",
                                            "instId": symbol,
                                        }
                                    ],
                                }
                                await self.ws.send(json.dumps(unsub))
                            else:
                                unsub = {
                                    "op": "unsubscribe",
                                    "args": [
                                        {
                                            "instType": "USDT-FUTURES",
                                            "channel": "books15",
                                            "instId": symbol,
                                        }
                                    ],
                                }
                                await self.ws_futures.send(json.dumps(unsub))
                            
                            #print(f"➖ bitget batch unsubscribed: {symbol}")
                            await asyncio.sleep(0.05)  # Небольшая задержка между unsub
                            
                        except Exception as e:
                            print(f"❌ Batch bitget unsubscribe error {symbol}: {e}")
                
                # Очистка orderbook для накопленных символов
                #async with lock:
                for type, symbols in queue.items():
                    for symbol in symbols:
                        if symbol in orderbook:
                            if "bitget" in orderbook[symbol]:
                                if type in orderbook[symbol]["bitget"]:
                                    del orderbook[symbol]["bitget"][type]
                                    if not orderbook[symbol]['bitget']:
                                        del orderbook[symbol]["bitget"]
                                        if not orderbook[symbol]:
                                            del orderbook[symbol]


    async def _handle_futures_connection(self, conn_id: str):
        """Futures соединение с динамическими подписками"""
        url = "wss://ws.bitget.com/v2/ws/public"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
            #print(f"✅ bitget FUTURES conn#{conn_id} connected")
            self.ws_futures = ws
            receive_task = asyncio.create_task(self._process_futures_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "bitget", "futures"
                    )

                    # Лимит 30 на соединение
                    # target_symbols = set(list(target_symbols)[:30])

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    # Подписываемся
                    for symbol in to_subscribe:
                        try:

                            sub = {
                                "op": "subscribe",
                                "args": [
                                    {
                                        "instType": "USDT-FUTURES",
                                        "channel": "books15",
                                        "instId": symbol,
                                    }
                                ],
                            }
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            #print(f"➕ Bitget FUTURES subscribed: {symbol}")
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            print(f"❌ Bitget FUTURES subscribe error {symbol}: {e}")

                    # Отписываемся
                    # for symbol in to_unsubscribe:
                    #     try:

                    #         unsub = {
                    #             "op": "unsubscribe",
                    #             "args": [
                    #                 {
                    #                     "instType": "USDT-FUTURES",
                    #                     "channel": "books15",
                    #                     "instId": symbol,
                    #                 }
                    #             ],
                    #         }
                    #         await ws.send(json.dumps(unsub))
                    #         current_subscribed.remove(symbol)
                    #         print(f"➖ Bitget FUTURES unsubscribed: {symbol}")

                    #         async with lock:
                    #             if symbol in orderbook:
                    #                 if "bitget" in orderbook[symbol]:
                    #                     if "futures" in orderbook[symbol]["bitget"]:
                    #                         del orderbook[symbol]["bitget"]["futures"]
                    #     except Exception as e:
                    #         print(f"❌ Bitget FUTURES unsubscribe error {symbol}: {e}")
                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['futures'].add(symbol)
                        #print(f"📋 bitget futures queued for unsubscribe: {symbol}")

                    try:
                        await asyncio.wait_for(
                            self.manager.change_event.wait(), timeout=5
                        )
                        self.manager.change_event.clear()
                    except asyncio.TimeoutError:
                        pass
            finally:
                receive_task.cancel()

    async def _process_futures_messages(self, ws):
        """Обработка futures сообщений"""
        async for raw_message in ws:
            try:
                if raw_message == 'pong':
                    #print(raw_message)
                    continue
                
                msg = json.loads(raw_message)

                if msg.get("data"):
                    #async with lock:
                    fund, time = self.manager.find_data('bitget', 'futures', str(msg.get("arg").get("instId"))) # type: ignore
                    if fund != None and time != None:
                        orderbook[str(msg.get("arg").get("instId"))]['bitget']['futures']['funding'] = fund
                        orderbook[str(msg.get("arg").get("instId"))]['bitget']['futures']['get_funding'] = time
                    orderbook[str(msg.get("arg").get("instId"))]["bitget"][
                        "futures"
                    ]["asks"] = [[float(price), float(size)] for price, size in msg['data'][0]['asks']]#msg["data"][0]["asks"]
                    orderbook[str(msg.get("arg").get("instId"))]["bitget"][
                        "futures"
                    ]["bids"] = [[float(price), float(size)] for price, size in msg['data'][0]['bids']]#msg["data"][0]["bids"]
            except Exception as e:
                print(f"❌ Bitget FUTURES parse error: {e}")

    async def _handle_spot_connection(self, conn_id: str):
        """Futures соединение с динамическими подписками"""
        url = "wss://ws.bitget.com/v2/ws/public"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=10, ping_timeout=60) as ws:
            #print(f"✅ bitget spot conn#{conn_id} connected")
            self.ws = ws

            receive_task = asyncio.create_task(self._process_spot_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "bitget", "spot"
                    )

                    # Лимит 30 на соединение
                    # target_symbols = set(list(target_symbols)[:30])

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    # Подписываемся
                    for symbol in to_subscribe:
                        try:

                            sub = {
                                "op": "subscribe",
                                "args": [
                                    {
                                        "instType": "SPOT",
                                        "channel": "books15",
                                        "instId": symbol,
                                    }
                                ],
                            }
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            #print(f"➕ Bitget spot subscribed: {symbol}")
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            print(f"❌ Bitget spot subscribe error {symbol}: {e}")

                    # Отписываемся
                    # for symbol in to_unsubscribe:
                    #     try:

                    #         unsub = {
                    #             "op": "unsubscribe",
                    #             "args": [
                    #                 {
                    #                     "instType": "SPOT",
                    #                     "channel": "books15",
                    #                     "instId": symbol,
                    #                 }
                    #             ],
                    #         }
                    #         await ws.send(json.dumps(unsub))
                    #         current_subscribed.remove(symbol)
                    #         print(f"➖ Bitget SPOT unsubscribed: {symbol}")

                    #         async with lock:
                    #             if symbol in orderbook:
                    #                 if "bitget" in orderbook[symbol]:
                    #                     if "spot" in orderbook[symbol]["bitget"]:
                    #                         del orderbook[symbol]["bitget"]["spot"]
                    #     except Exception as e:
                    #         print(f"❌ Bitget spot unsubscribe error {symbol}: {e}")
                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['spot'].add(symbol)
                        #print(f"📋 bitget spot queued for unsubscribe: {symbol}")

                    try:
                        await asyncio.wait_for(
                            self.manager.change_event.wait(), timeout=5
                        )
                        self.manager.change_event.clear()
                    except asyncio.TimeoutError:
                        pass
            finally:
                receive_task.cancel()

    async def _process_spot_messages(self, ws):
        """Обработка futures сообщений"""
        async for raw_message in ws:
            try:
                if raw_message == 'pong':
                    #print(raw_message)
                    continue                
                msg = json.loads(raw_message)

                if msg.get("data"):
                    #async with lock:
                    orderbook[str(msg.get("arg").get("instId"))]["bitget"]["spot"][
                        "asks"
                    ] = [[float(price), float(size)] for price, size in msg['data'][0]['asks']]#msg["data"][0]["asks"]
                    orderbook[str(msg.get("arg").get("instId"))]["bitget"]["spot"][
                        "bids"
                    ] = [[float(price), float(size)] for price, size in msg['data'][0]['bids']]#msg["data"][0]["bids"]
            except Exception as e:
                print(f"❌ Bitget spot parse error: {e}")

    async def run_spot(self):
        """Запуск spot соединения"""
        await self._reconnect_wrapper(self._handle_spot_connection, "spot")

    async def run_futures(self):
        """Запуск futures соединения"""
        await self._reconnect_wrapper(self._handle_futures_connection, "futures")


class GateioDynamicWS(BaseDynamicWSClient):
    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("Gate", manager)
        self.ws = None
        self.ws_futures = None
        # Очереди для накопления отписок
        self.unsub = defaultdict(set)
        self.lock = asyncio.Lock()
        
        
    async def _batch_unsubscribe_worker(self):
        """Воркер для batch-отписок раз в 5 минут"""
        while self.is_running:
            await asyncio.sleep(90)  # 5 минут
            
            async with self.lock:
                queue = self.unsub.copy()
                self.unsub.clear()

            if queue:
                print(f"🗑️ gateio processing {len(queue)} unsubscribes")
                
                # Отправляем unsub для каждого символа
                for market, data in queue.items():
                    for symbol in data:
                        try:
                            if market == "spot":
                                unsub = {
                                    "channel": "spot.order_book",
                                    "event": "unsubscribe",
                                    "payload": [f"{symbol.replace('USDT', '_USDT')}"],
                                }
                                await self.ws.send(json.dumps(unsub))
                            else:
                                unsub = {
                                    "channel": "futures.order_book",
                                    "event": "unsubscribe",
                                    "payload": [f"{symbol.replace('USDT', '_USDT')}"],
                                }
                                await self.ws_futures.send(json.dumps(unsub))
                            
                            #print(f"➖ gateio batch unsubscribed: {symbol}")
                            await asyncio.sleep(0.05)  # Небольшая задержка между unsub
                            
                        except Exception as e:
                            print(f"❌ Batch gateio unsubscribe error {symbol}: {e}")
                
                # Очистка orderbook для накопленных символов
                #async with lock:
                for type, symbols in queue.items():
                    for symbol in symbols:
                        if symbol in orderbook:
                            if "gateio" in orderbook[symbol]:
                                if type in orderbook[symbol]["gateio"]:
                                    del orderbook[symbol]["gateio"][type]
                                    if not orderbook[symbol]['gateio']:
                                        del orderbook[symbol]["gateio"]
                                        if not orderbook[symbol]:
                                            del orderbook[symbol]


    async def _handle_futures_connection(self, conn_id: str):
        """Futures соединение с динамическими подписками"""
        url = f"wss://fx-ws.gateio.ws/v4/ws/usdt"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=20, ping_timeout=120) as ws:
            #print(f"✅ gateio FUTURES conn#{conn_id} connected")
            self.ws_futures = ws
            receive_task = asyncio.create_task(self._process_futures_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "gateio", "futures"
                    )

                    # Лимит 30 на соединение
                    # target_symbols = set(list(target_symbols)[:30])

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    # Подписываемся
                    for symbol in to_subscribe:
                        try:

                            sub = {
                                "channel": "futures.order_book",
                                "event": "subscribe",
                                "payload": [
                                    f"{symbol.replace('USDT', '_USDT')}",
                                    "100",
                                    "0",
                                ],
                            }
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            #print(f"➕ gateio FUTURES subscribed: {symbol}")
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            print(f"❌ gateio FUTURES subscribe error {symbol}: {e}")

                    # Отписываемся
                    # for symbol in to_unsubscribe:
                    #     try:

                    #         unsub = {
                    #             "channel": "futures.order_book",
                    #             "event": "unsubscribe",
                    #             "payload": [f"{symbol.replace('USDT', '_USDT')}"],
                    #         }
                    #         await ws.send(json.dumps(unsub))
                    #         current_subscribed.remove(symbol)
                    #         print(f"➖ gateio FUTURES unsubscribed: {symbol}")

                    #         async with lock:
                    #             if symbol in orderbook:
                    #                 if "gateio" in orderbook[symbol]:
                    #                     if "futures" in orderbook[symbol]["gateio"]:
                    #                         del orderbook[symbol]["gateio"]["futures"]
                    #     except Exception as e:
                    #         print(f"❌ gateio FUTURES unsubscribe error {symbol}: {e}")
                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['futures'].add(symbol)
                        #print(f"📋 gateio futures queued for unsubscribe: {symbol}")

                    try:
                        await asyncio.wait_for(
                            self.manager.change_event.wait(), timeout=5
                        )
                        self.manager.change_event.clear()
                    except asyncio.TimeoutError:
                        pass
            finally:
                receive_task.cancel()

    async def _process_futures_messages(self, ws):
        """Обработка futures сообщений"""
        async for raw_message in ws:
            try:
                msg = json.loads(raw_message)
                result = msg.get("result", {})

                if "asks" in result and "bids" in result:
                    fund, time = self.manager.find_data('gateio', 'futures', msg.get("result").get("contract", "UNKNOWN").replace("_USDT", "USDT")) # type: ignore
                    if fund != None and time != None:
                        orderbook[msg.get("result").get("contract", "UNKNOWN").replace("_USDT", "USDT")]['gateio']['futures']['funding'] = fund
                        orderbook[msg.get("result").get("contract", "UNKNOWN").replace("_USDT", "USDT")]['gateio']['futures']['get_funding'] = time
                    #async with lock:
                    orderbook[msg.get("result").get("contract", "UNKNOWN").replace("_USDT", "USDT")]["gateio"]["futures"]["asks"] = [
                        [float(l["p"]), float(l["s"])] for l in result.get("asks", [])
                    ]
                    orderbook[msg.get("result").get("contract", "UNKNOWN").replace("_USDT", "USDT")]["gateio"]["futures"]["bids"] = [
                        [float(l["p"]), float(l["s"])] for l in result.get("bids", [])
                    ]
                elif "status" in result:
                    continue
                    # это подтверждение подписки или ошибки
                    #print(f"ℹ️ Gateio FUTURES status message: {result['status']}")
                else:
                    # неожиданный формат
                    print(f"⚠️ Gateio FUTURES unknown message: {msg}")

            except Exception as e:
                print(f"❌ gateio FUTURES parse error: {e}")

    async def _handle_spot_connection(self, conn_id: str):
        """Futures соединение с динамическими подписками"""
        url = f"wss://api.gateio.ws/ws/v4/"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=20, ping_timeout=120) as ws:
            #print(f"✅ gateio spot conn#{conn_id} connected")
            self.ws = ws

            receive_task = asyncio.create_task(self._process_spot_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "gateio", "spot"
                    )

                    # Лимит 30 на соединение
                    # target_symbols = set(list(target_symbols)[:30])

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    # Подписываемся
                    for symbol in to_subscribe:
                        try:

                            sub = {
                                "channel": "spot.order_book",
                                "event": "subscribe",
                                "payload": [
                                    f"{symbol.replace('USDT', '_USDT')}",
                                    "100",
                                    "100ms",
                                ],
                            }
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            #print(f"➕ gateio spot subscribed: {symbol}")
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            print(f"❌ gateio spot subscribe error {symbol}: {e}")

                    # Отписываемся
                    # for symbol in to_unsubscribe:
                    #     try:

                    #         unsub = {
                    #             "channel": "spot.order_book",
                    #             "event": "unsubscribe",
                    #             "payload": [f"{symbol.replace('USDT', '_USDT')}"],
                    #         }
                    #         await ws.send(json.dumps(unsub))
                    #         current_subscribed.remove(symbol)
                    #         print(f"➖ gateio spot unsubscribed: {symbol}")

                    #         async with lock:
                    #             if symbol in orderbook:
                    #                 if "gateio" in orderbook[symbol]:
                    #                     if "spot" in orderbook[symbol]["gateio"]:
                    #                         del orderbook[symbol]["gateio"]["spot"]
                    #     except Exception as e:
                    #         print(f"❌ gateio spot unsubscribe error {symbol}: {e}")
                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['spot'].add(symbol)
                        #print(f"📋 gateio spot queued for unsubscribe: {symbol}")

                    try:
                        await asyncio.wait_for(
                            self.manager.change_event.wait(), timeout=5
                        )
                        self.manager.change_event.clear()
                    except asyncio.TimeoutError:
                        pass
            finally:
                receive_task.cancel()

    async def _process_spot_messages(self, ws):
        """Обработка futures сообщений"""
        async for raw_message in ws:
            try:
                msg = json.loads(raw_message)

                result = msg.get("result", {})

                if "asks" in result and "bids" in result:
                    #async with lock:
                    orderbook[msg.get("result").get("s").replace("_USDT", "USDT")][
                        "gateio"
                    ]["spot"]["asks"] = [
                        [float(price), float(size)]
                        for price, size in msg["result"]["asks"]
                    ]
                    orderbook[msg.get("result").get("s").replace("_USDT", "USDT")][
                        "gateio"
                    ]["spot"]["bids"] = [
                        [float(price), float(size)]
                        for price, size in msg["result"]["bids"]
                    ]
                elif "status" in result:
                    continue
                    # это подтверждение подписки или ошибки
                    #print(f"ℹ️ Gateio spot status message: {result['status']}")
                else:
                    # неожиданный формат
                    print(f"⚠️ Gateio spot unknown message: {msg}")

            except Exception as e:
                print(f"❌ gateio spot parse error: {e}")

    async def run_spot(self):
        """Запуск spot соединения"""
        await self._reconnect_wrapper(self._handle_spot_connection, "spot")

    async def run_futures(self):
        """Запуск futures соединения"""
        await self._reconnect_wrapper(self._handle_futures_connection, "futures")

async def update_data():
    while True:
        data = await mainn()
        async with subscriptions_lock:
            subscriptions_config.clear()
            subscriptions_config.update(data)
            #print(subscriptions_config)
        await asyncio.sleep(3)
            
class KucoinDynamicWS(BaseDynamicWSClient):
    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("Kucoin", manager)
        self.ws = None
        self.ws_futures = None
        # Очереди для накопления отписок
        self.unsub = defaultdict(set)
        self.lock = asyncio.Lock()
        
        
    async def _batch_unsubscribe_worker(self):
        """Воркер для batch-отписок раз в 5 минут"""
        while self.is_running:
            await asyncio.sleep(90)  # 5 минут
            
            async with self.lock:
                queue = self.unsub.copy()
                self.unsub.clear()

            if queue:
                print(f"🗑️ kucoin processing {len(queue)} unsubscribes")
                
                # Отправляем unsub для каждого символа
                for market, data in queue.items():
                    for symbol in data:
                        try:
                            if market == "spot":
                                unsub = {
                                    "id": int(time.time() * 1000),
                                    "type": "unsubscribe",
                                    "topic": f"/spotMarket/level2Depth50:{symbol.replace('USDT', '-USDT')}",
                                    "response": True,
                                }
                                await self.ws.send(json.dumps(unsub))
                            else:
                                unsub = {
                                    "id": int(time.time() * 1000),
                                    "type": "unsubscribe",
                                    "topic": f"/contractMarket/level2Depth50:{symbol}M",
                                    "response": True,
                                }
                                await self.ws_futures.send(json.dumps(unsub))
                            
                            #print(f"➖ kucoin batch unsubscribed: {symbol}")
                            await asyncio.sleep(0.05)  # Небольшая задержка между unsub
                            
                        except Exception as e:
                            print(f"❌ Batch kucoin unsubscribe error {symbol}: {e}")
                
                # Очистка orderbook для накопленных символов
                #async with lock:
                for type, symbols in queue.items():
                    for symbol in symbols:
                        if symbol in orderbook:
                            if "kucoin" in orderbook[symbol]:
                                if type in orderbook[symbol]["kucoin"]:
                                    del orderbook[symbol]["kucoin"][type]
                                    if not orderbook[symbol]['kucoin']:
                                        del orderbook[symbol]["kucoin"]
                                        if not orderbook[symbol]:
                                            del orderbook[symbol]
                                                
    async def _handle_futures_connection(self, conn_id: str):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api-futures.kucoin.com/api/v1/bullet-public"
            ) as r:
                data = await r.json()
                token = data["data"]["token"]

        url = f"wss://ws-api-futures.kucoin.com?token={token}"

        current_subscribed = set()

        async with websockets.connect(url) as ws:
            #print(f"✅ kucoin FUTURES conn#{conn_id} connected")
            self.ws_futures = ws
            receive_task = asyncio.create_task(self._process_futures_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "kucoin", "futures"
                    )

                    # Лимит 30 на соединение
                    # target_symbols = set(list(target_symbols)[:30])

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    # Подписываемся
                    for symbol in to_subscribe:
                        try:

                            sub = {
                                "id": int(time.time() * 1000),
                                "type": "subscribe",
                                "topic": f"/contractMarket/level2Depth50:{symbol}M",
                                "response": True,
                            }
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            #print(f"➕ Kucoin FUTURES subscribed: {symbol}")
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            print(f"❌ Kucoin FUTURES subscribe error {symbol}: {e}")

                    # Отписываемся
                    # for symbol in to_unsubscribe:
                    #     try:

                    #         unsub = {
                    #             "id": int(time.time() * 1000),
                    #             "type": "unsubscribe",
                    #             "topic": f"/contractMarket/level2Depth50:{symbol}M",
                    #             "response": True,
                    #         }
                    #         await ws.send(json.dumps(unsub))
                    #         current_subscribed.remove(symbol)
                    #         print(f"➖ Kucoin FUTURES unsubscribed: {symbol}")

                    #         async with lock:
                    #             if symbol in orderbook:
                    #                 if "kucoin" in orderbook[symbol]:
                    #                     if "futures" in orderbook[symbol]["kucoin"]:
                    #                         del orderbook[symbol]["kucoin"]["futures"]
                    #     except Exception as e:
                    #         print(f"❌ Kucoin FUTURES unsubscribe error {symbol}: {e}")
                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['futures'].add(symbol)
                        #print(f"📋 kucoin futures queued for unsubscribe: {symbol}")

                    try:
                        await asyncio.wait_for(
                            self.manager.change_event.wait(), timeout=5
                        )
                        self.manager.change_event.clear()
                    except asyncio.TimeoutError:
                        pass
            finally:
                # корректно завершаем receive_task
                receive_task.cancel()
                try:
                    await receive_task
                except asyncio.CancelledError:
                    pass

    async def _process_futures_messages(self, ws):
        """Обработка входящих сообщений KUCOIN FUTURES"""
        async for raw_message in ws:
            try:
                msg = json.loads(raw_message)

                # ---------- 1) Обработка ping (исправлено!) ----------
                if msg.get("type") == "ping":
                    await ws.send(json.dumps({"type": "pong", "id": msg["id"]}))
                    continue

                # ---------- 2) Проверка данных ордербука ----------
                if msg.get("data") and msg.get("topic"):

                    topic = msg["topic"]
                    # topic: "/contractMarket/level2Depth50:XBTUSDTM"

                    if ":" not in topic:
                        continue

                    full_symbol = topic.split(":", 1)[1]  # XBTUSDTM

                    # ---------- 3) Аккуратно убираем M ----------
                    symbol = (
                        full_symbol[:-1] if full_symbol.endswith("M") else full_symbol
                    )

                    # ---------- 4) ИНИЦИАЛИЗАЦИЯ ОБЪЕКТА ОБЯЗАТЕЛЬНА ----------
                    #async with lock:
                        # orderbook.setdefault(symbol, {}).setdefault("kucoin", {}).setdefault(
                        #     "futures", {"bids": [], "asks": []}
                        # )
                    fund, time = self.manager.find_data('kucoin', 'futures', symbol) # type: ignore
                    if fund != None and time != None:
                        orderbook[symbol]['kucoin']['futures']['funding'] = fund
                        orderbook[symbol]['kucoin']['futures']['get_funding'] = time
                    orderbook[symbol]["kucoin"]["futures"]["asks"] = [
                        [float(price), float(size)]
                        for price, size in msg["data"].get("asks", [])
                    ]
                    orderbook[symbol]["kucoin"]["futures"]["bids"] = [
                        [float(price), float(size)]
                        for price, size in msg["data"].get("bids", [])
                    ]

            except Exception as e:
                print(f"❌ Kucoin FUTURES parse error: {e}")

    async def _handle_spot_connection(self, conn_id: str):
        async with aiohttp.ClientSession() as session:
            async with session.post("https://api.kucoin.com/api/v1/bullet-public") as r:
                data = await r.json()
                token = data["data"]["token"]

        url = f"wss://ws-api-spot.kucoin.com?token={token}"

        current_subscribed = set()

        async with websockets.connect(url) as ws:
            #print(f"✅ kucoin spot conn#{conn_id} connected")
            self.ws = ws
            receive_task = asyncio.create_task(self._process_spot_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "kucoin", "spot"
                    )

                    # Лимит 30 на соединение
                    # target_symbols = set(list(target_symbols)[:30])

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    # Подписываемся
                    for symbol in to_subscribe:
                        try:

                            sub = {
                                "id": int(time.time() * 1000),
                                "type": "subscribe",
                                "topic": f"/spotMarket/level2Depth50:{symbol.replace('USDT', '-USDT')}",
                                "response": True,
                            }
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            #print(f"➕ Kucoin spot subscribed: {symbol}")
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            print(f"❌ Kucoin spot subscribe error {symbol}: {e}")

                    # Отписываемся
                    # for symbol in to_unsubscribe:
                    #     try:

                    #         unsub = {
                    #             "id": int(time.time() * 1000),
                    #             "type": "unsubscribe",
                    #             "topic": f"/spotMarket/level2Depth50:{symbol.replace('USDT', '-USDT')}",
                    #             "response": True,
                    #         }
                    #         await ws.send(json.dumps(unsub))
                    #         current_subscribed.remove(symbol)
                    #         print(f"➖ Kucoin spot unsubscribed: {symbol}")

                    #         async with lock:
                    #             if symbol in orderbook:
                    #                 if "kucoin" in orderbook[symbol]:
                    #                     if "spot" in orderbook[symbol]["kucoin"]:
                    #                         del orderbook[symbol]["kucoin"]["spot"]
                    #     except Exception as e:
                    #         print(f"❌ Kucoin spot unsubscribe error {symbol}: {e}")
                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['spot'].add(symbol)
                        #print(f"📋 kucoin spot queued for unsubscribe: {symbol}")

                    try:
                        await asyncio.wait_for(
                            self.manager.change_event.wait(), timeout=5
                        )
                        self.manager.change_event.clear()
                    except asyncio.TimeoutError:
                        pass
            finally:
                # корректно завершаем receive_task
                receive_task.cancel()
                try:
                    await receive_task
                except asyncio.CancelledError:
                    pass

    async def _process_spot_messages(self, ws):
        """Обработка входящих сообщений KUCOIN FUTURES"""
        async for raw_message in ws:
            try:
                msg = json.loads(raw_message)

                # ---------- 1) Обработка ping (исправлено!) ----------
                if msg.get("type") == "ping":
                    await ws.send(json.dumps({"type": "pong", "id": msg["id"]}))
                    continue

                # ---------- 2) Проверка данных ордербука ----------
                if msg.get("data") and msg.get("topic"):

                    topic = msg["topic"]
                    # topic: "/contractMarket/level2Depth50:XBTUSDTM"

                    if ":" not in topic:
                        continue

                    full_symbol = topic.split(":", 1)[1]  # XBTUSDTM

                    # ---------- 3) Аккуратно убираем M ----------
                    symbol = full_symbol.replace("-USDT", "USDT")

                    # ---------- 4) ИНИЦИАЛИЗАЦИЯ ОБЪЕКТА ОБЯЗАТЕЛЬНА ----------
                    #async with lock:


                    orderbook[symbol]["kucoin"]["spot"]["asks"] = [
                        [float(price), float(size)]
                        for price, size in msg["data"].get("asks", [])
                    ]
                    orderbook[symbol]["kucoin"]["spot"]["bids"] = [
                        [float(price), float(size)]
                        for price, size in msg["data"].get("bids", [])
                    ]

            except Exception as e:
                print(f"❌ Kucoin spot parse error: {e}")

    async def run_spot(self):
        """Запуск spot соединения"""
        await self._reconnect_wrapper(self._handle_spot_connection, "spot")

    async def run_futures(self):
        """Запуск futures соединения"""
        await self._reconnect_wrapper(self._handle_futures_connection, "futures")


class HtxDynamicWS(BaseDynamicWSClient):
    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("Htx", manager)
        self.ws = None
        self.ws_futures = None
        # Очереди для накопления отписок
        self.unsub = defaultdict(set)
        self.lock = asyncio.Lock()
        
        
    async def _batch_unsubscribe_worker(self):
        """Воркер для batch-отписок раз в 5 минут"""
        while self.is_running:
            await asyncio.sleep(90)  # 5 минут

            
            async with self.lock:

                queue = self.unsub.copy()
                self.unsub.clear()

            if queue:
                print(f"🗑️ htx processing {len(queue)} unsubscribes")
                
                # Отправляем unsub для каждого символа
                for market, data in queue.items():
                    for symbol in data:
                        try:
                            if market == "spot":
                                unsub = {"unsub": f"market.{symbol.lower()}.depth.step0"}
                                await self.ws.send(json.dumps(unsub))
                            else:
                                unsub = {
                                    "unsub": f"market.{symbol.replace('USDT', '-USDT')}.depth.step0"
                                }
                                await self.ws_futures.send(json.dumps(unsub))
                            
                            #print(f"➖ htx batch unsubscribed: {symbol}")
                            await asyncio.sleep(0.05)  # Небольшая задержка между unsub
                            
                        except Exception as e:
                            print(f"❌ Batch htx unsubscribe error {symbol}: {e}")
                
                # Очистка orderbook для накопленных символов
                #async with lock:
                for type, symbols in queue.items():
                    for symbol in symbols:
                        if symbol in orderbook:
                            if "htx" in orderbook[symbol]:
                                if type in orderbook[symbol]["htx"]:
                                    del orderbook[symbol]["htx"][type]
                                    if not orderbook[symbol]['htx']:
                                        del orderbook[symbol]["htx"]
                                        if not orderbook[symbol]:
                                            del orderbook[symbol]        

    
    
    async def _handle_futures_connection(self, conn_id: str):

        url = f"wss://api.hbdm.vn/linear-swap-ws"

        current_subscribed = set()

        async with websockets.connect(url) as ws:
            #print(f"✅ htx FUTURES conn#{conn_id} connected")
            self.ws_futures = ws

            receive_task = asyncio.create_task(self._process_futures_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "htx", "futures"
                    )

                    # Лимит 30 на соединение
                    # target_symbols = set(list(target_symbols)[:30])

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    # Подписываемся
                    for symbol in to_subscribe:
                        try:

                            sub = {
                                "sub": f"market.{symbol.replace('USDT', '-USDT')}.depth.step0"
                            }
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            #print(f"➕ htx FUTURES subscribed: {symbol}")
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            print(f"❌ htx FUTURES subscribe error {symbol}: {e}")

                    # Отписываемся
                    # for symbol in to_unsubscribe:
                    #     try:

                    #         unsub = {
                    #             "unsub": f"market.{symbol.replace('USDT', '-USDT')}.depth.step0"
                    #         }
                    #         await ws.send(json.dumps(unsub))
                    #         current_subscribed.remove(symbol)
                    #         print(f"➖ htx FUTURES unsubscribed: {symbol}")

                    #         async with lock:
                    #             if symbol in orderbook:
                    #                 if "htx" in orderbook[symbol]:
                    #                     if "futures" in orderbook[symbol]["htx"]:
                    #                         del orderbook[symbol]["htx"]["futures"]
                    #     except Exception as e:
                    #         print(f"❌ htx FUTURES unsubscribe error {symbol}: {e}")
                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['futures'].add(symbol)
                        #print(f"📋 htx futures queued for unsubscribe: {symbol}")

                    try:
                        await asyncio.wait_for(
                            self.manager.change_event.wait(), timeout=5
                        )
                        self.manager.change_event.clear()
                    except asyncio.TimeoutError:
                        pass
            finally:
                # корректно завершаем receive_task
                receive_task.cancel()
                try:
                    await receive_task
                except asyncio.CancelledError:
                    pass

    async def _process_futures_messages(self, ws):
        """Обработка futures сообщений"""
        async for raw_message in ws:
            try:
                decompressed = gzip.decompress(raw_message)
                msg = json.loads(decompressed.decode("utf-8"))

                if "ping" in msg:
                    await ws.send(json.dumps({"pong": msg["ping"]}))
                    continue

                if "status" in msg:
                    continue

                #async with lock:
                if msg['tick']['bids'] and msg['tick']['asks']:
                    fund, time = self.manager.find_data('htx', 'futures', msg.get("ch").split(".")[1].replace("-USDT", "USDT")) # type: ignore
                    if fund != None and time != None:
                        orderbook[msg.get("ch").split(".")[1].replace("-USDT", "USDT")]['htx']['futures']['funding'] = fund
                        orderbook[msg.get("ch").split(".")[1].replace("-USDT", "USDT")]['htx']['futures']['get_funding'] = time
                    orderbook[msg.get("ch").split(".")[1].replace("-USDT", "USDT")][
                        "htx"
                    ]["futures"]["asks"] = msg["tick"]["asks"]
                    orderbook[msg.get("ch").split(".")[1].replace("-USDT", "USDT")][
                        "htx"
                    ]["futures"]["bids"] = msg["tick"]["bids"]

            except Exception as e:
                print(f"❌ htx FUTURES parse error: {e}")

    async def _handle_spot_connection(self, conn_id: str):

        url = f"wss://api-aws.huobi.pro/ws"

        current_subscribed = set()

        async with websockets.connect(url) as ws:
            #print(f"✅ htx spot conn#{conn_id} connected")
            self.ws = ws
            receive_task = asyncio.create_task(self._process_spot_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "htx", "spot"
                    )

                    # Лимит 30 на соединение
                    # target_symbols = set(list(target_symbols)[:30])

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    # Подписываемся
                    for symbol in to_subscribe:
                        try:

                            sub = {"sub": [f"market.{symbol.lower()}.depth.step0"]}
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            #print(f"➕ htx spot subscribed: {symbol}")
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            print(f"❌ htx spot subscribe error {symbol}: {e}")

                    # Отписываемся
                    # for symbol in to_unsubscribe:
                    #     try:

                    #         unsub = {"unsub": f"market.{symbol.lower()}.depth.step0"}
                    #         await ws.send(json.dumps(unsub))
                    #         current_subscribed.remove(symbol)
                    #         print(f"➖ htx spot unsubscribed: {symbol}")

                    #         async with lock:
                    #             if symbol in orderbook:
                    #                 if "htx" in orderbook[symbol]:
                    #                     if "spot" in orderbook[symbol]["htx"]:
                    #                         del orderbook[symbol]["htx"]["spot"]
                    #     except Exception as e:
                    #         print(f"❌ htx spot unsubscribe error {symbol}: {e}")
                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['spot'].add(symbol)
                        #print(f"📋 htx spot queued for unsubscribe: {symbol}")
                            

                    try:
                        await asyncio.wait_for(
                            self.manager.change_event.wait(), timeout=5
                        )
                        self.manager.change_event.clear()
                    except asyncio.TimeoutError:
                        pass
            finally:
                # корректно завершаем receive_task
                receive_task.cancel()
                try:
                    await receive_task
                except asyncio.CancelledError:
                    pass

    async def _process_spot_messages(self, ws):
        """Обработка futures сообщений"""
        async for raw_message in ws:
            try:
                decompressed = gzip.decompress(raw_message)
                msg = json.loads(decompressed.decode("utf-8"))

                if "ping" in msg:
                    await ws.send(json.dumps({"pong": msg["ping"]}))
                    continue

                if "status" in msg:
                    continue

                #async with lock:
                
                if msg['tick']['bids'] and msg['tick']['asks']:
                    orderbook[
                        msg.get("ch").split(".")[1].replace("-USDT", "USDT").upper()
                    ]["htx"]["spot"]["asks"] = msg["tick"]["asks"]
                    orderbook[
                        msg.get("ch").split(".")[1].replace("-USDT", "USDT").upper()
                    ]["htx"]["spot"]["bids"] = msg["tick"]["bids"]

            except Exception as e:
                print(f"❌ htx FUTURES parse error: {e}")

    async def run_spot(self):
        """Запуск spot соединения"""
        await self._reconnect_wrapper(self._handle_spot_connection, "spot")

    async def run_futures(self):
        """Запуск futures соединения"""
        await self._reconnect_wrapper(self._handle_futures_connection, "futures")


class OkxDynamicWS(BaseDynamicWSClient):
    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("Okx", manager)
        
        self.ws = None
        self.ws_futures = None
        # Очереди для накопления отписок
        self.unsub = defaultdict(set)
        self.lock = asyncio.Lock()
        
        
    def _calculate_checksum(self, bids_list, asks_list):
        """
        Вычисляет CRC32 checksum для первых 25 уровней стакана.
        bids_list и asks_list — список списков: [[price, size], ...], оба float
        """
        # Берем только первые 25 уровней
        top_bids = bids_list[:25]
        top_asks = asks_list[:25]

        checksum_parts = []

        # Формируем строку: price1:size1:price2:size2...
        max_len = max(len(top_bids), len(top_asks))
        for i in range(max_len):
            if i < len(top_bids):
                p, s = top_bids[i]
                checksum_parts.append(f"{format(p, 'f')}:{format(s, 'f')}")
            if i < len(top_asks):
                p, s = top_asks[i]
                checksum_parts.append(f"{format(p, 'f')}:{format(s, 'f')}")

        checksum_str = ":".join(checksum_parts)

        # CRC32 через ctypes для signed int32
        return ctypes.c_int32(crc32(checksum_str.encode())).value
    
    
    
    
    def update_orderbook(self, symbol, exchange, market_type, data, action, orderbooks):
        """Обновляет стакан инкрементально или полностью, оставляя bids/asks списком списков с float"""
        ob = orderbooks[symbol][exchange][market_type]
        
        
        if market_type == 'futures':
            fund, time = self.manager.find_data('okx', 'futures', symbol) # type: ignore
            if fund != None and time != None:
                orderbook[symbol]['okx']['futures']['funding'] = fund
                orderbook[symbol]['okx']['futures']['get_funding'] = time
                
                
        if 'bids' not in ob:
            ob['bids'] = []
        if 'asks' not in ob:
            ob['asks'] = []
        if 'seqId' not in ob:
            ob['seqId'] = None
        if 'prevSeqId' not in ob:
            ob['prevSeqId'] = None

        prev_seq = data.get('prevSeqId')
        seq = data.get('seqId')

        # Проверка последовательности
        if action == 'snapshot':
            if prev_seq != -1:
                print(f"⚠️ Некорректный snapshot: prevSeqId должен быть -1, получено {prev_seq}")
            ob['bids'] = [[float(b[0]), float(b[1])] for b in data.get('bids', [])]
            ob['asks'] = [[float(a[0]), float(a[1])] for a in data.get('asks', [])]
        else:
            # Проверяем последовательность
            if ob['seqId'] is not None:
                if prev_seq != ob['seqId']:
                    if seq < prev_seq:
                        print(f"🔄 Обнаружен reset последовательности: prevSeqId={prev_seq}, seqId={seq}")
                    elif prev_seq == seq:
                        print(f"💓 Heartbeat сообщение: seqId={seq}")
                        return
                    else:
                        print(f"❌ Пропуск сообщения! Ожидается prevSeqId={ob['seqId']}, получено {prev_seq}")
                        return
            
            # Инкрементальное обновление
            # bids
            bids_dict = {b[0]: b[1] for b in ob['bids']}
            for bid in data.get('bids', []):
                price, size = float(bid[0]), float(bid[1])
                if size == 0:
                    bids_dict.pop(price, None)
                else:
                    bids_dict[price] = size
            # Сортируем по цене убыванию
            ob['bids'] = sorted([[p, s] for p, s in bids_dict.items()], key=lambda x: x[0], reverse=True)

            # asks
            asks_dict = {a[0]: a[1] for a in ob['asks']}
            for ask in data.get('asks', []):
                price, size = float(ask[0]), float(ask[1])
                if size == 0:
                    asks_dict.pop(price, None)
                else:
                    asks_dict[price] = size
            # Сортируем по цене возрастанию
            ob['asks'] = sorted([[p, s] for p, s in asks_dict.items()], key=lambda x: x[0])

        # Сохраняем seqId
        ob['prevSeqId'] = prev_seq
        ob['seqId'] = seq
        
        # if 'checksum' in data:
        #     calculated = self._calculate_checksum(ob['bids'], ob['asks'])
        #     received = data['checksum']
        #     if calculated != received:
        #         print(f"❌ CHECKSUM MISMATCH! Calculated: {calculated}, Received: {received}")
        #         print(f"   Рекомендуется переподписка на канал")
        #         # raise ChecksumMismatch(
        #         #     f"Checksum mismatch! Calculated={calculated}, Received={received}"
        #         # )
        #     else:
        #         print(f"✅ Checksum OK: {calculated}")

    async def _batch_unsubscribe_worker(self):
        """Воркер для batch-отписок раз в 5 минут"""
        while self.is_running:
            #print('gkgkk')
            await asyncio.sleep(90)  # 5 минут
            #print(self.ws)
            
            async with self.lock:
                #print(self.unsub)
                queue = self.unsub.copy()
                self.unsub.clear()

            if queue:
                print(f"🗑️ okx processing {len(queue)} unsubscribes")
                
                # Отправляем unsub для каждого символа
                for market, data in queue.items():
                    for symbol in data:
                        try:
                            if market == "spot":
                                unsub = {
                                    "op": "unsubscribe",
                                    "args": [
                                        {"channel": "books", "instId": f'{symbol.replace('USDT', '-USDT')}'}
                                    ],
                                }
                                await self.ws.send(json.dumps(unsub))
                            else:
                                unsub = {
                                    "op": "unsubscribe",
                                    "args": [
                                        {"channel": "books", "instId": f'{symbol.replace('USDT', '-USDT-SWAP')}'}
                                    ],
                                }
                                await self.ws_futures.send(json.dumps(unsub))
                            
                            #print(f"➖ okx batch unsubscribed: {symbol}")
                            await asyncio.sleep(0.05)  # Небольшая задержка между unsub
                            
                        except Exception as e:
                            print(f"❌ Batch okx unsubscribe error {symbol}: {e}")
                
                # Очистка orderbook для накопленных символов
                #async with lock:
                for type, symbols in queue.items():
                    for symbol in symbols:
                        if symbol in orderbook:
                            if "okx" in orderbook[symbol]:
                                if type in orderbook[symbol]["okx"]:
                                    del orderbook[symbol]["okx"][type]

    async def _handle_futures_connection(self, conn_id: str):

        url = f"wss://wspap.okx.com:8443/ws/v5/public"

        current_subscribed = set()

        async with websockets.connect(url) as ws:
            self.ws_futures = ws
            #print(f"✅ okx FUTURES conn#{conn_id} connected")

            receive_task = asyncio.create_task(self._process_futures_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "okx", "futures"
                    )

                    # Лимит 30 на соединение
                    # target_symbols = set(list(target_symbols)[:30])

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    # Подписываемся
                    for symbol in to_subscribe:
                        try:

                            sub = {
                                "op": "subscribe",
                                "args": [
                                    {"channel": "books", "instId": f'{symbol.replace('USDT', '-USDT-SWAP')}'}
                                ],
                            }
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            #print(f"➕ okx FUTURES subscribed: {symbol}")
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            print(f"❌ okx FUTURES subscribe error {symbol}: {e}")

                    # Отписываемся
                    # for symbol in to_unsubscribe:
                    #     try:

                    #         unsub = {
                    #             "op": "unsubscribe",
                    #             "args": [
                    #                 {"channel": "books", "instId": f'{symbol.replace('USDT', '-USDT-SWAP')}'}
                    #             ],
                    #         }
                    #         await ws.send(json.dumps(unsub))
                    #         current_subscribed.remove(symbol)
                    #         print(f"➖ okx FUTURES unsubscribed: {symbol}")

                    #         async with lock:
                    #             if symbol in orderbook:
                    #                 if "okx" in orderbook[symbol]:
                    #                     if "futures" in orderbook[symbol]["okx"]:
                    #                         del orderbook[symbol]["okx"]["futures"]
                    #     except Exception as e:
                    #         print(f"❌ okx FUTURES unsubscribe error {symbol}: {e}")
                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['futures'].add(symbol)
                        #print(f"📋 okx futures queued for unsubscribe: {symbol}")
                    try:
                        await asyncio.wait_for(
                            self.manager.change_event.wait(), timeout=5
                        )
                        self.manager.change_event.clear()
                    except asyncio.TimeoutError:
                        pass
            finally:
                # корректно завершаем receive_task
                receive_task.cancel()
                try:
                    await receive_task
                except asyncio.CancelledError:
                    pass
 
    async def _process_futures_messages(self, ws):
        async for raw in ws:
            try:
                msg = json.loads(raw)
                if msg.get("op") == "ping":
                    await ws.send(json.dumps({"op": "pong"}))
                    continue

                if msg.get("event") == "ping":
                    await ws.send(json.dumps({"event": "pong"}))
                    continue
                # Пропускаем служебные сообщения
                if 'event' in msg:
                    #print(f"📢 Event: {msg}")
                    continue
                
                if 'data' not in msg:
                    continue
                
                # Обработка данных стакана
                action = msg.get('action')  # 'snapshot' или 'update'
                arg = msg.get('arg', {})
                inst_id = arg.get('instId', 'UNKNOWN')
                
                for data in msg['data']:
                    # Определяем тип рынка (spot/futures)

                    
                    # Обновляем стакан
                    #async with lock:
                    self.update_orderbook(
                        symbol=inst_id.replace('-USDT-SWAP', 'USDT'),
                        exchange='okx',
                        market_type='futures',
                        data=data,
                        action=action,
                        orderbooks=orderbook
                    )
                    
                    # Выводим информацию
                      
            except Exception as e:
                print(f"❌ Ошибка: {e}")
                import traceback
                traceback.print_exc()
                                
    async def _handle_spot_connection(self, conn_id: str):

        url = f"wss://wspap.okx.com:8443/ws/v5/public"

        current_subscribed = set()

        async with websockets.connect(url) as ws:
            self.ws = ws
            #print(f"✅ okx spot conn#{conn_id} connected")

            receive_task = asyncio.create_task(self._process_spot_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "okx", "spot"
                    )

                    # Лимит 30 на соединение
                    # target_symbols = set(list(target_symbols)[:30])

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    # Подписываемся
                    for symbol in to_subscribe:
                        try:

                            sub = {
                                "op": "subscribe",
                                "args": [
                                    {"channel": "books", "instId": f'{symbol.replace('USDT', '-USDT')}'}
                                ],
                            }
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            #print(f"➕ okx spot subscribed: {symbol}")
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            print(f"❌ okx spot subscribe error {symbol}: {e}")

                    # Отписываемся
                    # for symbol in to_unsubscribe:
                    #     try:

                    # #         unsub = {
                    # #             "op": "unsubscribe",
                    # #             "args": [
                    # #                 {"channel": "books", "instId": f'{symbol.replace('USDT', '-USDT')}'}
                    # #             ],
                    # #         }
                    #         await ws.send(json.dumps(unsub))
                    #         current_subscribed.remove(symbol)
                    #         print(f"➖ okx spot unsubscribed: {symbol}")

                    #         async with lock:
                    #             if symbol in orderbook:
                    #                 if "okx" in orderbook[symbol]:
                    #                     if "spot" in orderbook[symbol]["okx"]:
                    #                         del orderbook[symbol]["okx"]["spot"]
                    #     except Exception as e:
                    #         print(f"❌ okx spot unsubscribe error {symbol}: {e}")
                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['spot'].add(symbol)
                        #print(f"📋 okx spot queued for unsubscribe: {symbol}")
                    try:
                        await asyncio.wait_for(
                            self.manager.change_event.wait(), timeout=5
                        )
                        self.manager.change_event.clear()
                    except asyncio.TimeoutError:
                        pass
            finally:
                # корректно завершаем receive_task
                receive_task.cancel()
                try:
                    await receive_task
                except asyncio.CancelledError:
                    pass
    
    async def _process_spot_messages(self, ws):
        async for raw in ws:
            try:
                msg = json.loads(raw)
                if msg.get("op") == "ping":
                    await ws.send(json.dumps({"op": "pong"}))
                    continue

                if msg.get("event") == "ping":
                    await ws.send(json.dumps({"event": "pong"}))
                    continue
                # Пропускаем служебные сообщения
                if 'event' in msg:
                    #print(f"📢 Event: {msg}")
                    continue
                
                if 'data' not in msg:
                    continue
                
                # Обработка данных стакана
                action = msg.get('action')  # 'snapshot' или 'update'
                arg = msg.get('arg', {})
                inst_id = arg.get('instId', 'UNKNOWN')
                
                for data in msg['data']:
                    # Определяем тип рынка (spot/futures)

                    
                    # Обновляем стакан
                    #async with lock:
                    self.update_orderbook(
                        symbol=inst_id.replace('-USDT', 'USDT'),
                        exchange='okx',
                        market_type='spot',
                        data=data,
                        action=action,
                        orderbooks=orderbook
                    )
                    
                    # Выводим информацию
                      
            except Exception as e:
                print(f"❌ Ошибка: {e}")
                import traceback
                traceback.print_exc()
    
    async def run_spot(self):
        """Запуск spot соединения"""
        await self._reconnect_wrapper(self._handle_spot_connection, "spot")

    async def run_futures(self):
        """Запуск futures соединения"""
        await self._reconnect_wrapper(self._handle_futures_connection, "futures")


async def order():
    """Сохранение orderbook"""
    #last_save = time.time()

    while True:
        await asyncio.sleep(0.5)

        #if time.time() - last_save >= 5:
        try:
            #async with lock:
            snapshot = dict(orderbook)

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: json.dump(snapshot, open("orderbook.json", "w"), indent=2),
            )
            #last_save = time.time()
            #print(f"💾 Orderbook saved: {len(snapshot)} symbols")
        except Exception as e:
            print(f"❌ Save error: {e}")


async def арбитраж_повтор(мин_обьем, макс_обьем, шаг):
    last_update_time = {}
    update_interval = 30  # обновлять каждые 10 секунд
    id_map = {}
    
    start_time = time.time()
    try:
        while True:
            #print('начало')
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
            
            # Собираем ВСЕ возможности со всех символов
            все_возможности = []
            
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


                                    buy_slippage = ((pos_buy["buy_avg"] - pos_buy["best_ask"]) / pos_buy["best_ask"]) * 100
                                    sell_slippage = ((pos_sell["best_bid"] - pos_sell["sell_avg"]) / pos_sell["best_bid"]) * 100
                                    
                                    total_slippage = buy_slippage + sell_slippage


                                    комиссии = pos_sell["fee_taker"] + pos_buy["fee_taker"]
                                    funding_spread = pos_sell["funding"] - pos_buy["funding"]
                                    курсовой = ((pos_sell["sell_avg"] - pos_buy["buy_avg"]) / pos_buy["buy_avg"]) * 100
                                    spread_total = курсовой - комиссии + funding_spread
                                    спред_юсдт = ((volume * 2) / 100) * spread_total
                                
                                    #if spread_total >= 9:
                                    if spread_total >= 2:
                                        все_возможности.append({
                                            "symbol": symbol,
                                            'slippage_for_long': sell_slippage,
                                            "ex_long": pos_buy["buy_avg"],
                                            "ex_long_id": pos_buy["exchange"],
                                            "ex_long_type": pos_buy["market_type"],
                                            "ex_short": pos_sell["sell_avg"],
                                            "ex_short_id": pos_sell["exchange"],
                                            "ex_short_type": pos_sell["market_type"],
                                            'slippage_for_short': buy_slippage,
                                            "fees": комиссии,
                                            "spread_total": spread_total,
                                            "spread_usdt": спред_юсдт,
                                            "funding_spread": funding_spread,
                                            "курсовой": курсовой,
                                            "funding_long": pos_buy["funding"],
                                            "funding_long_time": pos_buy["funding_time"],
                                            "funding_short": pos_sell["funding"],
                                            "funding_short_time": pos_sell["funding_time"],
                                            "volume": volume,
                                            'total_slippage': total_slippage
                                        })
            лучшие_возможности = {}

            for воз in все_возможности:
                # Ключ уникальный для пары бирж и их типов
                key = (
                    воз['ex_long_id'],
                    воз['ex_long_type'],
                    воз['ex_short_id'],
                    воз['ex_short_type']
                )

                # Если ключа нет или текущий spread_usdt больше
                if key not in лучшие_возможности or воз['spread_usdt'] > лучшие_возможности[key]['spread_usdt']:
                    лучшие_возможности[key] = воз

            # Получаем список лучших возможностей
            все_возможности = list(лучшие_возможности.values())
            
            # Обрабатываем все возможности ОДИН РАЗ за итерацию
            прошедшие_секунды = time.time() - start_time
            minutes = int(прошедшие_секунды // 60)
            sec = int(прошедшие_секунды % 60)
            время_жизни = f'{minutes} минут {sec} секунд'
            
            current_keys = set()
            
            for воз in все_возможности:
                key = f"{воз['symbol']}_{воз['ex_long_id']}_{воз['ex_long_type']}_{воз['ex_short_id']}_{воз['ex_short_type']}_{воз['volume']}"
                current_keys.add(key)
                монеты = воз['volume'] / воз['ex_long']
                
                now = time.time()
                
                # Формируем сообщение
                if воз.get('ex_long_type') == 'futures' and воз.get('ex_short_type') == 'futures':
                    msg = (
                        f"Валютная пара: {воз['symbol']}\n\n"
                        f"Лонг {воз['ex_long_id']} ({воз['ex_long_type']}) {воз['volume']} USDT {монеты:.4f}\n"
                        f"По цене: {воз['ex_long']:.6f}\n"
                        f"Фандинг: {воз['funding_long']:.2f}% Время: {воз['funding_long_time']}\n"
                        f'Slippage {воз['slippage_for_long']}\n\n'
                        f"Шорт {воз['ex_short_id']} ({воз['ex_short_type']}) {воз['volume']} USDT {монеты:.4f}\n"
                        f"По цене: {воз['ex_short']:.6f}\n"
                        f"Фандинг: {воз['funding_short']:.2f}% Время: {воз['funding_short_time']}\n"
                        f"Общий спред: {воз['spread_total']:.2f}% / {воз['spread_usdt']:.2f}$ "
                        f"Курсовой: {воз.get('курсовой'):.2f}% / {(воз.get('volume') * 2) / 100 * воз.get('курсовой'):.2f}$ "
                        f"Фандинговый: {воз.get('funding_spread'):.2f}% / {(воз.get('volume') * 2) / 100 * воз.get('funding_spread'):.2f}$\n"
                        f'Slippage {воз['slippage_for_short']}\n\n'
                        f'TOTAL SLIPPAGE {воз['total_slippage']}'
                        #f"Время жизни: {время_жизни}"
                        
                    )
                else:
                    msg = (
                        f"Валютная пара: {воз['symbol']}\n\n"
                        f"Лонг {воз['ex_long_id']} ({воз['ex_long_type']}) {воз['volume']} USDT {монеты:.4f}\n"
                        f"По цене: {воз['ex_long']:.6f}\n"
                        f'Slippage {воз['slippage_for_long']}\n\n'
                        f"Шорт {воз['ex_short_id']} ({воз['ex_short_type']}) {воз['volume']} USDT {монеты:.4f}\n"
                        f"По цене: {воз['ex_short']:.6f}\n"
                        f"Фандинг: {воз['funding_short']:.2f}% Время: {воз['funding_short_time']}\n"
                        f"Общий спред: {воз['spread_total']:.2f}% / {воз['spread_usdt']:.2f}$ "
                        f"Курсовой: {воз.get('курсовой'):.2f}% / {(воз.get('volume') * 2) / 100 * воз.get('курсовой'):.2f}$ "
                        f"Фандинговый: {воз.get('funding_spread'):.2f}% / {(воз.get('volume') * 2) / 100 * воз.get('funding_spread'):.2f}$\n"
                        f'Slippage {воз['slippage_for_short']}\n\n'
                        f'TOTAL SLIPPAGE {воз['total_slippage']}'
                        #f"Время жизни: {время_жизни}"
                    )
                
                try:

                    # Если сообщение существует И прошло достаточно времени - обновляем
                    if key in id_map:
                        if now - last_update_time.get(key, 0) >= update_interval:
                            #print(msg)
                            
                            await send_message_to_site(
                                msg, 
                                long_price=воз['ex_long'],
                                short_price=воз['ex_short'],
                                spread=воз['курсовой'],
                                message_id=id_map[key],
                                exchange_long=воз['ex_long_id'],
                                exchange_short=воз['ex_short_id'],
                                symbol=воз['symbol'],
                                volume=монеты,
                                exchange_short_type=воз['ex_short_type'],
                                exchange_long_type=воз['ex_long_type']
                            )
                            last_update_time[key] = now
                    else:
                        #print(msg)
                        #new_id = random.randint(1, 23423423)

                        # Создаём новое сообщение
                        new_id = await send_message_to_site(
                            msg,
                            long_price=воз['ex_long'],
                            short_price=воз['ex_short'],
                            spread=воз['курсовой'],
                            exchange_long=воз['ex_long_id'],
                            exchange_short=воз['ex_short_id'],
                            symbol=воз['symbol'],
                            volume=монеты,
                            exchange_short_type=воз['ex_short_type'],
                            exchange_long_type=воз['ex_long_type']
                        )
                        id_map[key] = new_id
                        last_update_time[key] = now
                        
                except Exception as e:
                    print(f"Ошибка в функции арбитража: {e}")
            
            # Удаляем исчезнувшие возможности
            keys_to_remove = set(id_map.keys()) - current_keys
            for key in keys_to_remove:
                try:
                    #if now - last_update_time.get(key, 0) >= update_interval:
                    await delete_message_from_site(message_id=id_map[key])
                    del id_map[key]
                    if key in last_update_time:
                        del last_update_time[key]
                except Exception as e:
                    print(f"Ошибка при удалении сообщения: {e}")
                    
    finally:
        # Очистка при завершении
        for msg_id in id_map.values():
            try:
                await delete_message_from_site(message_id=msg_id)
            except:
                pass

async def стакан():
    # Создаём менеджер подписок
    manager = DynamicSubscriptionManager()
    kucoin = KucoinDynamicWS(manager)
    gateio = GateioDynamicWS(manager)
    htx = HtxDynamicWS(manager)
    okx = OkxDynamicWS(manager)
    # Инициализируем все биржи
    bingx = BingxDynamicWS(manager)
    binance = BinanceDynamicWS(manager)
    mexc = MexcDynamicWS(manager)
    bitget = BitgetDynamicWS(manager)
    bybit_spot = BybitDynamicWS(manager, market="spot", depth=50)
    bybit_linear = BybitDynamicWS(manager, market="linear", depth=50)

    
    # Запускаем все задачи
    tasks = [
        asyncio.create_task(арбитраж_повтор(300, 2000, 100)),
        #asyncio.create_task(on_startup()),
        asyncio.create_task(update_data()),
        # Мониторинг изменений конфига
        asyncio.create_task(manager.monitor_changes(subscriptions_config)),
        #Обновление конфига (симуляция внешнего источника)
        #asyncio.create_task(update_subscriptions_config()),
        
        
        #BingX
        
        asyncio.create_task(bingx.run_spot()),
        asyncio.create_task(bingx.run_futures()),
        asyncio.create_task(bingx._batch_unsubscribe_worker()),
        
        
        # Binance
        asyncio.create_task(binance.run_spot()),
        asyncio.create_task(binance.run_futures()),
        # MEXC
        asyncio.create_task(mexc.run_spot()),
        asyncio.create_task(mexc.run_futures()),
        asyncio.create_task(mexc._batch_unsubscribe_worker()),
        asyncio.create_task(mexc.ping()),
        # Bybit
        asyncio.create_task(bybit_spot.run()),
        asyncio.create_task(bybit_linear.run()),
        # Сохранение orderbook
        # BITGET
        asyncio.create_task(bitget.run_spot()),
        asyncio.create_task(bitget.run_futures()),
        asyncio.create_task(bitget._batch_unsubscribe_worker()),
        asyncio.create_task(bitget.send_ping()),
        #KUCOIN
        asyncio.create_task(kucoin.run_spot()),
        asyncio.create_task(kucoin.run_futures()),
        asyncio.create_task(kucoin._batch_unsubscribe_worker()),
        #GATEIO
        asyncio.create_task(gateio.run_spot()),
        asyncio.create_task(gateio.run_futures()),
        asyncio.create_task(gateio._batch_unsubscribe_worker()),
        #HTX
        asyncio.create_task(htx.run_spot()),
        asyncio.create_task(htx.run_futures()),
        asyncio.create_task(htx._batch_unsubscribe_worker()),
        # #OKX
        asyncio.create_task(okx.run_spot()),
        asyncio.create_task(okx.run_futures()),
        asyncio.create_task(okx._batch_unsubscribe_worker()),
        
        #asyncio.create_task(order()),
    ]

    print("🚀 Starting FULL dynamic WebSocket manager...")


    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        bingx.stop()
        binance.stop()
        mexc.stop()
        bybit_spot.stop()
        bybit_linear.stop()

async def main():
    task_orderbook = asyncio.create_task(стакан())

    config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    task_server = asyncio.create_task(server.serve())
    #check = asyncio.create_task(monitor_memory())

    await asyncio.gather(task_orderbook, task_server)
        
        
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n✅ Graceful shutdown complete")
