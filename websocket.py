#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os

# Добавляем папку websocket_proto в sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'websocket_proto'))

import asyncio
import websockets
import json
import uuid
import gzip
from collections import defaultdict
import time

import PushDataV3ApiWrapper_pb2 # type: ignore
import PublicLimitDepthsV3Api_pb2 # type: ignore

# объект для десериализации


lock = asyncio.Lock()

orderbook = defaultdict(
    lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
)


class Bingx_ws_all:
    def __init__(self, symbols) -> None:
        self.symbols = symbols
        # self.orderbook = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    async def bingx_orderbook_futures(self):
        """
        Простой WebSocket для стакана BingX (asyncio)

        Args:
            symbol: торговая пара (BTC-USDT, ETH-USDT и т.д.)
            depth: глубина стакана (5, 10, 20, 50, 100)
            update_speed: скорость обновлений (100ms, 500ms, 1000ms)
        """
        url = "wss://open-api-swap.bingx.com/swap-market"

        async with websockets.connect(url) as ws:
            print(f"✅ Подключено к BingX")

            # Подписка на стакан
            for symbol in self.symbols:

                subscription = {
                    "id": f"{symbol}_futures",
                    "reqType": "sub",
                    "dataType": f"{symbol.replace('USDT', '-USDT')}@depth50@500ms",
                }

                await ws.send(json.dumps(subscription))
            print(f"📡 Подписка bingx\n")

            async for message in ws:
                # print(message)

                try:
                    # Распаковка gzip
                    data = gzip.decompress(message).decode("utf-8")  # type: ignore

                    # Ping-Pong
                    if data == "Ping":
                        await ws.send("Pong")
                        continue

                    # JSON → dict
                    orderboo = json.loads(data)

                    # ---- ФИЛЬТРЫ ----
                    if "data" not in orderboo:
                        continue

                    if orderboo["data"] is None:
                        continue

                    if "asks" not in orderboo["data"] or "bids" not in orderboo["data"]:
                        continue
                    # -------------------

                    symbol = orderboo["dataType"].split("@")[0]
                    asks = orderboo["data"]["asks"]
                    bids = orderboo["data"]["bids"]

                    async with lock:
                        base = symbol.replace("-USDT", "USDT")
                        orderbook[base]["bingx"]["futures"]["asks"] = asks[::-1]
                        orderbook[base]["bingx"]["futures"]["bids"] = bids

                except Exception as e:
                    print(f"❌ Ошибка bingx: {e}")

    async def bingx_orderbook_spot(self):
        url = "wss://open-api-ws.bingx.com/market"

        async with websockets.connect(url) as ws:
            print("✅ Подключено к BingX")

            for symbol in self.symbols:
                sub = {
                    "id": f"{symbol}_spot",
                    "reqType": "sub",
                    "dataType": f"{symbol.replace('USDT', '-USDT')}@depth50",
                }
                await ws.send(json.dumps(sub))
            print(f"📡 Подписка bingx spot\n")

            async for message in ws:

                try:
                    # Распаковка gzip
                    data = gzip.decompress(message).decode("utf-8")  # type: ignore

                    # Ping-Pong
                    if data == "ping" or data == "Ping":
                        await ws.send("Pong")
                        continue

                    # Парсим данные
                    orderboo = json.loads(data)

                    # ---- ФИЛЬТРАЦИЯ ----
                    if "data" not in orderboo:
                        continue

                    if orderboo["data"] is None:
                        continue

                    if "asks" not in orderboo["data"] or "bids" not in orderboo["data"]:
                        continue
                    # ---------------------

                    # SYMBOL (например BTC-USDT)
                    symbol = orderboo["dataType"].split("@")[0]

                    asks = orderboo["data"]["asks"]
                    bids = orderboo["data"]["bids"]

                    # Обновление orderbook
                    async with lock:
                        base = symbol.replace("-USDT", "USDT")  # spot не меняет формат
                        orderbook[base]["bingx"]["spot"]["asks"] = asks[::-1]
                        orderbook[base]["bingx"]["spot"]["bids"] = bids

                except Exception as e:
                    print(f"❌ Ошибка bingx spot: {e}")

class Mexc_ws_all:
    def __init__(self, symbols) -> None:
        self.symbols = symbols
        # self.orderbook = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    async def send_ping(self, ws):
        """Фоновая задача: каждые 10 сек отправляет ping"""
        while True:
            try:
                await ws.send(json.dumps({"method": "ping"}))
                print("🔄 Ping sent")
                await asyncio.sleep(10)
            except:
                break

    async def mexc_orderbook_futures(self):

        url = "wss://contract.mexc.com/edge"

        async with websockets.connect(url) as ws:
            print(f"✅ Подключено к MEXC FUTURES")

            asyncio.create_task(self.send_ping(ws))

            # Подписка на стакан
            for symbols in self.symbols:
                symbol = symbols.replace("USDT", "_USDT")
                subscription = {"method": "sub.depth.step", "param": {"symbol": symbol}}
                await ws.send(json.dumps(subscription))
            print(f"📡 Подписки на мекс FUTURES отправленны")

            async for raw_message in ws:
                try:
                    msg = json.loads(raw_message)

                    if "data" not in msg:
                        continue

                    if not msg.get("symbol"):
                        continue

                    symbol = msg.get("symbol").replace("_USDT", "USDT")

                    async with lock:
                        orderbook[symbol]["mexc"]["futures"]["asks"] = msg["data"][
                            "asks"
                        ]
                        orderbook[symbol]["mexc"]["futures"]["bids"] = msg["data"][
                            "bids"
                        ]

                except Exception as e:
                    print(f"❌ Ошибка: {e}\n{raw_message}")

    async def mexc_orderbook_spot(self):
        url = "wss://wbs-api.mexc.com/ws"

        async with websockets.connect(url) as ws:
            print(f"✅ Подключено к MEXC SPOT")

            asyncio.create_task(self.send_ping(ws))

            # Подписка на стакан
            for symbols in self.symbols:
                subscription = {
                    "method": "SUBSCRIPTION",
                    "params": [f"spot@public.limit.depth.v3.api.pb@{symbols}@20"],
                }
                await ws.send(json.dumps(subscription))
            print(f"📡 Подписки на MEXC SPOT отправлены")

            async for raw_message in ws:
                try:
                    # JSON ответы (подтверждения подписок)
                    if isinstance(raw_message, str):
                        continue

                    # Бинарные данные (protobuf)
                    # Шаг 1: Создаём объект wrapper и парсим
                    wrapper = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()  # type: ignore # ← ИСПРАВЛЕНО
                    wrapper.ParseFromString(raw_message)

                    # Шаг 2: Парсим DEPTH из wrapper.publicLimitDepths
                    if wrapper.HasField("publicLimitDepths"):  # ← ИСПРАВЛЕНО
                        depth = (
                            wrapper.publicLimitDepths
                        )  # ← ИСПРАВЛЕНО (depth уже готов)

                        # Конвертируем в списки
                        asks = [[float(l.price), float(l.quantity)] for l in depth.asks]
                        bids = [[float(l.price), float(l.quantity)] for l in depth.bids]

                        # Символ берём из wrapper (он точный!)
                        symbol = wrapper.symbol

                        # Обновляем orderbook
                        async with lock:
                            orderbook[symbol]["mexc"]["spot"]["asks"] = asks
                            orderbook[symbol]["mexc"]["spot"]["bids"] = bids

                except Exception as e:
                    print(f"❌ Ошибка: {e}")
                    import traceback

                    traceback.print_exc()

class Binance_ws_all:
    def __init__(self, symbols) -> None:
        self.symbols = symbols
        # self.orderbook = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    async def send_ping(self, ws):
        while True:
            try:
                await ws.ping()
                print("🔄 Ping sent")
                await asyncio.sleep(60)  # раз в 1 минуту — идеально
            except:
                break

    async def binance_orderbook_futures(self):

        streams = "/".join([
            f"{symbols.lower()}@depth20@100ms"
            for symbols in self.symbols
        ])

        # правильный URL
        url = f"wss://fstream.binance.com/stream?streams={streams}"
        async with websockets.connect(url) as ws:
            print(f"✅ Подключено к binance FUTURES")

            asyncio.create_task(self.send_ping(ws))

            # Подписка на стакан

            print(f"📡 Подписки на binance FUTURES отправленны")

            async for raw_message in ws:
                try:
                    msg = json.loads(raw_message)
                    #print(msg)

                    if "data" not in msg:
                        continue



                    symbol = msg['data']['s']

                    async with lock:
                        orderbook[symbol]["binance"]["futures"]["asks"] = msg["data"][
                            "a"
                        ]
                        orderbook[symbol]["binance"]["futures"]["bids"] = msg["data"][
                            "b"
                        ]

                except Exception as e:
                    print(f"❌ Ошибка: {e}\n{raw_message}")

    async def binance_orderbook_spot(self):

        streams = "/".join([
            f"{symbols.lower()}@depth20@100ms"
            for symbols in self.symbols
        ])

        # правильный URL
        url = f"wss://stream.binance.com:9443/stream?streams={streams}"
        async with websockets.connect(url) as ws:
            print(f"✅ Подключено к binance spot")


            # Подписка на стакан

            print(f"📡 Подписки на binance spot отправленны")

            async for raw_message in ws:
                try:
                    msg = json.loads(raw_message)
                    #print(msg)
                    
                    if not msg.get('stream') or not msg.get('data'):
                        continue
                    
                    
                    symbol = msg['stream'].split("@")[0].upper()



                    async with lock:
                        orderbook[symbol]["binance"]["spot"]["asks"] = msg["data"][
                            "asks"
                        ]
                        orderbook[symbol]["binance"]["spot"]["bids"] = msg["data"][
                            "bids"
                        ]

                except Exception as e:
                    print(f"❌ Ошибка: {e}\n{raw_message}")

class BybitWSManager:
    def __init__(self, symbols, market="spot", depth=50):
        """
        symbols: list of symbols ["BTCUSDT", "ETHUSDT", ...]
        market: "spot" or "linear" (futures)
        depth: orderbook depth (50, 200, etc.)
        """
        self.symbols = symbols
        self.market = market
        self.depth = depth
        self.base_url = {
            "spot": "wss://stream.bybit.com/v5/public/spot",
            "linear": "wss://stream.bybit.com/v5/public/linear",
        }[market]
        self.group_size = 10 if market == "spot" else len(symbols)  # spot: max 10 per sub
        self.groups = [symbols[i:i+self.group_size] for i in range(0, len(symbols), self.group_size)]

    async def send_heartbeat(self, ws, interval=20):
        while True:
            try:
                await ws.send(json.dumps({"req_id": str(time.time()), "op": "ping"}))
            except Exception:
                break
            await asyncio.sleep(interval)

    async def _connect_stream(self, symbols_group):
        while True:  # reconnect loop
            try:
                async with websockets.connect(self.base_url, ping_interval=None) as ws:
                    print(f"✅ Connected: {symbols_group}")
                    asyncio.create_task(self.send_heartbeat(ws))
                    
                    # подписка
                    args = [f"orderbook.{self.depth}.{s}" for s in symbols_group]
                    await ws.send(json.dumps({"op": "subscribe", "args": args, "req_id": str(time.time())}))
                    print(f"📡 Subscribed: {args}")

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except:
                            continue

                        # heartbeat pong
                        if msg.get("op") == "ping":
                            await ws.send(json.dumps({"op": "pong", "req_id": msg.get("req_id")}))
                            continue

                        topic = msg.get("topic")
                        tp = msg.get("type")
                        data = msg.get("data")
                        if not topic or not data:
                            continue

                        symbol = topic.split(".")[-1]
                        if symbol not in self.symbols:
                            continue

                        is_snapshot = tp == "snapshot" or data.get("u") == 1

                        if is_snapshot:
                            # snapshot — сброс локального стакана
                            bids = [[float(p), float(q)] for p, q in data.get("b", [])]
                            asks = [[float(p), float(q)] for p, q in data.get("a", [])]
                            async with lock:
                                orderbook[symbol]['bybit'][self.market]["bids"] = bids
                                orderbook[symbol]['bybit'][self.market]["asks"] = asks
                                print(f"🟢 Snapshot updated: {symbol} ({time.strftime('%X')})")
                        else:
                            # delta — применяем изменения
                            async with lock:
                                for side_key, side_name in [("b", "bids"), ("a", "asks")]:
                                    for price_str, qty_str in data.get(side_key, []):
                                        price = float(price_str)
                                        qty = float(qty_str)

                                        lst = orderbook[symbol]["bybit"][self.market][side_name]
                                        idx = next((i for i, lv in enumerate(lst) if lv[0] == price), None)

                                        if qty == 0:
                                            if idx is not None:
                                                lst.pop(idx)
                                        else:
                                            if idx is not None:
                                                lst[idx][1] = qty
                                            else:
                                                lst.append([price, qty])

                                # сортировка
                                orderbook[symbol]["bybit"][self.market]["bids"].sort(key=lambda x: -x[0])
                                orderbook[symbol]["bybit"][self.market]["asks"].sort(key=lambda x: x[0])
            except Exception as e:
                print(f"❌ Connection error {symbols_group}: {e}")
                print("🔄 Reconnecting in 5 seconds...")
                await asyncio.sleep(5)  # пауза перед reconnect

    async def run(self):
        tasks = [asyncio.create_task(self._connect_stream(group)) for group in self.groups]
        await asyncio.gather(*tasks)

    def get_orderbook(self, symbol):
        return orderbook.get(symbol)
    
    
async def order():
    while True:
        async with lock:
            # Делаем копию, чтобы не держать лок слишком долго
            snapshot = dict(orderbook)
        with open("orderbook.json", "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)

        await asyncio.sleep(5)


async def main():
    # Выбираем 5 монет
    symbols = [
        "BTCUSDT",  # Bitcoin - самая ликвидная
        "ETHUSDT",  # Ethereum - вторая по ликвидности
        # "SOLUSDT",  # Solana - популярная альткоин
        # "DOGEUSDT",  # Dogecoin - мем-монета
        "XRPUSDT",  # Ripple - стабильная
    ]

    # Создаём объекты для BingX и MEXC
    bingx = Bingx_ws_all(symbols)
    mexc = Mexc_ws_all(symbols)
    binance = Binance_ws_all(symbols)
    bybit = BybitWSManager(symbols, market="spot", depth=50)
    bybit_linear = BybitWSManager(symbols, market="linear", depth=50)
    # Создаём задачи для всех потоков
    tasks = [
        # # BingX Futures
        # asyncio.create_task(bingx.bingx_orderbook_futures()),
        # # BingX Spot
        # asyncio.create_task(bingx.bingx_orderbook_spot()),
        # # MEXC Futures
        # asyncio.create_task(mexc.mexc_orderbook_futures()),
        # # MEXC Spot
        # asyncio.create_task(mexc.mexc_orderbook_spot()),
        # # Фоновая задача для логирования orderbook в файл/консоль
        #asyncio.create_task(binance.binance_orderbook_spot()),
        #asyncio.create_task(binance.binance_orderbook_futures()),
        asyncio.create_task(bybit.run()),
        asyncio.create_task(bybit_linear.run()),
        asyncio.create_task(order()),  # твоя функция order()
    ]

    print("🚀 Запуск всех потоков WS для BingX и MEXC (spot + futures)")

    # Ждём, пока все задачи работают
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
