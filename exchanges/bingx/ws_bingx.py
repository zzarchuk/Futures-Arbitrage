import asyncio
from collections import defaultdict
import gzip

from config.config import state_arbitrage
import websockets
import json

from utils.exchange_ws.checker import BaseDynamicWSClient, DynamicSubscriptionManager    



class BingxDynamicWS(BaseDynamicWSClient):

    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("BingX", manager)
        self.max_per_connection = 100
        self.ws = None
        self.ws_futures = None
        self.unsub = defaultdict(set)
        self.lock = asyncio.Lock()


    async def _batch_unsubscribe_worker(self):
        while self.is_running:
            await asyncio.sleep(90)  # 5 минут
            
            async with self.lock:
                queue = self.unsub.copy()
                self.unsub.clear()

            if queue:
                print(f"BingX processing {len(queue)} unsubscribes")
                
                for market, data in queue.items():
                    for symbol in data:
                        try:
                            if market == "spot":
                                unsub = {
                                    "id": f"{symbol}_spot",
                                    "reqType": "unsub",
                                    "dataType": f"{symbol.replace('USDT', '-USDT')}@depth50",
                                }
                                await self.ws.send(json.dumps(unsub))# type: ignore
                            else:
                                unsub = {
                                    "id": f"{symbol}_futures",
                                    "reqType": "unsub",
                                    "dataType": f"{symbol.replace('USDT', '-USDT')}@depth50@500ms",
                                }
                                await self.ws_futures.send(json.dumps(unsub))# type: ignore
                            
                            await asyncio.sleep(0.05)  # Небольшая задержка между unsub
                            
                        except Exception as e:
                            print(f" Batch bingx unsubscribe error {symbol}: {e}")
                
                for type, symbols in queue.items():
                    for symbol in symbols:
                        if symbol in state_arbitrage.orderbook_arbitrage:
                            if "bingx" in state_arbitrage.orderbook_arbitrage[symbol]:
                                if type in state_arbitrage.orderbook_arbitrage[symbol]["bingx"]:
                                    del state_arbitrage.orderbook_arbitrage[symbol]["bingx"][type]
                                    
                                    if not state_arbitrage.orderbook_arbitrage[symbol]['bingx']:
                                        del state_arbitrage.orderbook_arbitrage[symbol]['bingx']
                                        if not state_arbitrage.orderbook_arbitrage[symbol]:
                                            del state_arbitrage.orderbook_arbitrage[symbol]


    async def _handle_spot_connection(self):
        url = "wss://open-api-ws.bingx.com/market"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
            self.ws = ws

            receive_task = asyncio.create_task(self._process_spot_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "bingx", "spot"
                    )

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    for symbol in to_subscribe:
                        try:
                            sub = {
                                "id": f"{symbol}_spot",
                                "reqType": "sub",
                                "dataType": f"{symbol.replace('USDT', '-USDT')}@depth50",
                            }
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            print(f" Subscribe error {symbol}: {e}")

                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['spot'].add(symbol)



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

                base = symbol.replace("-USDT", "USDT")
                state_arbitrage.orderbook_arbitrage[base]["bingx"]["spot"]["asks"] = [[float(price), float(size)] for price, size in asks[::-1]]
                state_arbitrage.orderbook_arbitrage[base]["bingx"]["spot"]["bids"] = [[float(price), float(size)] for price, size in bids]

            except Exception as e:
                print(f" BingX SPOT parse error: {e}")

    async def _handle_futures_connection(self):
        url = "wss://open-api-swap.bingx.com/swap-market"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
            self.ws_futures = ws

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
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            print(f"Subscribe error {symbol}: {e}")

                    for symbol in to_unsubscribe:
                        current_subscribed.remove(symbol)
                        async with self.lock:
                            self.unsub['futures'].add(symbol)


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

                base = symbol.replace("-USDT", "USDT")
                fund, get = self.manager.find_data('bingx', 'futures', base) # type: ignore
                if fund != None and get != None:
                    state_arbitrage.orderbook_arbitrage[base]["bingx"]["futures"]["funding"] = fund
                    state_arbitrage.orderbook_arbitrage[base]["bingx"]["futures"]["get_funding"] = get
                    
                state_arbitrage.orderbook_arbitrage[base]["bingx"]["futures"]["asks"] = [[float(price), float(size)] for price, size in asks[::-1]]
                state_arbitrage.orderbook_arbitrage[base]["bingx"]["futures"]["bids"] = [[float(price), float(size)] for price, size in bids]

            except Exception as e:
                print(f" BingX FUTURES parse error: {e}")

    async def run_spot(self):
        await self._reconnect_wrapper(self._handle_spot_connection, "spot_0")

    async def run_futures(self):
        await self._reconnect_wrapper(self._handle_futures_connection, "futures_0")