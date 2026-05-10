import asyncio
from collections import defaultdict

import aiohttp
from config.config import state_arbitrage
import websockets
import json
import time

from utils.exchange_ws.checker import BaseDynamicWSClient, DynamicSubscriptionManager         
import logging

logger = logging.getLogger(__name__)
         
         
class KucoinDynamicWS(BaseDynamicWSClient):
    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("Kucoin", manager)
        self.ws = None
        self.ws_futures = None
        self.unsub = defaultdict(set)
        self.lock = asyncio.Lock()
        
        
    async def _batch_unsubscribe_worker(self):
        while self.is_running:
            await asyncio.sleep(90)
            
            async with self.lock:
                queue = self.unsub.copy()
                self.unsub.clear()

            if queue:
                                
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
                                await self.ws.send(json.dumps(unsub)) # type: ignore
                            else:
                                unsub = {
                                    "id": int(time.time() * 1000),
                                    "type": "unsubscribe",
                                    "topic": f"/contractMarket/level2Depth50:{symbol}M",
                                    "response": True,
                                }
                                await self.ws_futures.send(json.dumps(unsub)) # type: ignore
                            
                            await asyncio.sleep(0.1)
                            
                        except Exception as e:
                            logger.error(f"Batch Kucoin WS unsubscribe error {symbol}: {e}")
                
                #async with lock:
                for type, symbols in queue.items():
                    for symbol in symbols:
                        if symbol in state_arbitrage.orderbook_arbitrage:
                            if "kucoin" in state_arbitrage.orderbook_arbitrage[symbol]:
                                if type in state_arbitrage.orderbook_arbitrage[symbol]["kucoin"]:
                                    del state_arbitrage.orderbook_arbitrage[symbol]["kucoin"][type]
                                    if not state_arbitrage.orderbook_arbitrage[symbol]['kucoin']:
                                        del state_arbitrage.orderbook_arbitrage[symbol]["kucoin"]
                                        if not state_arbitrage.orderbook_arbitrage[symbol]:
                                            del state_arbitrage.orderbook_arbitrage[symbol]
                                                
    async def _handle_futures_connection(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api-futures.kucoin.com/api/v1/bullet-public"
            ) as r:
                data = await r.json()
                token = data["data"]["token"]

        url = f"wss://ws-api-futures.kucoin.com?token={token}"

        current_subscribed = set()

        async with websockets.connect(url) as ws:
            self.ws_futures = ws
            receive_task = asyncio.create_task(self._process_futures_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "kucoin", "futures"
                    )


                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    
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
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            logger.error(f"Kucoin WS FUTURES subscribe error {symbol}: {e}", exc_info=True)
                            raise

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
                    #             if symbol in state_arbitrage.orderbook_arbitrage:
                    #                 if "kucoin" in state_arbitrage.orderbook_arbitrage[symbol]:
                    #                     if "futures" in state_arbitrage.orderbook_arbitrage[symbol]["kucoin"]:
                    #                         del state_arbitrage.orderbook_arbitrage[symbol]["kucoin"]["futures"]
                    #     except Exception as e:
                    #         print(f"❌ Kucoin FUTURES unsubscribe error {symbol}: {e}")
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
                try:
                    await receive_task
                except asyncio.CancelledError:
                    pass

    async def _process_futures_messages(self, ws):
        async for raw_message in ws:
            try:
                msg = json.loads(raw_message)

                if msg.get("type") == "ping":
                    await ws.send(json.dumps({"type": "pong", "id": msg["id"]}))
                    continue

                if msg.get("data") and msg.get("topic"):

                    topic = msg["topic"]

                    if ":" not in topic:
                        continue

                    full_symbol = topic.split(":", 1)[1]

                    symbol = (
                        full_symbol[:-1] if full_symbol.endswith("M") else full_symbol
                    )

                    fund, time = self.manager.find_data('kucoin', 'futures', symbol) # type: ignore
                    if fund != None and time != None:
                        state_arbitrage.orderbook_arbitrage[symbol]['kucoin']['futures']['funding'] = fund
                        state_arbitrage.orderbook_arbitrage[symbol]['kucoin']['futures']['get_funding'] = time
                    state_arbitrage.orderbook_arbitrage[symbol]["kucoin"]["futures"]["asks"] = [
                        [float(price), float(size)]
                        for price, size in msg["data"].get("asks", [])
                    ]
                    state_arbitrage.orderbook_arbitrage[symbol]["kucoin"]["futures"]["bids"] = [
                        [float(price), float(size)]
                        for price, size in msg["data"].get("bids", [])
                    ]

            except Exception as e:
                logger.error(f"Kucoin FUTURES WS parse error: {e}", exc_info=True)

    async def _handle_spot_connection(self):
        async with aiohttp.ClientSession() as session:
            async with session.post("https://api.kucoin.com/api/v1/bullet-public") as r:
                data = await r.json()
                token = data["data"]["token"]

        url = f"wss://ws-api-spot.kucoin.com?token={token}"

        current_subscribed = set()

        async with websockets.connect(url) as ws:
            self.ws = ws
            receive_task = asyncio.create_task(self._process_spot_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "kucoin", "spot"
                    )

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

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
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            logger.error(f"Kucoin WS spot subscribe error {symbol}: {e}", exc_info=True)
                            raise

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
                    #             if symbol in state_arbitrage.orderbook_arbitrage:
                    #                 if "kucoin" in state_arbitrage.orderbook_arbitrage[symbol]:
                    #                     if "spot" in state_arbitrage.orderbook_arbitrage[symbol]["kucoin"]:
                    #                         del state_arbitrage.orderbook_arbitrage[symbol]["kucoin"]["spot"]
                    #     except Exception as e:
                    #         print(f"❌ Kucoin spot unsubscribe error {symbol}: {e}")
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
                try:
                    await receive_task
                except asyncio.CancelledError:
                    pass

    async def _process_spot_messages(self, ws):
        async for raw_message in ws:
            try:
                msg = json.loads(raw_message)

                if msg.get("type") == "ping":
                    await ws.send(json.dumps({"type": "pong", "id": msg["id"]}))
                    continue

                if msg.get("data") and msg.get("topic"):

                    topic = msg["topic"]

                    if ":" not in topic:
                        continue

                    full_symbol = topic.split(":", 1)[1]

                    symbol = full_symbol.replace("-USDT", "USDT")


                    state_arbitrage.orderbook_arbitrage[symbol]["kucoin"]["spot"]["asks"] = [
                        [float(price), float(size)]
                        for price, size in msg["data"].get("asks", [])
                    ]
                    state_arbitrage.orderbook_arbitrage[symbol]["kucoin"]["spot"]["bids"] = [
                        [float(price), float(size)]
                        for price, size in msg["data"].get("bids", [])
                    ]

            except Exception as e:
                logger.error(f"Kucoin WS spot parse error: {e}", exc_info=True)

    async def run_spot(self):
        await self._reconnect_wrapper(self._handle_spot_connection, "spot")

    async def run_futures(self):
        await self._reconnect_wrapper(self._handle_futures_connection, "futures")