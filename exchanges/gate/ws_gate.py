import asyncio
from collections import defaultdict
from config.config import state_arbitrage
import websockets
import json


from utils.exchange_ws.checker import BaseDynamicWSClient, DynamicSubscriptionManager    
import logging

logger = logging.getLogger(__name__)


class GateioDynamicWS(BaseDynamicWSClient):
    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("Gate", manager)
        self.ws = None
        self.ws_futures = None
        self.unsub = defaultdict(set)
        self.lock = asyncio.Lock()
        
        
    async def _batch_unsubscribe_worker(self):
        while self.is_running:
            await asyncio.sleep(300)
            
            async with self.lock:
                queue = self.unsub.copy()
                self.unsub.clear()

            if queue:
                
                for market, data in queue.items():
                    for symbol in data:
                        try:
                            if market == "spot":
                                unsub = {
                                    "channel": "spot.order_book",
                                    "event": "unsubscribe",
                                    "payload": [f"{symbol.replace('USDT', '_USDT')}"],
                                }
                                await self.ws.send(json.dumps(unsub))# type: ignore
                            else:
                                unsub = {
                                    "channel": "futures.order_book",
                                    "event": "unsubscribe",
                                    "payload": [f"{symbol.replace('USDT', '_USDT')}"],
                                }
                                await self.ws_futures.send(json.dumps(unsub))# type: ignore
                            
                            await asyncio.sleep(0.05)  # Небольшая задержка между unsub
                            
                        except Exception as e:
                            logger.error(f"Gateio WS unsubscribe error {symbol}: {e}", exc_info=True)
                
                for type, symbols in queue.items():
                    for symbol in symbols:
                        if symbol in state_arbitrage.orderbook_arbitrage:
                            if "gateio" in state_arbitrage.orderbook_arbitrage[symbol]:
                                if type in state_arbitrage.orderbook_arbitrage[symbol]["gateio"]:
                                    del state_arbitrage.orderbook_arbitrage[symbol]["gateio"][type]
                                    if not state_arbitrage.orderbook_arbitrage[symbol]['gateio']:
                                        del state_arbitrage.orderbook_arbitrage[symbol]["gateio"]
                                        if not state_arbitrage.orderbook_arbitrage[symbol]:
                                            del state_arbitrage.orderbook_arbitrage[symbol]


    async def _handle_futures_connection(self):
        url = f"wss://fx-ws.gateio.ws/v4/ws/usdt"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=10, ping_timeout=60) as ws:
            self.ws_futures = ws
            receive_task = asyncio.create_task(self._process_futures_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "gateio", "futures"
                    )


                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

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
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            logger.error(f"Gateio WS FUTURES subscribe error {symbol}: {e}", exc_info=True)
                            raise

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
                    #             if symbol in state_arbitrage.orderbook_arbitrage:
                    #                 if "gateio" in state_arbitrage.orderbook_arbitrage[symbol]:
                    #                     if "futures" in state_arbitrage.orderbook_arbitrage[symbol]["gateio"]:
                    #                         del state_arbitrage.orderbook_arbitrage[symbol]["gateio"]["futures"]
                    #     except Exception as e:
                    #         print(f"❌ gateio FUTURES unsubscribe error {symbol}: {e}")
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
        async for raw_message in ws:
            try:
                msg = json.loads(raw_message)
                result = msg.get("result", {})

                if "asks" in result and "bids" in result:
                    fund, time = self.manager.find_data('gateio', 'futures', msg.get("result").get("contract", "UNKNOWN").replace("_USDT", "USDT")) # type: ignore
                    if fund != None and time != None:
                        state_arbitrage.orderbook_arbitrage[msg.get("result").get("contract", "UNKNOWN").replace("_USDT", "USDT")]['gateio']['futures']['funding'] = fund
                        state_arbitrage.orderbook_arbitrage[msg.get("result").get("contract", "UNKNOWN").replace("_USDT", "USDT")]['gateio']['futures']['get_funding'] = time
                    #async with lock:
                    state_arbitrage.orderbook_arbitrage[msg.get("result").get("contract", "UNKNOWN").replace("_USDT", "USDT")]["gateio"]["futures"]["asks"] = [
                        [float(l["p"]), float(l["s"])] for l in result.get("asks", [])
                    ]
                    state_arbitrage.orderbook_arbitrage[msg.get("result").get("contract", "UNKNOWN").replace("_USDT", "USDT")]["gateio"]["futures"]["bids"] = [
                        [float(l["p"]), float(l["s"])] for l in result.get("bids", [])
                    ]
                    gate_error = result
                elif "status" in result:
                    continue
                else:
                    logger.info(f"Gateio WS FUTURES unknown message: {msg}")

            except Exception as e:
                logger.error(f"Gateio WS FUTURES parse error: {e}\n\n{gate_error}", exc_info=True)

    async def _handle_spot_connection(self):
        url = f"wss://api.gateio.ws/ws/v4/"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=20, ping_timeout=120) as ws:
            self.ws = ws

            receive_task = asyncio.create_task(self._process_spot_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "gateio", "spot"
                    )


                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    
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
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            logger.error(f"Gateio WS spot subscribe error {symbol}: {e}", exc_info=True)
                            raise

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
                    #             if symbol in state_arbitrage.orderbook_arbitrage:
                    #                 if "gateio" in state_arbitrage.orderbook_arbitrage[symbol]:
                    #                     if "spot" in state_arbitrage.orderbook_arbitrage[symbol]["gateio"]:
                    #                         del state_arbitrage.orderbook_arbitrage[symbol]["gateio"]["spot"]
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
        async for raw_message in ws:
            try:
                msg = json.loads(raw_message)

                result = msg.get("result", {})

                if "asks" in result and "bids" in result:
                    state_arbitrage.orderbook_arbitrage[msg.get("result").get("s").replace("_USDT", "USDT")][
                        "gateio"
                    ]["spot"]["asks"] = [
                        [float(price), float(size)]
                        for price, size in msg["result"]["asks"]
                    ]
                    state_arbitrage.orderbook_arbitrage[msg.get("result").get("s").replace("_USDT", "USDT")][
                        "gateio"
                    ]["spot"]["bids"] = [
                        [float(price), float(size)]
                        for price, size in msg["result"]["bids"]
                    ]
                elif "status" in result:
                    continue
                else:
                    logger.info(f"Gateio WS spot unknown message: {msg}")

            except Exception as e:
                logger.error(f"Gateio WS spot parse error: {e}", exc_info=True)

    async def run_spot(self):
        await self._reconnect_wrapper(self._handle_spot_connection, "spot")

    async def run_futures(self):
        await self._reconnect_wrapper(self._handle_futures_connection, "futures")