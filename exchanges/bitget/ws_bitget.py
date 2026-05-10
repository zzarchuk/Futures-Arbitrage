import asyncio
from collections import defaultdict

from config.config import state_arbitrage
import websockets
import json

from utils.exchange_ws.checker import BaseDynamicWSClient, DynamicSubscriptionManager    
import logging

logger = logging.getLogger(__name__)

class BitgetDynamicWS(BaseDynamicWSClient):

    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("Bitget", manager)
        self.ws = None
        self.ws_futures = None
        self.unsub = defaultdict(set)
        self.lock = asyncio.Lock()
        
    async def send_ping(self):
        while True:
            try:

                await asyncio.sleep(30)
                await self.ws.send("ping")  # текстовое сообщение# type: ignore
                await self.ws_futures.send("ping")# type: ignore
            except Exception:
                continue       
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
                                    "op": "unsubscribe",
                                    "args": [
                                        {
                                            "instType": "SPOT",
                                            "channel": "books15",
                                            "instId": symbol,
                                        }
                                    ],
                                }
                                await self.ws.send(json.dumps(unsub))# type: ignore
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
                                await self.ws_futures.send(json.dumps(unsub))# type: ignore
                            
                            await asyncio.sleep(0.1)
                            
                        except Exception as e:
                            logger.error(f'Bitger WS unsub error: {e}', exc_info=True)
                
                for type, symbols in queue.items():
                    for symbol in symbols:
                        if symbol in state_arbitrage.orderbook_arbitrage:
                            if "bitget" in state_arbitrage.orderbook_arbitrage[symbol]:
                                if type in state_arbitrage.orderbook_arbitrage[symbol]["bitget"]:
                                    del state_arbitrage.orderbook_arbitrage[symbol]["bitget"][type]
                                    if not state_arbitrage.orderbook_arbitrage[symbol]['bitget']:
                                        del state_arbitrage.orderbook_arbitrage[symbol]["bitget"]
                                        if not state_arbitrage.orderbook_arbitrage[symbol]:
                                            del state_arbitrage.orderbook_arbitrage[symbol]


    async def _handle_futures_connection(self):
        url = "wss://ws.bitget.com/v2/ws/public"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
            self.ws_futures = ws
            receive_task = asyncio.create_task(self._process_futures_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "bitget", "futures"
                    )


                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

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
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            logger.error(f'Bitget WS sub error: {symbol} {e}', exc_info=True)
                            raise

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
                    #             if symbol in state_arbitrage.orderbook_arbitrage:
                    #                 if "bitget" in state_arbitrage.orderbook_arbitrage[symbol]:
                    #                     if "futures" in state_arbitrage.orderbook_arbitrage[symbol]["bitget"]:
                    #                         del state_arbitrage.orderbook_arbitrage[symbol]["bitget"]["futures"]
                    #     except Exception as e:
                    #         print(f"❌ Bitget FUTURES unsubscribe error {symbol}: {e}")
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
                if raw_message == 'pong':
                    continue
                
                msg = json.loads(raw_message)

                if msg.get("data"):
                    fund, time = self.manager.find_data('bitget', 'futures', str(msg.get("arg").get("instId"))) # type: ignore
                    if fund != None and time != None:
                        state_arbitrage.orderbook_arbitrage[str(msg.get("arg").get("instId"))]['bitget']['futures']['funding'] = fund
                        state_arbitrage.orderbook_arbitrage[str(msg.get("arg").get("instId"))]['bitget']['futures']['get_funding'] = time
                    state_arbitrage.orderbook_arbitrage[str(msg.get("arg").get("instId"))]["bitget"][
                        "futures"
                    ]["asks"] = [[float(price), float(size)] for price, size in msg['data'][0]['asks']]#msg["data"][0]["asks"]
                    state_arbitrage.orderbook_arbitrage[str(msg.get("arg").get("instId"))]["bitget"][
                        "futures"
                    ]["bids"] = [[float(price), float(size)] for price, size in msg['data'][0]['bids']]#msg["data"][0]["bids"]
            except Exception as e:
                logger.error(f"Bitget WS parse error: {e}", exc_info=True)

    async def _handle_spot_connection(self):
        url = "wss://ws.bitget.com/v2/ws/public"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=10, ping_timeout=60) as ws:
            self.ws = ws

            receive_task = asyncio.create_task(self._process_spot_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "bitget", "spot"
                    )


                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

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
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            logger.error(f"Bitget WS subscribe error {symbol}: {e}", exc_info=True)
                            raise

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
                    #             if symbol in state_arbitrage.orderbook_arbitrage:
                    #                 if "bitget" in state_arbitrage.orderbook_arbitrage[symbol]:
                    #                     if "spot" in state_arbitrage.orderbook_arbitrage[symbol]["bitget"]:
                    #                         del state_arbitrage.orderbook_arbitrage[symbol]["bitget"]["spot"]
                    #     except Exception as e:
                    #         print(f"❌ Bitget spot unsubscribe error {symbol}: {e}")
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
        async for raw_message in ws:
            try:
                if raw_message == 'pong':
                    continue                
                msg = json.loads(raw_message)

                if msg.get("data"):
                    state_arbitrage.orderbook_arbitrage[str(msg.get("arg").get("instId"))]["bitget"]["spot"][
                        "asks"
                    ] = [[float(price), float(size)] for price, size in msg['data'][0]['asks']]#msg["data"][0]["asks"]
                    state_arbitrage.orderbook_arbitrage[str(msg.get("arg").get("instId"))]["bitget"]["spot"][
                        "bids"
                    ] = [[float(price), float(size)] for price, size in msg['data'][0]['bids']]#msg["data"][0]["bids"]
            except Exception as e:
                logger.error(f" Bitget WS parse error: {e}", exc_info=True)

    async def run_spot(self):
        await self._reconnect_wrapper(self._handle_spot_connection, "spot")

    async def run_futures(self):
        await self._reconnect_wrapper(self._handle_futures_connection, "futures")