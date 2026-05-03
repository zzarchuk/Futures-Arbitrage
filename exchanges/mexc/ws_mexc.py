import os
import sys
sys.path.insert(0, os.path.abspath("websocket_proto"))

import asyncio
from collections import defaultdict
from config.config import state_arbitrage
import websockets
import json

from utils.exchange_ws.checker import BaseDynamicWSClient, DynamicSubscriptionManager
#from websocket_proto import PushDataV3ApiWrapper_pb2, PublicDealsV3Api_pb2
import PushDataV3ApiWrapper_pb2
import logging

logger = logging.getLogger(__name__)



class MexcDynamicWS(BaseDynamicWSClient):


    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("MEXC", manager)
        self.max_per_connection = 30 
        self.spot_unsubscribe_queue = set()
        self.futures_unsubscribe_queue = set()
        self.unsubscribe_lock = asyncio.Lock()
        self.ws = None
        self.ws_futures = None
        
        self.unsub = defaultdict(set)
        self.lock = asyncio.Lock()
        
    
    async def ping(self):
        while True:
            await asyncio.sleep(15)
            await self.ws.send(json.dumps({'method': 'ping'}))# type: ignore
            await self.ws_futures.send(json.dumps({'method': 'ping'}))# type: ignore
        
    async def _batch_unsubscribe_worker(self):

        while self.is_running:
            await asyncio.sleep(90) 
            
            async with self.lock:
                queue = self.unsub.copy()
                self.unsub.clear()

            if queue:
                
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
                                await self.ws.send(json.dumps(unsub))# type: ignore
                            else:
                                unsub = {
                                    "method": "unsub.depth.step",
                                    "param": {"symbol": symbol.replace('USDT','_USDT')},
                                }
                                await self.ws_futures.send(json.dumps(unsub))# type: ignore
                            
                            #print(f"mexc batch unsubscribed: {symbol}")
                            await asyncio.sleep(0.1)  # Небольшая задержка между unsub
                            
                        except Exception as e:
                            logger.error(f"Batch Mexc WS unsubscribe error {symbol}: {e}", exc_info=True)
                

                for type, symbols in queue.items():
                    for symbol in symbols:
                        if symbol in state_arbitrage.orderbook_arbitrage:
                            if "mexc" in state_arbitrage.orderbook_arbitrage[symbol]:
                                if type in state_arbitrage.orderbook_arbitrage[symbol]["mexc"]:
                                    del state_arbitrage.orderbook_arbitrage[symbol]["mexc"][type]
                                    if not state_arbitrage.orderbook_arbitrage[symbol]['mexc']:
                                        del state_arbitrage.orderbook_arbitrage[symbol]['mexc']
                                        if not state_arbitrage.orderbook_arbitrage[symbol]:
                                            del state_arbitrage.orderbook_arbitrage[symbol]

    async def _handle_spot_connection(self):
        url = "wss://wbs-api.mexc.com/ws"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=10, ping_timeout=60) as ws:
            self.ws = ws

            receive_task = asyncio.create_task(self._process_spot_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "mexc", "spot"
                    )


                    target_symbols = set(list(target_symbols)[:30])

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                 
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
                           
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            logger.error(f"MEXC WS SPOT subscribe error {symbol}: {e}", exc_info=True)

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
                    #             if symbol in state_arbitrage.orderbook_arbitrage:
                    #                 if "mexc" in state_arbitrage.orderbook_arbitrage[symbol]:
                    #                     if "spot" in state_arbitrage.orderbook_arbitrage[symbol]["mexc"]:
                    #                         del state_arbitrage.orderbook_arbitrage[symbol]["mexc"]["spot"]
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
                        state_arbitrage.orderbook_arbitrage[symbol]["mexc"]["spot"]["asks"] = asks
                        state_arbitrage.orderbook_arbitrage[symbol]["mexc"]["spot"]["bids"] = bids

                        state_arbitrage.orderbook_arbitrage[symbol]["mexc"]["spot"]['max_volume'] = vol # type: ignore
                    else:
                        state_arbitrage.orderbook_arbitrage[symbol]["mexc"]["spot"]["asks"] = asks
                        state_arbitrage.orderbook_arbitrage[symbol]["mexc"]["spot"]["bids"] = bids
            except Exception as e:
                logger.error(f"MEXC WS SPOT parse error: {e}", exc_info=True)

    async def _handle_futures_connection(self):
        url = "wss://contract.mexc.com/edge"

        current_subscribed = set()

        async with websockets.connect(url, ping_interval=10, ping_timeout=60) as ws:
            self.ws_futures = ws
            receive_task = asyncio.create_task(self._process_futures_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "mexc", "futures"
                    )

                    target_symbols = set(list(target_symbols)[:30])

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

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
                            logger.error(f"MEXC WS FUTURES subscribe error {symbol}: {e}", exc_info=True)

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
                    #             if symbol in state_arbitrage.orderbook_arbitrage:
                    #                 if "mexc" in state_arbitrage.orderbook_arbitrage[symbol]:
                    #                     if "futures" in state_arbitrage.orderbook_arbitrage[symbol]["mexc"]:
                    #                         del state_arbitrage.orderbook_arbitrage[symbol]["mexc"]["futures"]
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
        async for raw_message in ws:
            try:
                msg = json.loads(raw_message)

                if "data" not in msg or not msg.get("symbol"):
                    continue

                symbol = msg["symbol"].replace("_USDT", "USDT")
                
                fund, get = self.manager.find_data('mexc', 'futures', symbol)# type: ignore
                if fund and get:
                #async with lock:
                    state_arbitrage.orderbook_arbitrage[symbol]["mexc"]["futures"]["asks"] = msg["data"]["asks"]
                    state_arbitrage.orderbook_arbitrage[symbol]["mexc"]["futures"]["bids"] = msg["data"]["bids"]
                    state_arbitrage.orderbook_arbitrage[symbol]["mexc"]["futures"]['funding'] = fund
                    state_arbitrage.orderbook_arbitrage[symbol]["mexc"]["futures"]['get_funding'] = get
                    #state_arbitrage.orderbook_arbitrage[symbol]["mexc"]["futures"]['max_volume'] = vol
                else:
                    state_arbitrage.orderbook_arbitrage[symbol]["mexc"]["futures"]["asks"] = msg["data"]["asks"]
                    state_arbitrage.orderbook_arbitrage[symbol]["mexc"]["futures"]["bids"] = msg["data"]["bids"]
            except Exception as e:
                logger.error(f"MEXC WS FUTURES parse error: {e}", exc_info=True)

    async def run_spot(self):
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
        target_symbols = self.manager.get_symbols_for_exchange("mexc", "futures")
        connections_needed = (len(target_symbols) + 29) // 30

        tasks = [
            asyncio.create_task(
                self._reconnect_wrapper(self._handle_futures_connection, f"futures_{i}")
            )
            for i in range(max(1, connections_needed))
        ]
        
      