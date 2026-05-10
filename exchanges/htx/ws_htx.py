import asyncio
from collections import defaultdict
import gzip

from config.config import state_arbitrage
import websockets
import json

from utils.exchange_ws.checker import BaseDynamicWSClient, DynamicSubscriptionManager    
import logging

logger = logging.getLogger(__name__)

class HtxDynamicWS(BaseDynamicWSClient):
    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("Htx", manager)
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
                
                for market, data in queue.items():
                    for symbol in data:
                        try:
                            if market == "spot":
                                unsub = {"unsub": f"market.{symbol.lower()}.depth.step0"}
                                await self.ws.send(json.dumps(unsub))# type: ignore
                            else:
                                unsub = {
                                    "unsub": f"market.{symbol.replace('USDT', '-USDT')}.depth.step0"
                                }
                                await self.ws_futures.send(json.dumps(unsub))# type: ignore
                            
                            await asyncio.sleep(0.3)
                            
                        except Exception as e:
                            logger.error(f"Htx WS unsubscribe error {symbol}: {e}", exc_info=True)
                
                #async with lock:
                for type, symbols in queue.items():
                    for symbol in symbols:
                        if symbol in state_arbitrage.orderbook_arbitrage:
                            if "htx" in state_arbitrage.orderbook_arbitrage[symbol]:
                                if type in state_arbitrage.orderbook_arbitrage[symbol]["htx"]:
                                    del state_arbitrage.orderbook_arbitrage[symbol]["htx"][type]
                                    if not state_arbitrage.orderbook_arbitrage[symbol]['htx']:
                                        del state_arbitrage.orderbook_arbitrage[symbol]["htx"]
                                        if not state_arbitrage.orderbook_arbitrage[symbol]:
                                            del state_arbitrage.orderbook_arbitrage[symbol]        

    
    
    async def _handle_futures_connection(self):

        url = f"wss://api.hbdm.vn/linear-swap-ws"

        current_subscribed = set()

        async with websockets.connect(url) as ws:
            self.ws_futures = ws

            receive_task = asyncio.create_task(self._process_futures_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "htx", "futures"
                    )


                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    for symbol in to_subscribe:
                        try:

                            sub = {
                                "sub": f"market.{symbol.replace('USDT', '-USDT')}.depth.step0"
                            }
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            logger.error(f"Htx WS FUTURES subscribe error {symbol}: {e}", exc_info=True)
                            raise

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
                    #             if symbol in state_arbitrage.orderbook_arbitrage:
                    #                 if "htx" in state_arbitrage.orderbook_arbitrage[symbol]:
                    #                     if "futures" in state_arbitrage.orderbook_arbitrage[symbol]["htx"]:
                    #                         del state_arbitrage.orderbook_arbitrage[symbol]["htx"]["futures"]
                    #     except Exception as e:
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
                        state_arbitrage.orderbook_arbitrage[msg.get("ch").split(".")[1].replace("-USDT", "USDT")]['htx']['futures']['funding'] = fund
                        state_arbitrage.orderbook_arbitrage[msg.get("ch").split(".")[1].replace("-USDT", "USDT")]['htx']['futures']['get_funding'] = time
                    state_arbitrage.orderbook_arbitrage[msg.get("ch").split(".")[1].replace("-USDT", "USDT")][
                        "htx"
                    ]["futures"]["asks"] = msg["tick"]["asks"]
                    state_arbitrage.orderbook_arbitrage[msg.get("ch").split(".")[1].replace("-USDT", "USDT")][
                        "htx"
                    ]["futures"]["bids"] = msg["tick"]["bids"]

            except Exception as e:
                logger.error(f"Htx WS FUTURES parse error: {e}", exc_info=True)

    async def _handle_spot_connection(self):

        url = f"wss://api-aws.huobi.pro/ws"

        current_subscribed = set()

        async with websockets.connect(url) as ws:
            self.ws = ws
            receive_task = asyncio.create_task(self._process_spot_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "htx", "spot"
                    )


                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

                    for symbol in to_subscribe:
                        try:

                            sub = {"sub": [f"market.{symbol.lower()}.depth.step0"]}
                            await ws.send(json.dumps(sub))
                            current_subscribed.add(symbol)
                            await asyncio.sleep(0.2)
                        except Exception as e:
                            logger.error(f"Htx WS spot subscribe error {symbol}: {e}", exc_info=True)
                            raise

                    # Отписываемся
                    # for symbol in to_unsubscribe:
                    #     try:

                    #         unsub = {"unsub": f"market.{symbol.lower()}.depth.step0"}
                    #         await ws.send(json.dumps(unsub))
                    #         current_subscribed.remove(symbol)
                    #         print(f"➖ htx spot unsubscribed: {symbol}")

                    #         async with lock:
                    #             if symbol in state_arbitrage.orderbook_arbitrage:
                    #                 if "htx" in state_arbitrage.orderbook_arbitrage[symbol]:
                    #                     if "spot" in state_arbitrage.orderbook_arbitrage[symbol]["htx"]:
                    #                         del state_arbitrage.orderbook_arbitrage[symbol]["htx"]["spot"]
                    #     except Exception as e:
                    #         print(f"❌ htx spot unsubscribe error {symbol}: {e}")
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
                decompressed = gzip.decompress(raw_message)
                msg = json.loads(decompressed.decode("utf-8"))

                if "ping" in msg:
                    await ws.send(json.dumps({"pong": msg["ping"]}))
                    continue

                if "status" in msg:
                    continue

                
                if msg['tick']['bids'] and msg['tick']['asks']:
                    state_arbitrage.orderbook_arbitrage[
                        msg.get("ch").split(".")[1].replace("-USDT", "USDT").upper()
                    ]["htx"]["spot"]["asks"] = msg["tick"]["asks"]
                    state_arbitrage.orderbook_arbitrage[
                        msg.get("ch").split(".")[1].replace("-USDT", "USDT").upper()
                    ]["htx"]["spot"]["bids"] = msg["tick"]["bids"]

            except Exception as e:
                logger.error(f"Htx WS FUTURES parse error: {e}", exc_info=True)

    async def run_spot(self):
        await self._reconnect_wrapper(self._handle_spot_connection, "spot")

    async def run_futures(self):
        await self._reconnect_wrapper(self._handle_futures_connection, "futures")