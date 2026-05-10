import asyncio
from config.config import state_arbitrage
import websockets
import json


from utils.exchange_ws.checker import BaseDynamicWSClient, DynamicSubscriptionManager    

import logging

logger = logging.getLogger(__name__)

class BinanceDynamicWS(BaseDynamicWSClient):

    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("Binance", manager)
        self.spot_url_base = "wss://stream.binance.com:9443/stream?streams="
        self.futures_url_base = "wss://fstream.binance.com/stream?streams="

    async def _handle_spot_connection(self):
        ws = None

        while self.is_running:
            try:
                target_symbols = self.manager.get_symbols_for_exchange(
                    "binance", "spot"
                )
                if not target_symbols:
                    await asyncio.sleep(1)
                    continue

                streams = "/".join(
                    [f"{s.lower()}@depth20@100ms" for s in target_symbols]
                )
                url = f"{self.spot_url_base}{streams}"

                async with websockets.connect(
                    url, ping_interval=60, ping_timeout=180
                ) as ws:

                    async for raw_message in ws:
                        try:
                            msg = json.loads(raw_message)

                            if "stream" not in msg or "data" not in msg:
                                continue

                            symbol = msg["stream"].split("@")[0].upper()
                            data = msg["data"]

                            asks = [[float(p), float(q)] for p, q in data["asks"]]
                            bids = [[float(p), float(q)] for p, q in data["bids"]]

                            state_arbitrage.orderbook_arbitrage[symbol]["binance"]["spot"]["asks"] = asks
                            state_arbitrage.orderbook_arbitrage[symbol]["binance"]["spot"]["bids"] = bids

                        except Exception as e:
                            logger.error(f"Binance spot WS: {e}", exc_info=True)

            except Exception as e:
                logger.error(f"Binance spot WS reconect: {e}", exc_info=True)
                
                await asyncio.sleep(5)
                raise

    async def _handle_futures_connection(self):

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
                                state_arbitrage.orderbook_arbitrage[symbol]["binance"]["futures"]["asks"] = asks
                                state_arbitrage.orderbook_arbitrage[symbol]["binance"]["futures"]["bids"] = bids
                                state_arbitrage.orderbook_arbitrage[symbol]["binance"]["futures"]['funding'] = fund
                                state_arbitrage.orderbook_arbitrage[symbol]["binance"]["futures"]['get_funding'] = get
                                #state_arbitrage.orderbook_arbitrage[symbol]["mexc"]["futures"]['max_volume'] = vol
                            else:
                                state_arbitrage.orderbook_arbitrage[symbol]["binance"]["futures"]["asks"] = asks
                                state_arbitrage.orderbook_arbitrage[symbol]["binance"]["futures"]["bids"] = bids
                            #async with lock:
                            # state_arbitrage.orderbook_arbitrage[symbol]["binance"]["futures"]["asks"] = asks
                            # state_arbitrage.orderbook_arbitrage[symbol]["binance"]["futures"]["bids"] = bids

                        except Exception as e:
                            logger.error(f"Binance futures WS: {e}", exc_info=True)

            except Exception as e:
                logger.error(f"Binance futures WS reconect: {e}", exc_info=True)
                await asyncio.sleep(5)
                raise

    async def run_spot(self):
        await self._reconnect_wrapper(self._handle_spot_connection, "spot")

    async def run_futures(self):
        await self._reconnect_wrapper(self._handle_futures_connection, "futures")
