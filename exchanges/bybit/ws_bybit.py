import asyncio

import aiohttp
from config.config import state_arbitrage
import websockets
import json
import time

from utils.exchange_ws.checker import BaseDynamicWSClient, DynamicSubscriptionManager    
import logging

logger = logging.getLogger(__name__)


class BybitDynamicWS(BaseDynamicWSClient):

    def __init__(self, manager: DynamicSubscriptionManager, market="spot", depth=50):
        super().__init__(f"Bybit-{market}", manager)
        self.market = market
        self.depth = depth
        self.base_url = {
            "spot": "wss://stream.bybit.com/v5/public/spot",
            "linear": "wss://stream.bybit.com/v5/public/linear",
        }[market]
        self.max_per_subscription = 10 if market == "spot" else 50

    async def _handle_connection(self):
        current_subscribed = set()

        async with websockets.connect(
            self.base_url, ping_interval=20, ping_timeout=60
        ) as ws:

            receive_task = asyncio.create_task(self._process_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "bybit", self.market
                    )

                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

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
                                await asyncio.sleep(0.1)
                            except Exception as e:
                                logger.error(f"Bybit {self.market} subscribe error: {e}", exc_info=True)

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
                                for symbol in batch:
                                    if symbol in state_arbitrage.orderbook_arbitrage:
                                        if "bybit" in state_arbitrage.orderbook_arbitrage[symbol]:
                                            if (
                                                self.market
                                                in state_arbitrage.orderbook_arbitrage[symbol]["bybit"]
                                            ):
                                                del state_arbitrage.orderbook_arbitrage[symbol]["bybit"][
                                                    self.market
                                                ]
                                await asyncio.sleep(0.1)
                            except Exception as e:
                                logger.error(f" Bybit {self.market} unsubscribe error: {e}", exc_info=True)

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
        async for raw in ws:
            try:
                msg = json.loads(raw)

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
                        
                        state_arbitrage.orderbook_arbitrage[symbol]["bybit"][self.market]["bids"] = bids
                        state_arbitrage.orderbook_arbitrage[symbol]["bybit"][self.market]["asks"] = asks # type: ignore
                    else:
                        fund, get = self.manager.find_data('bybit', 'futures', symbol) # type: ignore
                        if fund != None and get != None:
                            state_arbitrage.orderbook_arbitrage[symbol]["bybit"][self.market]["bids"] = bids
                            state_arbitrage.orderbook_arbitrage[symbol]["bybit"][self.market]["asks"] = asks
                            #orderbook[symbol]["bybit"][self.market]["max_volume"] = vol
                            state_arbitrage.orderbook_arbitrage[symbol]["bybit"][self.market]["funding"] = fund # type: ignore
                            state_arbitrage.orderbook_arbitrage[symbol]["bybit"][self.market]["get_funding"] = get # type: ignore
                        else:
                            state_arbitrage.orderbook_arbitrage[symbol]["bybit"][self.market]["bids"] = bids
                            state_arbitrage.orderbook_arbitrage[symbol]["bybit"][self.market]["asks"] = asks

                else:
                    for side_key, side_name in [("b", "bids"), ("a", "asks")]:
                        for price_str, qty_str in data.get(side_key, []):
                            price = float(price_str)
                            qty = float(qty_str)

                            lst = state_arbitrage.orderbook_arbitrage[symbol]["bybit"][self.market][side_name]
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

                    if self.market == 'futures':
                        fund, get = self.manager.find_data('bybit', 'futures', symbol) # type: ignore
                        if fund != None and get != None:
                            state_arbitrage.orderbook_arbitrage[symbol]['bybit'][self.market if self.market == 'futures' else 'spot']['funding'] = fund
                            state_arbitrage.orderbook_arbitrage[symbol]['bybit'][self.market if self.market == 'futures' else 'spot']['get_funding'] = get
                    state_arbitrage.orderbook_arbitrage[symbol]["bybit"][
                        self.market if self.market == "spot" else "futures"
                    ]["bids"].sort(key=lambda x: -x[0])
                    state_arbitrage.orderbook_arbitrage[symbol]["bybit"][
                        self.market if self.market == "spot" else "futures"
                    ]["asks"].sort(key=lambda x: x[0])

            except Exception as e:
                logger.error(f"Bybit {self.market} parse error: {e}", exc_info=True)

    async def run(self):
        await self._reconnect_wrapper(self._handle_connection, "main")