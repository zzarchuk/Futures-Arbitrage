import asyncio
from collections import defaultdict
import ctypes

from config.config import state_arbitrage
import websockets
import json
from binascii import crc32

from utils.exchange_ws.checker import BaseDynamicWSClient, DynamicSubscriptionManager

class OkxDynamicWS(BaseDynamicWSClient):
    def __init__(self, manager: DynamicSubscriptionManager):
        super().__init__("Okx", manager)
        
        self.ws = None
        self.ws_futures = None
        # Очереди для накопления отписок
        self.unsub = defaultdict(set)
        self.lock = asyncio.Lock()
        
        
    def _calculate_checksum(self, bids_list, asks_list):
        top_bids = bids_list[:25]
        top_asks = asks_list[:25]

        checksum_parts = []

        max_len = max(len(top_bids), len(top_asks))
        for i in range(max_len):
            if i < len(top_bids):
                p, s = top_bids[i]
                checksum_parts.append(f"{format(p, 'f')}:{format(s, 'f')}")
            if i < len(top_asks):
                p, s = top_asks[i]
                checksum_parts.append(f"{format(p, 'f')}:{format(s, 'f')}")

        checksum_str = ":".join(checksum_parts)

        return ctypes.c_int32(crc32(checksum_str.encode())).value
    
    
    
    
    def update_orderbook(self, symbol, exchange, market_type, data, action, orderbooks):
        ob = orderbooks[symbol][exchange][market_type]
        
        
        if market_type == 'futures':
            fund, time = self.manager.find_data('okx', 'futures', symbol) # type: ignore
            if fund != None and time != None:
                state_arbitrage.orderbook_arbitrage[symbol]['okx']['futures']['funding'] = fund
                state_arbitrage.orderbook_arbitrage[symbol]['okx']['futures']['get_funding'] = time
                
                
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


        if action == 'snapshot':
            if prev_seq != -1:
                print(f"Некорректный snapshot: prevSeqId должен быть -1, получено {prev_seq}")
            ob['bids'] = [[float(b[0]), float(b[1])] for b in data.get('bids', [])]
            ob['asks'] = [[float(a[0]), float(a[1])] for a in data.get('asks', [])]
        else:

            if ob['seqId'] is not None:
                if prev_seq != ob['seqId']:
                    if seq < prev_seq:
                        print(f"Обнаружен reset последовательности: prevSeqId={prev_seq}, seqId={seq}")
                    elif prev_seq == seq:
                        print(f"Heartbeat сообщение: seqId={seq}")
                        return
                    else:
                        print(f"Пропуск сообщения! Ожидается prevSeqId={ob['seqId']}, получено {prev_seq}")
                        return
            
            bids_dict = {b[0]: b[1] for b in ob['bids']}
            for bid in data.get('bids', []):
                price, size = float(bid[0]), float(bid[1])
                if size == 0:
                    bids_dict.pop(price, None)
                else:
                    bids_dict[price] = size
            ob['bids'] = sorted([[p, s] for p, s in bids_dict.items()], key=lambda x: x[0], reverse=True)

            asks_dict = {a[0]: a[1] for a in ob['asks']}
            for ask in data.get('asks', []):
                price, size = float(ask[0]), float(ask[1])
                if size == 0:
                    asks_dict.pop(price, None)
                else:
                    asks_dict[price] = size
            ob['asks'] = sorted([[p, s] for p, s in asks_dict.items()], key=lambda x: x[0])

        ob['prevSeqId'] = prev_seq
        ob['seqId'] = seq
        

    async def _batch_unsubscribe_worker(self):
        """Воркер для batch-отписок раз в 5 минут"""
        while self.is_running:
            await asyncio.sleep(90)  # 5 минут
            
            async with self.lock:
                queue = self.unsub.copy()
                self.unsub.clear()

            if queue:
                print(f"🗑️ okx processing {len(queue)} unsubscribes")
                
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
                                await self.ws.send(json.dumps(unsub))# type: ignore
                            else:
                                unsub = {
                                    "op": "unsubscribe",
                                    "args": [
                                        {"channel": "books", "instId": f'{symbol.replace('USDT', '-USDT-SWAP')}'}
                                    ],
                                }
                                await self.ws_futures.send(json.dumps(unsub)) # type: ignore
                            
                            await asyncio.sleep(0.05)  # Небольшая задержка между unsub
                            
                        except Exception as e:
                            print(f"Batch okx unsubscribe error {symbol}: {e}")
                
                #async with lock:
                for type, symbols in queue.items():
                    for symbol in symbols:
                        if symbol in state_arbitrage.orderbook_arbitrage:
                            if "okx" in state_arbitrage.orderbook_arbitrage[symbol]:
                                if type in state_arbitrage.orderbook_arbitrage[symbol]["okx"]:
                                    del state_arbitrage.orderbook_arbitrage[symbol]["okx"][type]

    async def _handle_futures_connection(self):

        url = f"wss://wspap.okx.com:8443/ws/v5/public"

        current_subscribed = set()

        async with websockets.connect(url) as ws:
            self.ws_futures = ws

            receive_task = asyncio.create_task(self._process_futures_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "okx", "futures"
                    )


                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

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
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            print(f"okx FUTURES subscribe error {symbol}: {e}")

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
                    #             if symbol in state_arbitrage.orderbook_arbitrage:
                    #                 if "okx" in state_arbitrage.orderbook_arbitrage[symbol]:
                    #                     if "futures" in state_arbitrage.orderbook_arbitrage[symbol]["okx"]:
                    #                         del state_arbitrage.orderbook_arbitrage[symbol]["okx"]["futures"]
                    #     except Exception as e:
                    #         print(f"❌ okx FUTURES unsubscribe error {symbol}: {e}")
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
        async for raw in ws:
            try:
                msg = json.loads(raw)
                if msg.get("op") == "ping":
                    await ws.send(json.dumps({"op": "pong"}))
                    continue

                if msg.get("event") == "ping":
                    await ws.send(json.dumps({"event": "pong"}))
                    continue
                if 'event' in msg:
                    continue
                
                if 'data' not in msg:
                    continue
                
                action = msg.get('action')  # 'snapshot' или 'update'
                arg = msg.get('arg', {})
                inst_id = arg.get('instId', 'UNKNOWN')
                
                for data in msg['data']:

                    
                    self.update_orderbook(
                        symbol=inst_id.replace('-USDT-SWAP', 'USDT'),
                        exchange='okx',
                        market_type='futures',
                        data=data,
                        action=action,
                        orderbooks=state_arbitrage.orderbook_arbitrage
                    )
                    
                      
            except Exception as e:
                print(f"Ошибка: {e}")
                import traceback
                traceback.print_exc()
                                
    async def _handle_spot_connection(self):

        url = f"wss://wspap.okx.com:8443/ws/v5/public"

        current_subscribed = set()

        async with websockets.connect(url) as ws:
            self.ws = ws

            receive_task = asyncio.create_task(self._process_spot_messages(ws))

            try:
                while self.is_running:
                    target_symbols = self.manager.get_symbols_for_exchange(
                        "okx", "spot"
                    )


                    to_subscribe = target_symbols - current_subscribed
                    to_unsubscribe = current_subscribed - target_symbols

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
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            print(f"okx spot subscribe error {symbol}: {e}")

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
                    #             if symbol in state_arbitrage.orderbook_arbitrage:
                    #                 if "okx" in state_arbitrage.orderbook_arbitrage[symbol]:
                    #                     if "spot" in state_arbitrage.orderbook_arbitrage[symbol]["okx"]:
                    #                         del state_arbitrage.orderbook_arbitrage[symbol]["okx"]["spot"]
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
                if 'event' in msg:
                    continue
                
                if 'data' not in msg:
                    continue
                
                action = msg.get('action')  # 'snapshot' или 'update'
                arg = msg.get('arg', {})
                inst_id = arg.get('instId', 'UNKNOWN')
                
                for data in msg['data']:

                    
                    #async with lock:
                    self.update_orderbook(
                        symbol=inst_id.replace('-USDT', 'USDT'),
                        exchange='okx',
                        market_type='spot',
                        data=data,
                        action=action,
                        orderbooks=state_arbitrage.orderbook_arbitrage
                    )
                    
                      
            except Exception as e:
                print(f"Ошибка: {e}")
                import traceback
                traceback.print_exc()
    
    async def run_spot(self):
        await self._reconnect_wrapper(self._handle_spot_connection, "spot")

    async def run_futures(self):
        await self._reconnect_wrapper(self._handle_futures_connection, "futures")