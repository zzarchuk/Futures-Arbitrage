from exchanges.binance.ws_binance import BinanceDynamicWS
from exchanges.bingx.ws_bingx import BingxDynamicWS
from exchanges.bitget.ws_bitget import BitgetDynamicWS
from exchanges.bybit.ws_bybit import BybitDynamicWS
from exchanges.gate.ws_gate import GateioDynamicWS
from exchanges.htx.ws_htx import HtxDynamicWS
from exchanges.kucoin.ws_kucoin import KucoinDynamicWS
from exchanges.mexc.ws_mexc import MexcDynamicWS
from exchanges.okx.ws_okx import OkxDynamicWS
from utils.exchange_ws.checker import DynamicSubscriptionManager
from utils.parser_func.parsers import арбитраж_повтор



#from config.config import orderbook, subscriptions_config, subscriptions_lock
from config.config import state_filter, state_arbitrage
import asyncio
import json



from webb import app
import uvicorn
from run_filter.run_filter import run_filter


async def update_data():
    while True:
        valid_tokens_with_spread = await run_filter()
        #print(valid_tokens_with_spread)
        async with state_filter.lock_filter:
            state_filter.valid_tokens_filter.clear()
            state_filter.valid_tokens_filter.update(valid_tokens_with_spread)
        await asyncio.sleep(3)
   

        
        
async def order():
    """Сохранение orderbook"""
    #last_save = time.time()

    while True:
        await asyncio.sleep(0.5)

        #if time.time() - last_save >= 5:
        try:
            #async with lock:
            snapshot = dict(state_arbitrage.orderbook_arbitrage)

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: json.dump(snapshot, open("orderbook.json", "w"), indent=2),
            )
            #last_save = time.time()
            #print(f"💾 Orderbook saved: {len(snapshot)} symbols")
        except Exception as e:
            print(f"❌ Save error: {e}")


async def стакан():
    # Создаём менеджер подписок
    manager = DynamicSubscriptionManager()
    kucoin = KucoinDynamicWS(manager)
    gateio = GateioDynamicWS(manager)
    htx = HtxDynamicWS(manager)
    okx = OkxDynamicWS(manager)
    # Инициализируем все биржи
    bingx = BingxDynamicWS(manager)
    binance = BinanceDynamicWS(manager)
    mexc = MexcDynamicWS(manager)
    bitget = BitgetDynamicWS(manager)
    bybit_spot = BybitDynamicWS(manager, market="spot", depth=50)
    bybit_linear = BybitDynamicWS(manager, market="linear", depth=50)
    

    
    # Запускаем все задачи
    tasks = [
        asyncio.create_task(арбитраж_повтор(60, 61, 50)),
        #asyncio.create_task(on_startup()),
        asyncio.create_task(update_data()),
        # Мониторинг изменений конфига
        asyncio.create_task(manager.monitor_changes(state_filter.valid_tokens_filter)),
        #Обновление конфига (симуляция внешнего источника)
        #asyncio.create_task(update_subscriptions_config()),
        
        
        
        #BingX
        
        asyncio.create_task(bingx.run_spot()),
        asyncio.create_task(bingx.run_futures()),
        asyncio.create_task(bingx._batch_unsubscribe_worker()),
        
        
        # Binance
        asyncio.create_task(binance.run_spot()),
        asyncio.create_task(binance.run_futures()),
        # MEXC
        asyncio.create_task(mexc.run_spot()),
        asyncio.create_task(mexc.run_futures()),
        asyncio.create_task(mexc._batch_unsubscribe_worker()),
        asyncio.create_task(mexc.ping()),
        # Bybit
        asyncio.create_task(bybit_spot.run()),
        asyncio.create_task(bybit_linear.run()),
        # Сохранение orderbook
        # BITGET
        asyncio.create_task(bitget.run_spot()),
        asyncio.create_task(bitget.run_futures()),
        asyncio.create_task(bitget._batch_unsubscribe_worker()),
        asyncio.create_task(bitget.send_ping()),
        #KUCOIN
        asyncio.create_task(kucoin.run_spot()),
        asyncio.create_task(kucoin.run_futures()),
        asyncio.create_task(kucoin._batch_unsubscribe_worker()),
        #GATEIO
        asyncio.create_task(gateio.run_spot()),
        asyncio.create_task(gateio.run_futures()),
        asyncio.create_task(gateio._batch_unsubscribe_worker()),
        #HTX
        asyncio.create_task(htx.run_spot()),
        asyncio.create_task(htx.run_futures()),
        asyncio.create_task(htx._batch_unsubscribe_worker()),
        # #OKX
        asyncio.create_task(okx.run_spot()),
        asyncio.create_task(okx.run_futures()),
        asyncio.create_task(okx._batch_unsubscribe_worker()),
        
        #asyncio.create_task(order())
    ]

    print("🚀 Starting FULL dynamic WebSocket manager...")


    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        bingx.stop()
        binance.stop()
        mexc.stop()
        bybit_spot.stop()
        bybit_linear.stop()

async def main():
    task_orderbook = asyncio.create_task(стакан())

    config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    task_server = asyncio.create_task(server.serve())
    #check = asyncio.create_task(monitor_memory())

    await asyncio.gather(task_orderbook, task_server)
        
        
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n✅ Graceful shutdown complete")
