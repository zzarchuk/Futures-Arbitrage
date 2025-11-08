import asyncio
import json
import websockets

async def main():
    url = "wss://contract.mexc.com/edge"
    async with websockets.connect(url) as ws:
        # подписка на стакан
        sub_msg = {
            "method": "sub.depth",
            "param": {
                "symbol": "BTC_USDT",
                'limit': 100
            }
        }
        await ws.send(json.dumps(sub_msg))
        print("✅ Подписка отправлена")

        # получение обновлений стакана

        msg = await ws.recv()
        data = json.loads(msg)
        print(data)

asyncio.run(main())
