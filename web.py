# from fastapi import FastAPI, WebSocket, WebSocketDisconnect
# from fastapi.responses import HTMLResponse
# from pathlib import Path
# import asyncio
# import uuid

# app = FastAPI()
# clients = set()
# lock = asyncio.Lock()

# @app.get("/", response_class=HTMLResponse)
# async def main_page():
#     html_path = Path(__file__).parent / "templates" / "index.html"
#     return html_path.read_text(encoding="utf-8")


# @app.websocket("/ws")
# async def websocket_endpoint(ws: WebSocket):
#     await ws.accept()
#     async with lock:
#         clients.add(ws)
#     try:
#         while True:
#             await ws.receive_text()
#     except WebSocketDisconnect:
#         async with lock:
#             clients.remove(ws)


# async def send_message_to_site(text: str, message_id: str | None = None):
#     """
#     Отправка или обновление сообщения на сайте.
#     Если message_id не указан — создаётся новое сообщение.
#     Если указан — обновляется существующее.
#     """
#     if message_id is None:
#         message_id = str(uuid.uuid4())
#         action = "create"
#     else:
#         action = "update"

#     data = {"action": action, "id": message_id, "text": text}

#     async with lock:
#         for ws in list(clients):
#             try:
#                 await ws.send_json(data)
#             except:
#                 clients.remove(ws)

#     return message_id  # возвращаем ID, чтобы потом обновить это сообщение


# async def delete_message_from_site(message_id: str):
#     """
#     Удаляет сообщение с указанным message_id на всех подключённых клиентах.
#     """
#     data = {"action": "delete", "id": message_id}

#     async with lock:
#         for ws in list(clients):
#             try:
#                 await ws.send_json(data)
#             except:
#                 clients.remove(ws)

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from схождения import схождения
from pydantic import BaseModel
from pathlib import Path
import asyncio
from state import orderbook
import uuid

app = FastAPI()
clients = set()
lock = asyncio.Lock()


@app.get("/", response_class=HTMLResponse)
async def main_page():
    html_path = Path(__file__).parent / "templates" / "index.html"
    return html_path.read_text(encoding="utf-8")


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    async with lock:
        clients.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        async with lock:
            clients.remove(ws)


# ----- кнопка открытия сделки -----
class TradeRequest(BaseModel):
    exchange_long: dict
    exchange_short: dict
    symbol: str
    volume: float
    long_price: float
    short_price: float
    spread: float



@app.post("/open_trade")
async def open_trade(req: TradeRequest):
    try:
        # здесь вызываешь свой арбитражный код, например:
        asyncio.create_task(схождения(req.exchange_long, req.exchange_short, req.symbol, req.volume, req.long_price, req.short_price, req.spread))
        #print(f"Данные:\n\n\n{req.exchange_long}\n{req.exchange_short}\n{req.symbol}\n{req.volume}\n{req.long_price}\n{req.short_price}\n{req.spread}\n")

        return {"status": "ok"}
    except Exception as e:
        print("Ошибка открытия позиции:", e)
        return {"status": "error", "detail": str(e)}


# ----- WebSocket сообщения -----
async def send_message_to_site(text: str, long_price: float, short_price: float, spread: float, exchange_long: str = "", exchange_short: str = "", exchange_short_type: str = "", exchange_long_type: str = "",
                               symbol: str = "", volume: float = 0, message_id: str | None = None):

    if message_id is None:
        message_id = str(uuid.uuid4())
        action = "create"
    else:
        action = "update"

    # Заменяем \n на <br> для HTML
    text_html = text.replace('\n', '<br>')

    data = {
        "action": action,
        "id": message_id,
        "text": text_html,
        "exchange_long": {exchange_long: exchange_long_type},
        "exchange_short": {exchange_short: exchange_short_type},
        "symbol": symbol,
        "volume": volume,
        'long_price': long_price,
        'short_price': short_price,
        'spread': spread
    }

    async with lock:
        for ws in list(clients):
            try:
                await ws.send_json(data)
            except:
                clients.remove(ws)

    return message_id

async def update_spread(text: str, message_id: str | None = None):

    if message_id is None:
        message_id = str(uuid.uuid4())
        action = "create"
    else:
        action = "update"

    # Заменяем \n на <br> для HTML
    text_html = text.replace('\n', '<br>')

    data = {
        "action": action,
        "id": message_id,
        "text": text_html,
        'message_type': 'update_spread'

    }

    async with lock:
        for ws in list(clients):
            try:
                await ws.send_json(data)
            except:
                clients.remove(ws)

    return message_id


async def delete_message_from_site(message_id: str):
    data = {"action": "delete", "id": message_id}

    async with lock:
        for ws in list(clients):
            try:
                await ws.send_json(data)
            except:
                clients.remove(ws)

