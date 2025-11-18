from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pathlib import Path
import asyncio
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


async def send_message_to_site(text: str, message_id: str | None = None):
    """
    Отправка или обновление сообщения на сайте.
    Если message_id не указан — создаётся новое сообщение.
    Если указан — обновляется существующее.
    """
    if message_id is None:
        message_id = str(uuid.uuid4())
        action = "create"
    else:
        action = "update"

    data = {"action": action, "id": message_id, "text": text}

    async with lock:
        for ws in list(clients):
            try:
                await ws.send_json(data)
            except:
                clients.remove(ws)

    return message_id  # возвращаем ID, чтобы потом обновить это сообщение


async def delete_message_from_site(message_id: str):
    """
    Удаляет сообщение с указанным message_id на всех подключённых клиентах.
    """
    data = {"action": "delete", "id": message_id}

    async with lock:
        for ws in list(clients):
            try:
                await ws.send_json(data)
            except:
                clients.remove(ws)
