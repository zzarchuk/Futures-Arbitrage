from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi.staticfiles import StaticFiles
import asyncio
import uuid
from fastapi.responses import FileResponse
import random

stop_chart = False
stop_positions = False

app = FastAPI()

clients = set()
clients_chart = {}
clients_position = {}

lock = asyncio.Lock()
lock_chart = asyncio.Lock()
lock_position = asyncio.Lock()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # создаём фоновые задачи
    task1 = asyncio.create_task(send_chart_data())
    task2 = asyncio.create_task(схождение_покупка())

    try:
        yield  # приложение запускается
    finally:
        # останавливаем задачи при завершении приложения
        task1.cancel()
        task2.cancel()
        try:
            await task1
        except asyncio.CancelledError:
            pass
        try:
            await task2
        except asyncio.CancelledError:
            pass

app.router.lifespan_context = lifespan

@app.get("/position.css")
async def position_css():
    file_path = Path(__file__).parent / "static" / "position.css"
    return FileResponse(
        file_path,
        headers={"Cache-Control": "no-store"}
    )

@app.get("/script.js")
async def script():
    file_path = Path(__file__).parent / "static" / "script.js"
    return FileResponse(
        file_path,
        headers={"Cache-Control": "no-store"}
    )
    
@app.get("/style.css")
async def styles():
    file_path = Path(__file__).parent / "static" / "style.css"
    return FileResponse(
        file_path,
        headers={"Cache-Control": "no-store"}
    )
 
@app.get("/chart.css")
async def styles_chart():
    file_path = Path(__file__).parent / "static" / "chart.css"
    return FileResponse(
        file_path,
        headers={"Cache-Control": "no-store"}
    )

@app.get("/chart.js")
async def java_chart():
    file_path = Path(__file__).parent / "static" / "chart.js"
    return FileResponse(
        file_path,
        headers={"Cache-Control": "no-store"}
    )

@app.get("/position.js")
async def java_position():
    file_path = Path(__file__).parent / "static" / "position.js"
    return FileResponse(
        file_path,
        headers={"Cache-Control": "no-store"}
    )




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

@app.websocket("/ws_chart")
async def websocket_endpoint_chart(ws: WebSocket):
    await ws.accept()
    symbol = ws.query_params.get("symbol")
    if symbol not in clients_chart:
        clients_chart[symbol] = set()
    print(symbol)
    async with lock_chart:
        clients_chart[symbol].add(ws)
    
    try:
        while True:
            # просто ждём от клиента, чтобы соединение держалось
            await asyncio.sleep(0.2)
    except WebSocketDisconnect:
        async with lock_chart:
            del clients_chart[symbol]



@app.websocket("/ws_position")
async def websocket_endpoint_position(ws: WebSocket):
    
    await ws.accept()
    
    symbol = ws.query_params.get("symbol")
    long = ws.query_params.get("long")
    type_long = ws.query_params.get("type_long")
    short = ws.query_params.get("short")
    type_short = ws.query_params.get("type_short")
    quant_coins = ws.query_params.get("quant_coins")
    
    data = (symbol, long, type_long, short, type_short, quant_coins)
    print(data)
    
    
    if data not in clients_position:
        clients_position[data] = set()
        
        
    async with lock_position:
        clients_position[data].add(ws)
            
        
    try:
        while True:
            await asyncio.sleep(0.2)
    except WebSocketDisconnect:
        async with lock_position:
            clients_position[data].discard(ws)  # ← удаляем только конкретный ws
            if not clients_position[data]:  # если set пустой
                del clients_position[data] 
            

# ----- кнопка открытия сделки -----


@app.get("/chart", response_class=HTMLResponse)
async def chart_page(symbol):
    #print(symbol)
    html_path = Path(__file__).parent / "templates" / "chart.html"
    return html_path.read_text(encoding="utf-8")

@app.get('/position', response_class=HTMLResponse)
async def open_position(symbol, long, type_long, short, type_short, quant_coins):
    #print(symbol, long, type_long, short, type_short, quant_coins)
    html_path = Path(__file__).parent / "templates" / "position.html"
    return html_path.read_text(encoding="utf-8")




# ----- WebSocket сообщения -----
async def message_to_site(symbol, data = None):



    # Заменяем \n на <br> для HTML

    dataa = {
        "symbol": symbol,
        'data': data or []
    }

    async with lock:
        for ws in list(clients):
            try:
                await ws.send_json(dataa)
            except:
                clients.remove(ws)

    return symbol



async def send_chart_data():
    while True:
        await asyncio.sleep(1)  # обновление каждую секунду
        async with lock_chart:
            for symbol, clients in list(clients_chart.items()):  # копия словаря
                if not clients:
                    del clients_chart[symbol]
                    continue
                data = f'{symbol} + {random.randint(1, 666)}'
                print(data)
                for ws in set(clients):
                    try:
                        await ws.send_json({"chart": data})
                    except:
                        clients.discard(ws)


async def схождение_покупка():
    while True:
        await asyncio.sleep(1)  # обновление каждую секунду
        async with lock_position:
            for data, clients in list(clients_position.items()):  # копия словаря
                if not clients:
                    del clients_position[data]
                    continue
                #data = f'{data} + {random.randint(1, 666)}'
                print(data)
                symbol, long, type_long, short, type_short, quant_coins = data
                quant_coins = random.randint(1, 900)
                for ws in set(clients):
                    try:
                        await ws.send_json({"symbol": symbol, "long": long, "type_long": type_long, "short": short, "type_short": type_short, "quant_coins": quant_coins})
                    except:
                        clients.discard(ws)
                        
@app.get('/exit_position')
async def exit_position(symbol, long, type_long, short, type_short, quant_coins):
    print(f'функция сработала {symbol} {long} {type_long} {short} {type_short} {quant_coins}')
    data = (symbol, long, type_long, short, type_short, quant_coins)
    async with lock_position:
        del clients_position[data]
