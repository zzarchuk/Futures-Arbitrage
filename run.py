from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from contextlib import asynccontextmanager
from pathlib import Path
from config.config import state_websocket
from main import стакан
import asyncio
from database.database import run_database
from web.backend.routes.filters_route import filter_router
from web.backend.routes.stop_run_route import stop_run_router

BASE_DIR = Path(__file__).resolve().parent

STATIC_DIR = BASE_DIR / "web" / "frontend" / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # asyncio.create_task(стакан())
    asyncio.create_task(run_database())
    # print(BASE_DIR)
    # что будет делать до начала запуска приложения
    yield
    # что будет делать после заверщения приложения


app = FastAPI(lifespan=lifespan)
app.include_router(filter_router)
app.include_router(stop_run_router)


app.mount("/static", StaticFiles(directory=STATIC_DIR), "static")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


templates = Jinja2Templates(directory=BASE_DIR / "web" / "frontend" / "templates")


@app.get(
    "/",
    tags=["Main Page"],
    summary="Information",
    description="This endpoint will take you to a page that will describe this pet project in detail, where you can learn about the bots functionality and the technologies that were used to create it.",
)
async def main_page(request: Request):
    return templates.TemplateResponse(request=request, name="main_page.html")


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    async with state_websocket.lock_websocket:
        state_websocket.websocket_clients.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        async with state_websocket.lock_websocket:
            state_websocket.websocket_clients.remove(ws)


if __name__ == "__main__":
    uvicorn.run("run:app", reload=True)
