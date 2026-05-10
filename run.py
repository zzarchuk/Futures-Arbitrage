from fastapi import Depends, FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from contextlib import asynccontextmanager
from config.config import state_websocket
import asyncio
from main import стакан
from database.database import run_database
from utils.common.jwt_help import get_user_for_ws
from web.backend.routes.filters_route import filter_router
from web.backend.routes.stop_run_route import stop_run_router
from web.backend.routes.login_register import reg_log
from web.backend.routes.blacklist_route import blacklist_router
from web.backend.routes.main_page import first_page
from web.backend.routes.chart_route import chart_router

from config.logger_config import setup_logger

from utils.common.jinja import STATIC_DIR, templates





@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logger()
    asyncio.create_task(стакан())
    asyncio.create_task(run_database())
    # print(BASE_DIR)
    # что будет делать до начала запуска приложения
    yield
    # что будет делать после заверщения приложения



app = FastAPI(lifespan=lifespan)
app.include_router(filter_router)
app.include_router(stop_run_router)
app.include_router(reg_log)
app.include_router(blacklist_router)
app.include_router(first_page)
app.include_router(chart_router)

app.mount("/static", StaticFiles(directory=STATIC_DIR), "static")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)




@app.get("/socket_view", tags=["Socket Page"])
async def socket_page(request: Request):
    return templates.TemplateResponse(request=request, name="socket.html")




@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket, user_id: int = Depends(get_user_for_ws)):
    await ws.accept()
    async with state_websocket.lock_websocket:
        state_websocket.websocket_clients[user_id] = ws
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        async with state_websocket.lock_websocket:
            del state_websocket.websocket_clients[user_id]




if __name__ == "__main__":
    uvicorn.run("run:app", reload=True)
