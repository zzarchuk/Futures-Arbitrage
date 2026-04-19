from fastapi import APIRouter, HTTPException
from database.database import SessionDep, Filters
from sqlalchemy import select


stop_run_router = APIRouter(prefix="/api/v1", tags=["STOP / RUN"])


@stop_run_router.post("/start", summary='Start Bot', description='To launch the bot, you must provide the ID of your filter for the bot to work. If there is no filter yet, create one and enter its ID.')
async def start(filter_id: int, session: SessionDep):
    response = await session.execute(select(Filters).where(Filters.id == filter_id))
    obj = response.scalar_one_or_none()

    if not obj:
        raise HTTPException(status_code=404, detail="Filter not found")

    # тут начнеться работа бота с фильтром из бд
    return {"success": True, "message": "Bot started"}


@stop_run_router.post("/stop", summary='Stop Bot', description="The bot stops its work")
async def stop():
    # а тут бот перестает работать естественно делаеться проверка или запущена задача стакана, 
    # если запущена тогда задача перестает работать, если даже не запущена а ты хочешь остановить 
    # то что не запущено тогда даст ошибку что бот даже не работал
    return {"success": True, 'message': 'Bot stopped'}
