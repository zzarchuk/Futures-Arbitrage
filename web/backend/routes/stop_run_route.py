from fastapi import APIRouter, Depends, HTTPException
from database.database import SessionDep, Filters, Blacklist
from sqlalchemy import select
from utils.common.jwt_help import get_user
from utils.parser_func.parsers import арбитраж_повтор
import asyncio
from config.config import state_tasks

stop_run_router = APIRouter(prefix="/api/v1", tags=["STOP / RUN"])


@stop_run_router.post(
    "/start",
    summary="Start Bot",
    description="To launch the bot, you must provide the ID of your filter for the bot to work. If there is no filter yet, create one and enter its ID.",
)
async def start(filter_id: int, session: SessionDep, user_id: int = Depends(get_user)):
    filter_response = await session.execute(
        select(Filters).where(Filters.id == filter_id, Filters.user_id == user_id)
    )
    filter_obj = filter_response.scalar_one_or_none()

    blacklist_response = await session.execute(
        select(Blacklist).where(Blacklist.user_id == user_id)
    )
    blacklist_obj = blacklist_response.scalar_one_or_none()
    blacklist_tokens = blacklist_obj.tokens if blacklist_obj else []

    if not filter_obj:
        raise HTTPException(status_code=404, detail="Filter not found")

    if state_tasks.tasks.get(user_id):
        raise HTTPException(
            status_code=404,
            detail=f"Bot is already running with filterd id: {filter_id}",
        )
    admin = int(user_id)
    if admin == 1:

        
        task = asyncio.create_task(
            арбитраж_повтор(
                макс_обьем=filter_obj.max_volume,
                мин_обьем=filter_obj.min_volume,
                шаг=filter_obj.gap,
                list_exchanges=filter_obj.exchanges,
                spread=filter_obj.spread,
                user_id=user_id,
                blacklist=blacklist_tokens,
                admin=True
            )
        )
        state_tasks.tasks[user_id] = task
        return {"success": True, "message": "Bot started"}
        
        
    task = asyncio.create_task(
        арбитраж_повтор(
            макс_обьем=filter_obj.max_volume,
            мин_обьем=filter_obj.min_volume,
            шаг=filter_obj.gap,
            list_exchanges=filter_obj.exchanges,
            spread=filter_obj.spread,
            user_id=user_id,
            blacklist=blacklist_tokens,
        )
    )
    state_tasks.tasks[user_id] = task

    return {"success": True, "message": "Bot started"}


@stop_run_router.post("/stop", summary="Stop Bot", description="The bot stops its work")
async def stop(user_id: int = Depends(get_user)):
    if not state_tasks.tasks.get(user_id):
        raise HTTPException(status_code=404, detail="Your bot dont work")

    task_id = state_tasks.tasks.get(user_id)
    task_id.cancel()  # type: ignore
    try:
        await task_id  # type: ignore
    except asyncio.CancelledError:
        pass
    state_tasks.tasks.clear()
    # а тут бот перестает работать естественно делаеться проверка или запущена задача стакана,
    # если запущена тогда задача перестает работать, если даже не запущена а ты хочешь остановить
    # то что не запущено тогда даст ошибку что бот даже не работал
    return {"success": True, "message": "Bot stopped"}
