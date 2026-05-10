from utils.common.jinja import templates
from fastapi import APIRouter, HTTPException, Depends, Request
from sqlalchemy import select
from database.database import SessionDep, Candles
from utils.common.jwt_help import get_user
from web.backend.schemas.schemas import CandlesSchema, ChartSchema
from collections import defaultdict

chart_router = APIRouter(prefix="/api/v1", tags=["Chart"])


@chart_router.get("/chart")
async def charts(request: Request):
    return templates.TemplateResponse(request=request, name="chart.html")


@chart_router.get("/candles")
async def candles(session: SessionDep, data: CandlesSchema = Depends()):
    response = await session.execute(
        select(Candles)
        .where(
            Candles.exchange_long == data.exchange_long,
            Candles.exchange_short == data.exchange_short,
            Candles.type_long == data.type_long,
            Candles.type_short == data.type_short,
            Candles.token == data.symbol,
            Candles.volume == data.volume,
        )
        .order_by(Candles.time)
    )

    candles = response.scalars().all()

    if not candles:
        return HTTPException(status_code=401, detail="Пока что нету графика")

    return [
        {
            "exchange_long": data.exchange_long,
            "type_long": data.type_long,
            "exchange_short": data.exchange_short,
            "type_short": data.type_short,
            "time": c.time,
            "open": c.open_spread,
            "high": c.high,
            "low": c.low,
            "close": c.close,
        }
        for c in candles
    ]


@chart_router.get("/graphs")
async def graphs_token(session: SessionDep):

    tokens = []
    seen = set()

    obj = await session.execute(
        select(Candles).order_by(Candles.time.desc())
    )

    result = obj.scalars().all()

    for row in result:
        if row.token not in seen:
            tokens.append(row.token)
            seen.add(row.token)

    return tokens





@chart_router.get("/one_chart")
async def chart_token(session: SessionDep, data: ChartSchema = Depends()):
    valid = defaultdict(list)


    obj = await session.execute(
        select(Candles).where(Candles.token == data.symbol).order_by(Candles.time)
    )
    result = obj.scalars().all()

    for i in result:
        key = f'{i.token}_{i.exchange_long}_{i.type_long}_{i.exchange_short}_{i.type_short}_{i.volume}'
        valid[key].append(i)
    
    return valid
