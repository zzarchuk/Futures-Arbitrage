from fastapi import APIRouter, HTTPException
from database.database import get_my_filters, SessionDep, Filters
from web.backend.schemas.schemas import FilterSchema, FilterUpdateSchema
from sqlalchemy import select

filter_router = APIRouter(
    prefix="/api/v1/filters",
    tags=["Filters"]
)



@filter_router.get(
    "",
    summary="Get my filters",
    description="Returns all configured filtering rules used for spread detection.",
)
async def get_filters():
    return await get_my_filters()






@filter_router.post(
    '',
    summary='Add filter for Bot',
    description='Your filter is added to the database for further work with it.',
)
async def add_filter(post_filter: FilterSchema, session: SessionDep):
    new_filter = Filters(
        exchanges = post_filter.exchanges,
        min_volume = post_filter.min_volume,
        max_volume = post_filter.max_volume,
        gap = post_filter.gap
    )
    session.add(new_filter)
    await session.commit()
    return {'message': 'Filter created', 'data': post_filter}






@filter_router.delete(
    '/{filter_id}',
    summary="Delete filter",
    description="Delete filter by ID",
)
async def delete_filter(filter_id: int, session: SessionDep):
    result = await session.execute(select(Filters).where(Filters.id == filter_id))
    obj = result.scalar_one_or_none()
    
    if not obj:
        raise HTTPException(status_code=404, detail="Filter not found")
    
    await session.delete(obj)
    await session.commit()
    return {'succes': True}






@filter_router.patch(
    '/{filter_id}',
    summary='Change filter.',
    description='Here you can change certain parameters of your filter'
)
async def change_filter(filter_id: int, session: SessionDep, update_filter: FilterUpdateSchema):
    response = await session.execute(select(Filters).where(Filters.id == filter_id))
    obj = response.scalar_one_or_none()
    
    if not obj:
        raise HTTPException(status_code=404, detail='Filter not found')
    
    update_data = update_filter.model_dump(exclude_unset=True)
    print(update_data)
    for field, value in update_data.items():
        setattr(obj, field, value)
    
    await session.commit()
    await session.refresh(obj)
    return {'success': True}