from fastapi import APIRouter, HTTPException, Depends
from database.database import SessionDep, Blacklist, Users
from web.backend.schemas.schemas import BlacklistSchema
from sqlalchemy import select
from utils.common.jwt_help import get_user
from sqlalchemy.orm import selectinload

blacklist_router = APIRouter(
    prefix="/api/v1/blacklist",
    tags=["Blacklist"]
)


@blacklist_router.get(
    "",
    summary="Get my blacklist",
    description="Returns your all blacklist",
)
async def get_blacklist(session: SessionDep, user_id: int = Depends(get_user)):
    query = await session.execute(select(Blacklist).where(Blacklist.user_id == user_id))
    obj = query.scalars().all()
    
    if not obj:
        raise HTTPException(status_code=401, detail='Your blacklist is empty')
    
    return {'success': True, 'data': obj}




@blacklist_router.post(
    "",
    summary="Post my blacklist",
    description="Post your tokens for blacklist, like this: BTCUSDT, ETHUSDT...",
)
async def post_blacklist(blacklist_schema: BlacklistSchema, session: SessionDep, user_id: int = Depends(get_user)):
    result = await session.execute(select(Users).options(selectinload(Users.blacklist)).where(Users.id == user_id))
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(status_code=401, detail='You are not registered yet')

    if not user.blacklist: # type: ignore
    
        user.blacklist = Blacklist( # type: ignore
            user_id = user_id, 
            tokens = blacklist_schema.tokens
        )
        
        await session.commit()
        return {"success": True, 'data': 'blacklist is created'}
    
    user.blacklist.tokens = list(set(user.blacklist.tokens).union(blacklist_schema.tokens)) # type: ignore
        
        

    await session.commit()
    return {"success": True, 'data': 'Tokens are added'}
    
    
    




@blacklist_router.delete(
    "",
    summary="Delete tokens from backlist",
    description="Delete your tokens for blacklist, like this: BTCUSDT, ETHUSDT...",
)
async def delete_tokens_blacklist(blacklist_schema: BlacklistSchema, session: SessionDep, user_id: int = Depends(get_user)):
    result = await session.execute(select(Users).options(selectinload(Users.blacklist)).where(Users.id == user_id))
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(status_code=401, detail='You are not registered yet')
    
    if not user.blacklist: # type: ignore
        raise HTTPException(status_code=404, detail='You dont have blacklist')


    user.blacklist.tokens = list(set(user.blacklist.tokens) - set(blacklist_schema.tokens)) # type: ignore
        
        

    await session.commit()
    return {"success": True, 'data': 'token is deleted'}
    



