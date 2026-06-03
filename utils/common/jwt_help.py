from config.secret import config
import jwt
from database.database import Users, SessionDep
from sqlalchemy import select
from jwt import ExpiredSignatureError, InvalidTokenError
from fastapi import Request, HTTPException, WebSocket

async def get_user(req: Request, session: SessionDep):
    
    token = req.cookies.get(config.JWT_ACCESS_COOKIE_NAME)
    if not token:
        raise HTTPException(status_code=401, detail='You are not logined or registered')
    try:
        data = jwt.decode(jwt=token, algorithms=['HS256'], key=config.JWT_SECRET_KEY) # type: ignore
        
        user_id = data.get('sub')
        result = await session.execute(select(Users).where(Users.id == user_id))
        user = result.scalar_one_or_none()
        if not user:
            raise HTTPException(
                401,
                "User no longer exists"
            )           
        
        return data.get('sub')
    except ExpiredSignatureError:
        raise HTTPException(status_code=401, detail='Token expired, login one more time')
    except InvalidTokenError:
        raise HTTPException(status_code=401, detail='Invalid token')
    
    
async def get_user_for_ws(ws: WebSocket, session: SessionDep):
    
    token = ws.cookies.get(config.JWT_ACCESS_COOKIE_NAME)
    if not token:
        
        raise HTTPException(status_code=401, detail='You are not logined or registered')
    try:
        data = jwt.decode(jwt=token, algorithms=['HS256'], key=config.JWT_SECRET_KEY) # type: ignore
        
        user_id = data.get('sub')
        result = await session.execute(select(Users).where(Users.id == user_id))
        user = result.scalar_one_or_none()
        if not user:
            raise HTTPException(
                401,
                "User no longer exists"
            ) 
        
        return data.get('sub')
    except ExpiredSignatureError:
        raise HTTPException(status_code=401, detail='Token expired, login one more time')
    except InvalidTokenError:
        raise HTTPException(status_code=401, detail='Invalid token')
    
    