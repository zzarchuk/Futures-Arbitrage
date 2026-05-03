from fastapi import APIRouter, Depends, HTTPException, Response, Request
from database.database import SessionDep, Users
from sqlalchemy import select
from config.secret import security, config
from utils.security.security import hash_password, verify_password
from utils.common.jwt_help import get_user
from web.backend.schemas.schemas import LoginRegisterSchema


reg_log = APIRouter(prefix="/api/v1", tags=["REGISTER / LOGIN"])






@reg_log.post("/register", description='Register for use this Bot')
async def register(register: LoginRegisterSchema, session: SessionDep, response: Response):
    email = register.email
    result = await session.execute(select(Users).where(Users.email == email))
    obj = result.scalar_one_or_none()
    if obj:
        raise HTTPException(status_code=409, detail='Email already registered')
    
    
    new_user = Users(
        email = register.email,
        password = await hash_password(register.password)
    )
    session.add(new_user)
    await session.commit()
    await session.refresh(new_user)
    
    
    token = security.create_access_token(uid=str(new_user.id))
    response.set_cookie(config.JWT_ACCESS_COOKIE_NAME, token)
    
    
    return {"success": True, "data": "User created"}





@reg_log.post('/login', description='Login for use this Bot')
async def login(session: SessionDep, login: LoginRegisterSchema, response: Response):
    email = login.email
    result = await session.execute(select(Users).where(Users.email == email))
    obj = result.scalar_one_or_none()
    if not obj: 
        raise HTTPException(status_code=401, detail='You are not registered')
    
    
    verif = await verify_password(login.password, obj.password)
    
    if verif:
        token = security.create_access_token(uid=str(obj.id))
        response.set_cookie(config.JWT_ACCESS_COOKIE_NAME, token)
        
        return {'success': True, 'data': 'succesfully logined'}
    
    else:
        raise HTTPException(status_code=401, detail='You are password is not correct')
    
    
    
    
@reg_log.post('/logout', description='Logout from system')
async def logout(response: Response, req: Request, user_id: int = Depends(get_user)):

    response.delete_cookie(config.JWT_ACCESS_COOKIE_NAME)

    return {"success": True, "data": f'Logout is completed, user id: {user_id}'}
