from authx import AuthX, AuthXConfig
import os
from dotenv import load_dotenv

load_dotenv()


secret = os.getenv("JWT_SECRET")

config = AuthXConfig()
config.JWT_SECRET_KEY = secret
config.JWT_ACCESS_COOKIE_NAME = 'my_access_token'
config.JWT_TOKEN_LOCATION = ['cookies']

security = AuthX(config=config)