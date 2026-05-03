from fastapi.templating import Jinja2Templates
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent

STATIC_DIR = BASE_DIR / "web" / "frontend" / "static"

templates = Jinja2Templates(directory=BASE_DIR / "web" / "frontend" / "templates")