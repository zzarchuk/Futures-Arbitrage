from utils.common.jinja import templates
from fastapi import APIRouter, Request

first_page = APIRouter(
    prefix="/api/v1",
    tags=["Main Page"]
)



@first_page.get(
    "/",
    summary="Information",
    description="This endpoint will take you to a page that will describe this pet project in detail, where you can learn about the bots functionality and the technologies that were used to create it.",
)
async def main_page(request: Request):
    return templates.TemplateResponse(request=request, name="main_page.html")