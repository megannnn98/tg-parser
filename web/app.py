from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
import aiosqlite
from config import DB_PATH

app = FastAPI()
templates = Jinja2Templates(directory="web/templates")

@app.get("/users/html")
async def users_html(request: Request):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT u.username, COUNT(m.id)
            FROM users u
            LEFT JOIN messages m ON m.user_id = u.id
            GROUP BY u.username
            ORDER BY COUNT(m.id) DESC
        """) as cursor:
            rows = await cursor.fetchall()

    users = [
        {"username": r[0], "messages_count": r[1]}
        for r in rows
    ]

    return templates.TemplateResponse(
        "users.html",
        {"request": request, "users": users}
    )
