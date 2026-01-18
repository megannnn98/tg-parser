# web/app.py
from fastapi import FastAPI, Request, HTTPException
from fastapi.templating import Jinja2Templates
import aiosqlite
from pathlib import Path
from parser.utils import list_channels, db_path_for_channel

app = FastAPI()

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=BASE_DIR / "templates")


@app.get("/channels/{name}/users")
async def channel_users(request: Request, name: str):
    db_path = db_path_for_channel(name)

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Channel not found")

    async with aiosqlite.connect(db_path) as db:
        query = """
        SELECT
            u.id,
            u.username,
            COUNT(m.id) AS messages_count
        FROM users u
        LEFT JOIN messages m ON m.user_id = u.id
        GROUP BY u.id
        ORDER BY messages_count DESC
        """
        cursor = await db.execute(query)
        rows = await cursor.fetchall()

    users = [
        {
            "id": r[0],
            "username": r[1] or "anon",
            "count": r[2]
        }
        for r in rows
    ]

    return templates.TemplateResponse(
        "users.html",
        {
            "request": request,
            "channel": name,
            "users": users
        }
    )


@app.get("/channels/{name}/users/{user_id}")
async def channel_user_messages(request: Request, name: str, user_id: int):
    db_path = db_path_for_channel(name)

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Channel not found")

    async with aiosqlite.connect(db_path) as db:
        query = """
        SELECT
            u.username,
            m.date,
            m.text
        FROM messages m
        JOIN users u ON u.id = m.user_id
        WHERE u.id = ?
        ORDER BY m.date DESC
        """
        cursor = await db.execute(query, (user_id,))
        rows = await cursor.fetchall()

    if not rows:
        username = "unknown"
        messages = []
    else:
        username = rows[0][0] or "anon"
        messages = [{"date": r[1], "text": r[2]} for r in rows]

    return templates.TemplateResponse(
        "user_messages.html",
        {
            "request": request,
            "channel": name,
            "username": username,
            "messages": messages
        }
    )

@app.get("/channels")
async def channels_index(request: Request):
    channels = list_channels()

    return templates.TemplateResponse(
        "channels.html",
        {
            "request": request,
            "channels": channels
        }
    )
