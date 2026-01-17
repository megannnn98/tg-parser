from pyrogram import Client
from config import API_ID, API_HASH
import asyncio
from parser.telegram import get_client, fetch_messages, fetch_users
from parser.database import get_db, init_db
from parser.storage import save_message, save_user
from config import CHANNEL_USERNAME

async def main():
    db = await get_db()
    await init_db(db)

    app = get_client()
    await app.start()
    print("Logged in")

    await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
