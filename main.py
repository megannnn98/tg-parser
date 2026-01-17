from pyrogram import Client
from config import API_ID, API_HASH
import asyncio
from parser.telegram import get_client, fetch_messages, fetch_users
from parser.database import get_db, init_db
from parser.storage import save_message, save_user
from config import CHANNEL_USERNAME

async def main():

    app = get_client()

    async with app:
        channel = await app.get_chat(CHANNEL_USERNAME)
        print("Logged in, channel:", channel.title)

        if not channel.linked_chat:
            print("Failed to get linked chat")
            return

        discussion_id = channel.linked_chat.id

        db = await get_db()
        await init_db(db)

        async for user in fetch_users(app, discussion_id):
            await save_user(db, user)

        await db.commit()
        await db.close()
        print("Done")

if __name__ == "__main__":
    asyncio.run(main())
