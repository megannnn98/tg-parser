from parser.telegram import fetch_messages
from parser.storage import upsert_user, get_user_id, save_message
from config import CHANNEL_USERNAME


async def collect_data(app, db):
    channel = await app.get_chat(CHANNEL_USERNAME)

    if not channel.linked_chat:
        print("У канала нет комментариев")
        return

    discussion_id = channel.linked_chat.id

    print("Collecting data...")

    async for msg in fetch_messages(app, discussion_id):
        await upsert_user(db, msg.tg_user_id, msg.username)
        user_id = await get_user_id(db, msg.tg_user_id)

        msg.user_id = user_id
        await save_message(db, msg)

    print("Done")
