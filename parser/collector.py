from parser.telegram import fetch_messages
from parser.storage import get_or_create_user, save_message

async def collect_channel(app, db, channel_username: str):
    channel = await app.get_chat(channel_username)

    if not channel.linked_chat:
        return

    discussion_id = channel.linked_chat.id

    async for msg in fetch_messages(app, discussion_id):
        user_id = await get_or_create_user(
            db,
            msg["tg_id"],
            msg["username"]
        )
        await save_message(db, discussion_id, msg, user_id)

    await db.commit()
