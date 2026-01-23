from parser.telegram import fetch_messages
from parser.storage import upsert_user, save_message
from parser.logger import get_logger

async def collect_channel(tg_client, db, channel_username: str):
    channel = await tg_client.get_chat(channel_username)

    if not channel.linked_chat:
        return

    chat_id = channel.linked_chat.id

    logger = get_logger("main")

    logger.info(f"Collecting channel \"{channel_username}\"")

    async for msg in fetch_messages(tg_client, chat_id):
        user_id = await upsert_user(
            db,
            tg_id=msg["tg_id"],
            username=msg["username"]
        )

        await save_message(
            db,
            discussion_id=chat_id,
            msg=msg,
            user_id=user_id
        )

    await db.commit()
