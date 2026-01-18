from parser.telegram import fetch_messages
from parser.storage import upsert_user, get_user_id, save_message
from config import CHANNEL_USERNAME
from parser.logger import get_logger

logger = get_logger(__name__)

async def collect_data(app, db):
    logger.info("Collecting data started")
    channel = await app.get_chat(CHANNEL_USERNAME)

    if not channel.linked_chat:
        logger.warning("Channel has no linked chat (no comments)")
        return

    discussion_id = channel.linked_chat.id
    logger.info(f"Discussion ID: {discussion_id}")

    async for msg in fetch_messages(app, discussion_id):
        logger.debug(f"Message {msg.tg_message_id} from user {msg.username}")
        await upsert_user(db, msg.tg_user_id, msg.username)
        user_id = await get_user_id(db, msg.tg_user_id)

        msg.user_id = user_id
        await save_message(db, msg)

    print("Done")
