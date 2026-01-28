from parser.telegram import fetch_messages
from parser.storage import upsert_user, save_message
from parser.logger import get_logger
from parser.utils import parse_channels, db_path_for_channel
from parser.telegram import get_client
from parser.database import get_db, init_db
from config import CHANNELS
import aiosqlite

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

async def get_db_stats(db_path: str) -> tuple[int, int]:
    async with aiosqlite.connect(db_path) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cur:
            users_count = (await cur.fetchone())[0]

        async with db.execute("SELECT COUNT(*) FROM messages") as cur:
            messages_count = (await cur.fetchone())[0]

    return users_count, messages_count


async def collect_db():
    channels = parse_channels(CHANNELS)
    if not channels:
        raise RuntimeError("CHANNELS are empty")

    tg_client = get_client()
    logger = get_logger("main")

    async with tg_client:
        for channel in channels:
            db_path = db_path_for_channel(channel)

            users_before, messages_before = await get_db_stats(db_path)

            logger.info(
                f"[{channel}] BEFORE → users={users_before}, messages={messages_before}"
            )

            db = await get_db(db_path)
            try:
                logger.info(f"Collecting channel {channel}")
                await init_db(db)
                await collect_channel(tg_client, db, channel)
                logger.info(f"Finished collecting channel {channel}")
            finally:
                await db.close()

            users_after, messages_after = await get_db_stats(db_path)

            logger.info(
                f"[{channel}] AFTER  → users={users_after} (+{users_after - users_before}), "
                f"messages={messages_after} (+{messages_after - messages_before})"
            )

        logger.info("Done")
