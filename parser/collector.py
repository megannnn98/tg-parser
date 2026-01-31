from parser.telegram import fetch_messages, get_client
from parser.storage import upsert_user, save_message, upsert_topic, get_db, init_db
from parser.logger import get_logger
from parser.utils import db_path_for_channel
from config import CHANNELS
import aiosqlite
from pathlib import Path

async def collect_channel(tg_client, db, channel_username: str):
    logger = get_logger("main")

    channel = await tg_client.get_chat(channel_username)

    if not channel.linked_chat:
        logger.warning(f"Channel {channel_username} has no linked discussion")
        return

    discussion_id = channel.linked_chat.id
    topic_title = channel.title

    await upsert_topic(
        db,
        discussion_id=discussion_id,
        title=topic_title
    )

    async for msg in fetch_messages(tg_client, discussion_id):
        user_id = await upsert_user(
            db,
            tg_id=msg["tg_id"],
            username=msg["username"]
        )

        await save_message(
            db,
            discussion_id=discussion_id,
            msg=msg,
            user_id=user_id
        )

    await db.commit()


async def get_db_stats(db_path: Path) -> tuple[int, int, int]:
    async with aiosqlite.connect(db_path) as db:

        async def count(table: str) -> int:
            try:
                async with db.execute(f"SELECT COUNT(*) FROM {table}") as cur:
                    return (await cur.fetchone())[0]
            except aiosqlite.OperationalError:
                return 0

        users_count = await count("users")
        messages_count = await count("messages")
        topics_count = await count("topic")

    return users_count, messages_count, topics_count



async def collect_db():
    channels = CHANNELS
    if not channels:
        raise RuntimeError("CHANNELS are empty")

    tg_client = get_client()
    logger = get_logger("main")

    async with tg_client:
        for channel in channels:
            db_path = db_path_for_channel(channel)

            # ⬇️ СНАЧАЛА открываем БД и создаём схему
            db = await get_db(db_path)
            try:
                await init_db(db)
            finally:
                await db.close()

            # ⬇️ ТЕПЕРЬ таблицы гарантированно есть
            users_before, messages_before, topics_before = await get_db_stats(db_path)

            logger.info(
                f"[{channel}] BEFORE → users={users_before}, "
                f"messages={messages_before}, topics={topics_before}"
            )

            db = await get_db(db_path)
            try:
                await collect_channel(tg_client, db, channel)
            finally:
                await db.close()

            users_after, messages_after, topics_after = await get_db_stats(db_path)

            logger.info(
                f"[{channel}] AFTER  → users={users_after} (+{users_after - users_before}), "
                f"messages={messages_after} (+{messages_after - messages_before}), "
                f"topics={topics_after} (+{topics_after - topics_before})"
            )

        logger.info("Done")
