from parser.telegram import fetch_messages, get_client
from parser.storage import upsert_user, upsert_channel, save_message, get_db, init_db
from parser.logger import get_logger
from parser.utils import db_path_for_channel
from config import CHANNELS
from pathlib import Path
from parser.measure_time import measure_time
import aiosqlite
import asyncio
import parser.logger as logger

async def collect_channel(tg_client, db, channel_username: str):
    logger = get_logger("collector")

    channel = await tg_client.get_chat(channel_username)

    if not channel.linked_chat:
        logger.warning(f"Channel {channel_username} has no linked discussion")
        return

    chat_id = channel.linked_chat.id

    async for msg in fetch_messages(tg_client, chat_id):
        await upsert_user(
            db,
            tg_id=msg["tg_id"],
            username=msg["username"]
        )
        await upsert_channel(
            db,
            name=channel_username
        )

        await save_message(
            db,
            msg["tg_id"],
            channel_username,
            msg=msg,
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

    return users_count, messages_count




async def collect_one_channel(
    tg_client,
    channel: str,
    sem: asyncio.Semaphore,
):
    logger = get_logger("collector")
    db_path = db_path_for_channel(channel)

    async with sem:
        logger.info(f"[{channel}] start")

        # init db
        db = await get_db(db_path)
        try:
            await init_db(db)
        finally:
            await db.close()

        users_before, messages_before = await get_db_stats(db_path)

        logger.info(
            f"[{channel}] BEFORE → users={users_before}, "
            f"messages={messages_before}"
        )

        # collect
        db = await get_db(db_path)
        try:
            await collect_channel(tg_client, db, channel)
        finally:
            await db.close()

        users_after, messages_after = await get_db_stats(db_path)

        logger.info(
            f"[{channel}] AFTER  → users={users_after} (+{users_after - users_before}), "
            f"messages={messages_after} (+{messages_after - messages_before})"
        )


@measure_time(name="collect_db")
async def collect_db():
    channels = CHANNELS
    if not channels:
        raise RuntimeError("CHANNELS are empty")

    tg_client = get_client()
    logger = get_logger("collector")

    sem = asyncio.Semaphore(15)

    async with tg_client:
        tasks = [
            asyncio.create_task(
                collect_one_channel(tg_client, channel, sem)
            )
            for channel in channels
        ]

        await asyncio.gather(*tasks)

    logger.info("Done")
