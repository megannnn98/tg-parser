from parser.telegram import fetch_messages, get_client
from parser.storage import (
    get_db,
    init_db,
    save_messages_many,
    upsert_channels_many,
    upsert_users_many,
)
from parser.logger import get_logger
from parser.utils import db_path_for_channel
from config import CHANNELS
from parser.measure_time import measure_time
import aiosqlite
import asyncio

_QUEUE_CHANNEL = "channel"
_QUEUE_USER = "user"
_QUEUE_MESSAGE = "message"


async def _db_writer(db_path, queue: asyncio.Queue, batch_size: int = 500):
    db = await get_db(db_path)
    try:
        await init_db(db)
        channel_buf: set[str] = set()
        user_buf: dict[int, str | None] = {}
        msg_buf: list[tuple[int, str, str, str]] = []

        async def flush():
            if not channel_buf and not user_buf and not msg_buf:
                return
            await db.execute("BEGIN")
            try:
                if channel_buf:
                    await upsert_channels_many(
                        db,
                        [(name,) for name in channel_buf],
                    )
                if user_buf:
                    await upsert_users_many(
                        db,
                        [(tg_id, username) for tg_id, username in user_buf.items()],
                    )
                if msg_buf:
                    await save_messages_many(db, msg_buf)
                await db.commit()
            except Exception:
                await db.rollback()
                raise
            finally:
                channel_buf.clear()
                user_buf.clear()
                msg_buf.clear()

        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break

            kind = item[0]
            if kind == _QUEUE_CHANNEL:
                channel_buf.add(item[1])
            elif kind == _QUEUE_USER:
                user_buf[item[1]] = item[2]
            elif kind == _QUEUE_MESSAGE:
                msg_buf.append(item[1])

            if (
                len(msg_buf) >= batch_size
                or len(user_buf) >= batch_size
                or len(channel_buf) >= batch_size
            ):
                await flush()

            queue.task_done()

        await flush()
    finally:
        await db.close()


async def collect_channel(tg_client, queue: asyncio.Queue, channel_username: str):
    logger = get_logger("collector")

    channel = await tg_client.get_chat(channel_username)

    if not channel.linked_chat:
        logger.warning(f"Channel {channel_username} has no linked discussion")
        return

    chat_id = channel.linked_chat.id
    await queue.put((_QUEUE_CHANNEL, channel_username))

    seen_users: dict[int, str | None] = {}

    async for msg in fetch_messages(tg_client, chat_id):
        tg_id = msg["tg_id"]
        username = msg["username"]
        prev_username = seen_users.get(tg_id)
        if prev_username is None or prev_username != username:
            await queue.put((_QUEUE_USER, tg_id, username))
            seen_users[tg_id] = username

        await queue.put(
            (
                _QUEUE_MESSAGE,
                (
                    tg_id,
                    channel_username,
                    msg["text"],
                    msg["date"],
                ),
            )
        )


async def get_db_stats(db_path, channel: str) -> tuple[int, int]:
    try:
        async with aiosqlite.connect(db_path) as db:
            async with db.execute(
                "SELECT COUNT(DISTINCT user) FROM messages WHERE channel = ?",
                (channel,),
            ) as cur:
                users_count = (await cur.fetchone())[0]
    except aiosqlite.OperationalError:
        users_count = 0

    try:
        async with aiosqlite.connect(db_path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM messages WHERE channel = ?",
                (channel,),
            ) as cur:
                messages_count = (await cur.fetchone())[0]
    except aiosqlite.OperationalError:
        messages_count = 0

    return users_count, messages_count


async def collect_one_channel(
    tg_client,
    queue: asyncio.Queue,
    channel: str,
    sem: asyncio.Semaphore,
    db_path,
):
    logger = get_logger("collector")
    async with sem:
        logger.info(f"[{channel}] start")

        users_before, messages_before = await get_db_stats(db_path, channel)

        logger.info(
            f"[{channel}] BEFORE → users={users_before}, "
            f"messages={messages_before}"
        )
        await collect_channel(tg_client, queue, channel)
        users_after, messages_after = await get_db_stats(db_path, channel)

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

    db_path = db_path_for_channel("default")
    queue: asyncio.Queue = asyncio.Queue(maxsize=5000)
    writer_task = asyncio.create_task(_db_writer(db_path, queue))
    sem = asyncio.Semaphore(8)

    async with tg_client:
        tasks = [
            asyncio.create_task(
                collect_one_channel(tg_client, queue, channel, sem, db_path)
            )
            for channel in channels
        ]
        await asyncio.gather(*tasks)

    await queue.put(None)
    await writer_task

    logger.info("Done")
