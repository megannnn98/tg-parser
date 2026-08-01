import asyncio

from pyrogram import Client, enums
from pyrogram.errors import AuthKeyDuplicated, FloodWait, Unauthorized
from config import API_ID, API_HASH, LIMIT
from parser.logger import get_logger

# Session is dead: retrying the next channel cannot succeed.
FATAL_TG_ERRORS = (Unauthorized, AuthKeyDuplicated)

logger = get_logger("telegram")

def get_client():
    return Client(
        "my_session",
        api_id=API_ID,
        api_hash=API_HASH,
        sleep_threshold=60,
    )


async def get_chat_with_retry(tg_client, chat_id, logger=logger, sleep_fn=None):
    try:
        return await tg_client.get_chat(chat_id)
    except FATAL_TG_ERRORS:
        raise
    except FloodWait as exc:
        wait = exc.value + 1
        logger.warning(f"FloodWait {wait}s on get_chat({chat_id!r}), waiting it out")
        await (sleep_fn or asyncio.sleep)(wait)

    return await tg_client.get_chat(chat_id)


async def fetch_messages(tg_client, channel_linked_chat_id):
    async for msg in tg_client.get_chat_history(channel_linked_chat_id, LIMIT):
        if not msg.text or not msg.from_user:
            continue

        yield {
            "tg_id": msg.from_user.id,
            "username": msg.from_user.username,
            "message_id": msg.id,
            "date": str(msg.date),
            "text": msg.text,
        }

async def describe_channel(tg_client, username: str) -> dict | None:
    try:
        chat = await tg_client.get_chat(username)
    except FATAL_TG_ERRORS:
        raise
    except FloodWait:
        # A rate limit is not a verdict on the channel: let the caller wait it
        # out, otherwise a good candidate is written off as unresolvable.
        raise
    except Exception as exc:
        logger.warning(f"Cannot resolve @{username}: {exc}")
        return None

    # A discussion supergroup also has a linked_chat (the channel it belongs
    # to), so the type check is what keeps chats out of the channel list.
    if chat.type != enums.ChatType.CHANNEL:
        logger.info(f"Skipped @{username}: not a channel ({chat.type})")
        return None

    if not chat.linked_chat:
        logger.info(f"Skipped @{username}: no linked discussion")
        return None

    return {
        "username": username,
        "title": chat.title,
        "members": chat.members_count,
    }

async def fetch_mentions(tg_client, chat_id):
    async for msg in tg_client.get_chat_history(chat_id, LIMIT):
        forward = msg.forward_from_chat

        yield {
            "forward_channel": forward.username if forward else None,
            "text": msg.text or msg.caption,
        }

def _as_found_user(user) -> dict:
    return {
        "tg_id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
    }

def _matches_name(user, query: str) -> bool:
    haystack = " ".join(
        part
        for part in (user.username, user.first_name, user.last_name)
        if part
    )
    return query.strip().casefold() in haystack.casefold()

async def find_chat_members(tg_client, chat_id, query: str):
    async for member in tg_client.get_chat_members(chat_id, query=query):
        if not member.user:
            continue

        yield _as_found_user(member.user)

async def find_history_authors(tg_client, chat_id, query: str):
    seen: set[int] = set()

    async for msg in tg_client.get_chat_history(chat_id, LIMIT):
        user = msg.from_user
        if not user or user.id in seen or not _matches_name(user, query):
            continue

        seen.add(user.id)
        yield _as_found_user(user)

async def resolve_user(tg_client, user_ref: int | str) -> dict:
    try:
        user = await tg_client.get_users(user_ref)
    except FATAL_TG_ERRORS:
        # A dead session is not an "unknown user": keep the original type.
        raise
    except Exception as exc:
        raise RuntimeError(f"Cannot resolve user {user_ref!r}: {exc}") from exc

    return _as_found_user(user)

async def fetch_user_messages(tg_client, chat_id, tg_id: int):
    async for msg in tg_client.search_messages(chat_id, from_user=tg_id, limit=0):
        if not msg.text:
            continue

        if not msg.from_user or msg.from_user.id != tg_id:
            sender = msg.from_user.id if msg.from_user else None
            logger.warning(
                f"Skipped message {msg.id} in chat {chat_id}: "
                f"sender {sender} != requested {tg_id}"
            )
            continue

        yield {
            "message_id": msg.id,
            "date": str(msg.date),
            "text": msg.text,
        }
