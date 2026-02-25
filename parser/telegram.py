from pathlib import Path

from pyrogram import Client
from config import API_ID, API_HASH, LIMIT, SESSION_NAME


def get_client():
    return Client(
        SESSION_NAME,
        api_id=API_ID,
        api_hash=API_HASH
    )


def reset_session_files(session_name: str = SESSION_NAME) -> int:
    session_path = Path(session_name)
    base = session_path if session_path.suffix == ".session" else Path(f"{session_name}.session")
    candidates = (
        base,
        base.with_name(f"{base.name}-journal"),
        base.with_name(f"{base.name}-shm"),
        base.with_name(f"{base.name}-wal"),
    )
    removed = 0
    for file_path in candidates:
        try:
            file_path.unlink()
            removed += 1
        except FileNotFoundError:
            pass
    return removed


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
