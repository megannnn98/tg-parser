from pyrogram import Client
from config import API_ID, API_HASH, LIMIT

def get_client():
    return Client(
        "my_session",
        api_id=API_ID,
        api_hash=API_HASH
    )

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
