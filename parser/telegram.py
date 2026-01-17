from pyrogram import Client
from parser.models import TelegramMessage
from config import API_ID, API_HASH, LIMIT


def get_client():
    return Client("my_session", api_id=API_ID, api_hash=API_HASH)


async def fetch_messages(app, discussion_id):
    async for msg in app.get_chat_history(discussion_id, limit=LIMIT):
        if not msg.text or not msg.from_user:
            continue

        yield TelegramMessage(
            tg_user_id=msg.from_user.id,
            username=msg.from_user.first_name or "anon",
            tg_message_id=msg.id,
            discussion_id=discussion_id,
            date=str(msg.date),
            text=msg.text
        )
