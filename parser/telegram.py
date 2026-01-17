from pyrogram import Client
from parser.models import Message, User
from config import API_ID, API_HASH, LIMIT


def get_client():
    return Client("my_session", api_id=API_ID, api_hash=API_HASH)


async def fetch_messages(app, discussion_id):
    async for msg in app.get_chat_history(discussion_id, LIMIT):
        if not msg.text:
            continue

        username = (
            msg.from_user.first_name
            if msg.from_user and msg.from_user.first_name
            else "anon"
        )

        yield Message(
            discussion_id=discussion_id,
            date=str(msg.date),
            user=username,
            text=msg.text
        )

async def fetch_users(app, discussion_id):
    async for msg in app.get_chat_history(discussion_id, LIMIT):
        if not msg.text:
            continue

        username = (
            msg.from_user.first_name
            if msg.from_user and msg.from_user.first_name
            else "anon"
        )

        yield User(
            user=username
        )
