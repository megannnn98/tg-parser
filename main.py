from pyrogram import Client
from config import API_ID, API_HASH
import asyncio
from parser.telegram import get_client, fetch_messages, fetch_users
from parser.database import get_db, init_db
from parser.storage import save_message, save_user
from config import CHANNEL_USERNAME

with Client("my_session", api_id=API_ID, api_hash=API_HASH) as app:
    print("Logged in")


