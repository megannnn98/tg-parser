from parser.telegram import fetch_messages, fetch_users
from parser.storage import save_message, save_user
from config import CHANNEL_USERNAME


async def collect_data(app, db):
    channel = await app.get_chat(CHANNEL_USERNAME)

    if not channel.linked_chat:
        print("Failed to get linked chat")
        return

    discussion_id = channel.linked_chat.id

    print("Start collecting users...")
    async for user in fetch_users(app, discussion_id):
        await save_user(db, user)

    # print("Start collecting messages...")
    # async for message in fetch_messages(app, discussion_id):
    #     await save_message(db, message)
