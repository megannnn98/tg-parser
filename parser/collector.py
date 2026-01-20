from parser.telegram import fetch_messages
from parser.storage import upsert_user, save_message

async def collect_channel(tg_client, db, channel_username: str):
    channel = await tg_client.get_chat(channel_username)

    if not channel.linked_chat:
        return

    discussion_id = channel.linked_chat.id

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
