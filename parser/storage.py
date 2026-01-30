
async def save_message(db, chat_id, msg, user_id, channel_id):
    await db.execute(
        """
        INSERT OR IGNORE INTO messages
        (chat_id, message_id, user_id, date, text, channel_id)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            chat_id,
            msg["message_id"],
            user_id,
            msg["date"],
            msg["text"],
            channel_id,
        )
    )
async def upsert_user(db, tg_id: int, username: str | None) -> int:
    async with db.execute(
        """
        INSERT INTO users (tg_id, username)
        VALUES (?, ?)
        ON CONFLICT(tg_id) DO UPDATE SET
            username = excluded.username
        RETURNING id
        """,
        (tg_id, username)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0]

async def get_user_id(db, tg_id: int) -> int | None:
    async with db.execute(
        "SELECT id FROM users WHERE tg_id = ?",
        (tg_id,)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else None
