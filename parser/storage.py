async def get_or_create_user(db, tg_id: int, username: str | None):
    await db.execute(
        """
        INSERT OR IGNORE INTO users (tg_id, username)
        VALUES (?, ?)
        """,
        (tg_id, username)
    )

    async with db.execute(
        "SELECT id FROM users WHERE tg_id = ?",
        (tg_id,)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0]

async def save_message(db, discussion_id, msg, user_id):
    await db.execute(
        """
        INSERT OR IGNORE INTO messages
        (discussion_id, message_id, user_id, date, text)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            discussion_id,
            msg["message_id"],
            user_id,
            msg["date"],
            msg["text"],
        )
    )
