async def upsert_user(db, tg_user_id, username):
    await db.execute(
        """
        INSERT INTO users (tg_user_id, username)
        VALUES (?, ?)
        ON CONFLICT(tg_user_id)
        DO UPDATE SET username = excluded.username
        """,
        (tg_user_id, username)
    )


async def get_user_id(db, tg_user_id):
    async with db.execute(
        "SELECT id FROM users WHERE tg_user_id = ?",
        (tg_user_id,)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0]


async def save_message(db, msg):
    await db.execute(
        """
        INSERT OR IGNORE INTO messages
        (tg_message_id, discussion_id, user_id, date, text)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            msg.tg_message_id,
            msg.discussion_id,
            msg.user_id,
            msg.date,
            msg.text
        )
    )
