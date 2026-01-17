from parser.models import Message, User

async def save_message(db, message: Message):
    await db.execute(
        """
        INSERT INTO messages (discussion_id, date, user, text)
        VALUES (?, ?, ?, ?)
        """,
        (
            message.discussion_id,
            message.date,
            message.user,
            message.text
        )
    )

async def save_user(db, user: User):
    await db.execute(
        """
        INSERT OR IGNORE INTO users (user)
        VALUES (?)
        """,
        (user.user,)
    )
