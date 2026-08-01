# parser/user_storage.py
from parser.utils import normalize


async def init_user_db(db):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS user_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER NOT NULL,
            username TEXT,
            channel TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            date TEXT NOT NULL,

            UNIQUE(channel, message_id)
        )
    """)

    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_user_messages_tg_id
        ON user_messages(tg_id)
    """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_user_messages_channel
        ON user_messages(channel)
    """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_user_messages_date
        ON user_messages(date)
    """)

    await db.commit()


async def save_user_messages_many(
    db,
    rows: list[tuple[int, str | None, str, int, str, str]],
):
    if not rows:
        return
    normalized_rows = [
        (tg_id, username, channel, message_id, normalize(text), date)
        for tg_id, username, channel, message_id, text, date in rows
    ]
    await db.executemany(
        """
        INSERT OR IGNORE INTO user_messages
        (tg_id, username, channel, message_id, text, date)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        normalized_rows,
    )
