
async def get_haters(db, hate_words: list[str]) -> list[tuple[str | None, int, int]]:
    result = []

    query = """
    SELECT
        u.username,
        u.tg_id,
        COUNT(*) AS cnt
    FROM messages m
    JOIN users u ON u.tg_id = m.user
    WHERE LOWER(m.text) LIKE LOWER(?)
    GROUP BY u.tg_id, u.username
    ORDER BY cnt DESC
    """

    async with db.execute(query, (f"%{hate_words[0]}%",)) as cursor:
        async for username, tg_id, cnt in cursor:
            result.append((username, tg_id, cnt))

    return result
