
async def get_user_comments(
    db,
    username: str,
) -> list[tuple[str, str, str]]:
    result = []

    query = """
    SELECT m.channel, m.date, m.text
    FROM messages m
    JOIN users u ON u.tg_id = m.user
    WHERE LOWER(u.username) = LOWER(?)
    ORDER BY m.channel, m.date
    """

    async with db.execute(query, (username,)) as cursor:
        async for channel, date, text in cursor:
            result.append((channel, date, text))

    return result
