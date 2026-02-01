
async def get_haters(
    db,
    hate_words: list[str],
    channel: str,
) -> list[tuple[str | None, int, int, int, float]]:
    result = []

    normalized_hate_words = [word.strip() for word in hate_words if word.strip()]
    if not normalized_hate_words:
        return result

    hate_condition = " OR ".join(
        ["LOWER(COALESCE(m.text, '')) LIKE LOWER(?)"] * len(normalized_hate_words)
    )

    query = f"""
    SELECT
        u.username,
        u.tg_id,
        SUM(CASE WHEN {hate_condition} THEN 1 ELSE 0 END) AS hate_messages,
        COUNT(*) AS total_messages,
        ROUND(
            100.0 * SUM(CASE WHEN {hate_condition} THEN 1 ELSE 0 END) / COUNT(*),
            2
        ) AS hate_percent
    FROM messages m
    JOIN users u ON u.tg_id = m.user
    WHERE m.channel = ?
    GROUP BY u.tg_id, u.username
    HAVING hate_messages > 0
    ORDER BY hate_messages DESC, total_messages DESC
    """

    patterns = [f"%{word}%" for word in normalized_hate_words]
    params = tuple(patterns + patterns + [channel])

    async with db.execute(query, params) as cursor:
        async for username, tg_id, hate_messages, total_messages, hate_percent in cursor:
            result.append(
                (username, tg_id, hate_messages, total_messages, hate_percent)
            )

    return result
