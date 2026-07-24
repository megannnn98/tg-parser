import asyncio
from pathlib import Path

from parser.analytics import get_user_comments
from parser.storage import (
    get_db,
    init_db,
    save_messages_many,
    upsert_channels_many,
    upsert_users_many,
)


def test_get_user_comments_returns_empty_for_unknown_username(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "app.db")
        try:
            await init_db(db)
            return await get_user_comments(db, "nobody")
        finally:
            await db.close()

    result = asyncio.run(_run())
    assert result == []


def test_get_user_comments_collects_across_channels_case_insensitive(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "app.db")
        try:
            await init_db(db)
            await upsert_channels_many(db, [("chan_a",), ("chan_b",)])
            await upsert_users_many(db, [(1, "Alice"), (2, "bob")])
            await save_messages_many(
                db,
                [
                    (1, "chan_a", "first", "2025-02-02"),
                    (1, "chan_a", "second", "2025-02-01"),
                    (1, "chan_b", "third", "2025-02-01"),
                    (2, "chan_a", "not alice", "2025-02-01"),
                ],
            )
            return await get_user_comments(db, "ALICE")
        finally:
            await db.close()

    result = asyncio.run(_run())
    assert result == [
        ("chan_a", "2025-02-01", "second"),
        ("chan_a", "2025-02-02", "first"),
        ("chan_b", "2025-02-01", "third"),
    ]
