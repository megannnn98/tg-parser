import asyncio
from pathlib import Path

from parser.analytics import get_haters
from parser.storage import (
    get_db,
    init_db,
    save_messages_many,
    upsert_channels_many,
    upsert_users_many,
)


def test_get_haters_returns_empty_for_blank_hate_words(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "app.db")
        try:
            await init_db(db)
            return await get_haters(db, ["", "   "], "chan_a")
        finally:
            await db.close()

    result = asyncio.run(_run())
    assert result == []


def test_get_haters_counts_messages_case_insensitive(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "app.db")
        try:
            await init_db(db)
            await upsert_channels_many(db, [("chan_a",)])
            await upsert_users_many(db, [(1, "alice"), (2, "bob")])
            await save_messages_many(
                db,
                [
                    (1, "chan_a", "I hate SPAM", "2025-02-01"),
                    (1, "chan_a", "No issues", "2025-02-01"),
                    (2, "chan_a", "spam everywhere", "2025-02-01"),
                    (2, "chan_a", "SPAM again", "2025-02-01"),
                ],
            )
            return await get_haters(db, ["spam"], "chan_a")
        finally:
            await db.close()

    result = asyncio.run(_run())
    assert result == [
        ("bob", 2, 2, 2, 100.0),
        ("alice", 1, 1, 2, 50.0),
    ]


def test_get_haters_filters_by_channel(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "app.db")
        try:
            await init_db(db)
            await upsert_channels_many(db, [("chan_a",), ("chan_b",)])
            await upsert_users_many(db, [(1, "alice")])
            await save_messages_many(
                db,
                [
                    (1, "chan_a", "spam here", "2025-02-01"),
                    (1, "chan_b", "spam there", "2025-02-01"),
                ],
            )
            return (
                await get_haters(db, ["spam"], "chan_a"),
                await get_haters(db, ["spam"], "chan_b"),
            )
        finally:
            await db.close()

    chan_a, chan_b = asyncio.run(_run())
    assert chan_a == [("alice", 1, 1, 1, 100.0)]
    assert chan_b == [("alice", 1, 1, 1, 100.0)]
