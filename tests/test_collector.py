import asyncio
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
from fake_pyrogram import FakeUnauthorized

from parser.collector import CollectorConfig, CollectorDeps, collect_db
from parser.telegram import CollectedMessage


class FakeTGClient:
    def __init__(self, chats: dict[str, object]):
        self.chats = chats

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get_chat(self, channel_username: str):
        chat = self.chats[channel_username]
        if isinstance(chat, Exception):
            raise chat
        return chat


class FakeLogger:
    def __init__(self):
        self.infos: list[str] = []
        self.warnings: list[str] = []
        self.exceptions: list[str] = []

    def info(self, msg):
        self.infos.append(msg)

    def warning(self, msg):
        self.warnings.append(msg)

    def exception(self, msg):
        self.exceptions.append(msg)


def _chat(linked_chat_id: int):
    return SimpleNamespace(linked_chat=SimpleNamespace(id=linked_chat_id))


def _deps(chats: dict[str, object], logger: FakeLogger) -> CollectorDeps:
    async def fetch_messages_fn(_tg_client, chat_id):
        yield CollectedMessage(
            tg_id=chat_id,
            username="vasya",
            message_id=1,
            date="2026-10-08 06:00:00",
            text="hello",
        )

    return CollectorDeps(
        tg_client_factory=lambda: FakeTGClient(chats),
        fetch_messages_fn=fetch_messages_fn,
        logger_factory=lambda _name: logger,
    )


def _saved_channels(db_path: Path) -> list[str]:
    with sqlite3.connect(db_path) as db:
        return [row[0] for row in db.execute("SELECT channel FROM messages")]


def test_collect_db_skips_failed_channel_and_collects_the_rest(tmp_path: Path):
    db_path = tmp_path / "app.db"
    logger = FakeLogger()
    chats = {
        "dead": RuntimeError("USERNAME_NOT_OCCUPIED"),
        "alive": _chat(100),
    }

    asyncio.run(
        collect_db(
            db_path, CollectorConfig(channels=["dead", "alive"]), _deps(chats, logger)
        )
    )

    assert _saved_channels(db_path) == ["alive"]
    assert logger.exceptions == ["[dead] failed, skipped"]
    assert logger.warnings == ["1 of 2 channels failed"]
    assert "[dead] done" not in logger.infos


def test_collect_db_aborts_on_dead_session(tmp_path: Path):
    logger = FakeLogger()
    chats = {"first": FakeUnauthorized("session revoked")}

    with pytest.raises(FakeUnauthorized):
        asyncio.run(
            collect_db(
                tmp_path / "app.db",
                CollectorConfig(channels=["first"]),
                _deps(chats, logger),
            )
        )

    assert logger.exceptions == []
