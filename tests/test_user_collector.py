import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from fake_pyrogram import FakeUnauthorized

from parser.user_collector import (
    UserCollectorConfig,
    UserCollectorDeps,
    collect_user_comments,
)


class FakeTGClient:
    def __init__(self, chats: dict[str, object]):
        self.chats = chats
        self.entered = 0
        self.exited = 0
        self.searched: list[int] = []

    async def __aenter__(self):
        self.entered += 1
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exited += 1
        return False

    async def get_chat(self, channel_username: str):
        return self.chats[channel_username]


class FakeLogger:
    def __init__(self):
        self.warnings: list[str] = []
        self.infos: list[str] = []
        self.exceptions: list[str] = []
        self.errors: list[str] = []

    def info(self, msg):
        self.infos.append(msg)

    def warning(self, msg):
        self.warnings.append(msg)

    def error(self, msg):
        self.errors.append(msg)

    def exception(self, msg):
        self.exceptions.append(msg)


def _chat(linked_chat_id: int | None):
    linked = SimpleNamespace(id=linked_chat_id) if linked_chat_id else None
    return SimpleNamespace(linked_chat=linked)


def _resolved(username="vasya", first_name=None, last_name=None, tg_id=555):
    return {
        "tg_id": tg_id,
        "username": username,
        "first_name": first_name,
        "last_name": last_name,
    }


def _deps(tg_client, logger, messages: dict[int, list[dict]], resolved=None):
    resolved = _resolved() if resolved is None else resolved

    async def resolve_user_fn(_tg_client, user_ref):
        if isinstance(resolved, Exception):
            raise resolved
        return resolved

    async def fetch_user_messages_fn(_tg_client, chat_id, tg_id):
        tg_client.searched.append((chat_id, tg_id))
        for msg in messages.get(chat_id, []):
            yield msg

    return UserCollectorDeps(
        tg_client_factory=lambda: tg_client,
        fetch_user_messages_fn=fetch_user_messages_fn,
        resolve_user_fn=resolve_user_fn,
        logger_factory=lambda _name: logger,
    )


async def _read_rows(db_path: Path):
    from parser.storage import get_db

    db = await get_db(db_path)
    try:
        async with db.execute(
            """
            SELECT tg_id, username, channel, message_id, text, date
            FROM user_messages
            ORDER BY channel, message_id
            """
        ) as cur:
            return await cur.fetchall()
    finally:
        await db.close()


def test_collect_user_comments_writes_rows_and_skips_channel_without_discussion(
    tmp_path: Path,
):
    tg_client = FakeTGClient(
        {
            "chan_a": _chat(1001),
            "chan_no_discussion": _chat(None),
        }
    )
    logger = FakeLogger()
    messages = {
        1001: [
            {"message_id": 10, "text": "Первый КОММЕНТ", "date": "2025-02-01"},
            {"message_id": 11, "text": "Второй", "date": "2025-02-02"},
        ]
    }

    async def _run():
        db_path, saved = await collect_user_comments(
            tmp_path,
            UserCollectorConfig(channels=["chan_a", "chan_no_discussion"]),
            "@vasya",
            _deps(tg_client, logger, messages),
        )
        return db_path, saved, await _read_rows(db_path)

    db_path, saved, rows = asyncio.run(_run())

    assert db_path == tmp_path / "vasya_555.db"
    assert saved == 2
    assert rows == [
        (555, "vasya", "chan_a", 10, "первый коммент", "2025-02-01"),
        (555, "vasya", "chan_a", 11, "второй", "2025-02-02"),
    ]
    assert tg_client.searched == [(1001, 555)]
    assert tg_client.entered == 1
    assert tg_client.exited == 1
    assert any("chan_no_discussion" in msg for msg in logger.warnings)


def test_collect_user_comments_second_run_adds_no_duplicates(tmp_path: Path):
    messages = {
        1001: [{"message_id": 10, "text": "Первый", "date": "2025-02-01"}],
    }

    async def _run():
        results = []
        for _ in range(2):
            tg_client = FakeTGClient({"chan_a": _chat(1001)})
            results.append(
                await collect_user_comments(
                    tmp_path,
                    UserCollectorConfig(channels=["chan_a"]),
                    555,
                    _deps(tg_client, FakeLogger(), messages),
                )
            )
        db_paths = [path for path, _ in results]
        return db_paths, [saved for _, saved in results], await _read_rows(db_paths[0])

    db_paths, (first, second), rows = asyncio.run(_run())

    assert db_paths[0] == db_paths[1] == tmp_path / "vasya_555.db"
    assert first == 1
    assert second == 0
    assert len(rows) == 1


def test_collect_user_comments_names_db_by_display_name_without_username(
    tmp_path: Path,
):
    messages = {1001: [{"message_id": 10, "text": "Первый", "date": "2025-02-01"}]}

    async def _run():
        return await collect_user_comments(
            tmp_path,
            UserCollectorConfig(channels=["chan_a"]),
            555,
            _deps(
                FakeTGClient({"chan_a": _chat(1001)}),
                FakeLogger(),
                messages,
                resolved=_resolved(
                    username=None, first_name="Хрюкало", last_name="Офф"
                ),
            ),
        )

    db_path, saved = asyncio.run(_run())

    assert db_path == tmp_path / "хрюкало_офф_555.db"
    assert saved == 1


def test_collect_user_comments_names_db_by_tg_id_without_username_or_name(
    tmp_path: Path,
):
    messages = {1001: [{"message_id": 10, "text": "Первый", "date": "2025-02-01"}]}

    async def _run():
        return await collect_user_comments(
            tmp_path,
            UserCollectorConfig(channels=["chan_a"]),
            555,
            _deps(
                FakeTGClient({"chan_a": _chat(1001)}),
                FakeLogger(),
                messages,
                resolved=_resolved(username=None),
            ),
        )

    db_path, saved = asyncio.run(_run())

    assert db_path == tmp_path / "555.db"
    assert saved == 1


def test_collect_user_comments_prefers_username_over_display_name(tmp_path: Path):
    messages = {1001: [{"message_id": 10, "text": "Первый", "date": "2025-02-01"}]}

    async def _run():
        return await collect_user_comments(
            tmp_path,
            UserCollectorConfig(channels=["chan_a"]),
            "@hryukalo",
            _deps(
                FakeTGClient({"chan_a": _chat(1001)}),
                FakeLogger(),
                messages,
                resolved=_resolved(
                    username="hryukalo", first_name="Хрюкало", last_name="Офф"
                ),
            ),
        )

    db_path, _ = asyncio.run(_run())

    assert db_path == tmp_path / "hryukalo_555.db"


def test_collect_user_comments_honours_db_path_override(tmp_path: Path):
    messages = {1001: [{"message_id": 10, "text": "Первый", "date": "2025-02-01"}]}
    override = tmp_path / "nested" / "custom.db"

    async def _run():
        return await collect_user_comments(
            tmp_path,
            UserCollectorConfig(channels=["chan_a"]),
            "@vasya",
            _deps(FakeTGClient({"chan_a": _chat(1001)}), FakeLogger(), messages),
            db_path_override=override,
        )

    db_path, saved = asyncio.run(_run())

    assert db_path == override
    assert override.exists()
    assert not (tmp_path / "vasya_555.db").exists()
    assert saved == 1


def test_collect_user_comments_creates_no_db_when_user_unresolved(tmp_path: Path):
    async def _run():
        await collect_user_comments(
            tmp_path,
            UserCollectorConfig(channels=["chan_a"]),
            "@ghost",
            _deps(
                FakeTGClient({"chan_a": _chat(1001)}),
                FakeLogger(),
                {},
                resolved=RuntimeError("Cannot resolve user '@ghost'"),
            ),
        )

    with pytest.raises(RuntimeError):
        asyncio.run(_run())

    assert list(tmp_path.iterdir()) == []


def test_collect_user_comments_keeps_going_after_channel_error(tmp_path: Path):
    class BrokenChatClient(FakeTGClient):
        async def get_chat(self, channel_username: str):
            if channel_username == "chan_broken":
                raise RuntimeError("CHANNEL_INVALID")
            return await super().get_chat(channel_username)

    tg_client = BrokenChatClient({"chan_a": _chat(1001)})
    logger = FakeLogger()
    messages = {1001: [{"message_id": 10, "text": "Первый", "date": "2025-02-01"}]}

    async def _run():
        db_path, saved = await collect_user_comments(
            tmp_path,
            UserCollectorConfig(channels=["chan_broken", "chan_a"]),
            "@vasya",
            _deps(tg_client, logger, messages),
        )
        return saved, await _read_rows(db_path)

    saved, rows = asyncio.run(_run())

    assert saved == 1
    assert len(rows) == 1
    assert rows[0][2] == "chan_a"
    assert any("chan_broken" in msg for msg in logger.exceptions)


def test_collect_user_comments_aborts_on_fatal_session_error(tmp_path: Path):
    class DeadSessionClient(FakeTGClient):
        def __init__(self, chats):
            super().__init__(chats)
            self.get_chat_calls: list[str] = []

        async def get_chat(self, channel_username: str):
            self.get_chat_calls.append(channel_username)
            raise FakeUnauthorized("SESSION_REVOKED")

    tg_client = DeadSessionClient({"chan_a": _chat(1001), "chan_b": _chat(1002)})
    logger = FakeLogger()

    async def _run():
        await collect_user_comments(
            tmp_path,
            UserCollectorConfig(channels=["chan_a", "chan_b"]),
            "@vasya",
            _deps(tg_client, logger, {}),
        )

    with pytest.raises(FakeUnauthorized):
        asyncio.run(_run())

    assert tg_client.get_chat_calls == ["chan_a"]
    assert logger.exceptions == []
    assert any("chan_a" in msg for msg in logger.errors)


def test_collect_user_comments_reports_saved_rows_before_fatal_abort(tmp_path: Path):
    class DiesOnSecondChannelClient(FakeTGClient):
        async def get_chat(self, channel_username: str):
            if channel_username == "chan_b":
                raise FakeUnauthorized("SESSION_REVOKED")
            return await super().get_chat(channel_username)

    tg_client = DiesOnSecondChannelClient({"chan_a": _chat(1001)})
    logger = FakeLogger()
    messages = {
        1001: [
            {"message_id": 10, "text": "Первый", "date": "2025-02-01"},
            {"message_id": 11, "text": "Второй", "date": "2025-02-02"},
        ]
    }

    async def _run():
        await collect_user_comments(
            tmp_path,
            UserCollectorConfig(channels=["chan_a", "chan_b"]),
            "@vasya",
            _deps(tg_client, logger, messages),
        )

    with pytest.raises(FakeUnauthorized):
        asyncio.run(_run())

    # Rows from chan_a are committed and the abort log says how many survived.
    rows = asyncio.run(_read_rows(tmp_path / "vasya_555.db"))
    assert len(rows) == 2
    assert any("2" in msg for msg in logger.errors)


def test_collect_user_comments_rejects_empty_channels(tmp_path: Path):
    async def _run():
        await collect_user_comments(
            tmp_path,
            UserCollectorConfig(channels=[]),
            "@vasya",
            _deps(FakeTGClient({}), FakeLogger(), {}),
        )

    with pytest.raises(RuntimeError):
        asyncio.run(_run())

    assert list(tmp_path.iterdir()) == []
