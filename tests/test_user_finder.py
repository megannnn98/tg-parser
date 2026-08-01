import asyncio
from types import SimpleNamespace

import pytest
from fake_pyrogram import FakeUnauthorized

from parser.user_finder import (
    FoundUser,
    UserFinderConfig,
    UserFinderDeps,
    find_users,
    format_found_users,
)


class FakeTGClient:
    def __init__(self, chats: dict[str, object]):
        self.chats = chats

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get_chat(self, channel_username: str):
        return self.chats[channel_username]


class FakeLogger:
    def __init__(self):
        self.infos: list[str] = []
        self.warnings: list[str] = []
        self.errors: list[str] = []
        self.exceptions: list[str] = []

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


def _user(tg_id: int, username=None, first="Хрюкало", last="Офф"):
    return {
        "tg_id": tg_id,
        "username": username,
        "first_name": first,
        "last_name": last,
    }


def _deps(tg_client, logger, members: dict, history: dict, calls: list | None = None):
    async def find_chat_members_fn(_tg_client, chat_id, query):
        if calls is not None:
            calls.append(("members", chat_id))
        for row in members.get(chat_id, []):
            yield row

    async def find_history_authors_fn(_tg_client, chat_id, query):
        if calls is not None:
            calls.append(("history", chat_id))
        for row in history.get(chat_id, []):
            yield row

    return UserFinderDeps(
        tg_client_factory=lambda: tg_client,
        find_chat_members_fn=find_chat_members_fn,
        find_history_authors_fn=find_history_authors_fn,
        logger_factory=lambda _name: logger,
    )


def test_find_users_dedupes_the_same_person_across_channels():
    tg_client = FakeTGClient({"chan_a": _chat(1001), "chan_b": _chat(1002)})
    members = {
        1001: [_user(555, "hryukalo")],
        1002: [_user(555, "hryukalo"), _user(777, "other", "Хрюкало", "Второй")],
    }

    found = asyncio.run(
        find_users(
            UserFinderConfig(channels=["chan_a", "chan_b"]),
            "Хрюкало",
            _deps(tg_client, FakeLogger(), members, {}),
        )
    )

    assert [u.tg_id for u in found] == [555, 777]
    assert found[0].channels == ["chan_a", "chan_b"]
    assert found[0].sources == ["members"]
    assert found[1].channels == ["chan_b"]


def test_find_users_falls_back_to_history_only_when_members_are_empty():
    tg_client = FakeTGClient({"chan_a": _chat(1001), "chan_b": _chat(1002)})
    members = {1001: [_user(555, "hryukalo")]}
    history = {1002: [_user(555, "hryukalo")]}
    calls: list = []

    found = asyncio.run(
        find_users(
            UserFinderConfig(channels=["chan_a", "chan_b"]),
            "Хрюкало",
            _deps(tg_client, FakeLogger(), members, history, calls),
        )
    )

    # chan_a matched in members -> its history is never scanned
    assert calls == [("members", 1001), ("members", 1002), ("history", 1002)]
    assert [u.tg_id for u in found] == [555]
    assert found[0].channels == ["chan_a", "chan_b"]
    assert found[0].sources == ["members", "history"]


def _deps_with_broken_members(tg_client, logger, history: dict, calls: list, error):
    async def find_chat_members_fn(_tg_client, chat_id, query):
        calls.append(("members", chat_id))
        raise error
        yield {}  # unreachable: makes this function an async generator

    async def find_history_authors_fn(_tg_client, chat_id, query):
        calls.append(("history", chat_id))
        for row in history.get(chat_id, []):
            yield row

    return UserFinderDeps(
        tg_client_factory=lambda: tg_client,
        find_chat_members_fn=find_chat_members_fn,
        find_history_authors_fn=find_history_authors_fn,
        logger_factory=lambda _name: logger,
    )


def test_find_users_falls_back_to_history_when_member_search_fails():
    # get_chat_members needs membership in the discussion chat, get_chat_history
    # does not — a members failure must not take the channel down with it.
    logger = FakeLogger()
    calls: list = []

    found = asyncio.run(
        find_users(
            UserFinderConfig(channels=["chan_a"]),
            "Хрюкало",
            _deps_with_broken_members(
                FakeTGClient({"chan_a": _chat(1001)}),
                logger,
                {1001: [_user(555)]},
                calls,
                RuntimeError("CHAT_ADMIN_REQUIRED"),
            ),
        )
    )

    assert calls == [("members", 1001), ("history", 1001)]
    assert [u.tg_id for u in found] == [555]
    assert found[0].sources == ["history"]
    assert any("chan_a" in msg for msg in logger.warnings)
    assert logger.exceptions == []  # the channel is not counted as failed


def test_find_users_member_search_fatal_error_still_aborts():
    logger = FakeLogger()
    calls: list = []

    with pytest.raises(FakeUnauthorized):
        asyncio.run(
            find_users(
                UserFinderConfig(channels=["chan_a", "chan_b"]),
                "Хрюкало",
                _deps_with_broken_members(
                    FakeTGClient({"chan_a": _chat(1001), "chan_b": _chat(1002)}),
                    logger,
                    {1001: [_user(555)]},
                    calls,
                    FakeUnauthorized("SESSION_REVOKED"),
                ),
            )
        )

    # no history fallback, no second channel
    assert calls == [("members", 1001)]


def test_find_users_skips_channel_without_discussion():
    tg_client = FakeTGClient({"chan_a": _chat(1001), "chan_none": _chat(None)})
    logger = FakeLogger()
    calls: list = []

    found = asyncio.run(
        find_users(
            UserFinderConfig(channels=["chan_none", "chan_a"]),
            "Хрюкало",
            _deps(tg_client, logger, {1001: [_user(555)]}, {}, calls),
        )
    )

    assert [u.tg_id for u in found] == [555]
    assert calls == [("members", 1001)]
    assert any("chan_none" in msg for msg in logger.warnings)


def test_find_users_keeps_going_after_channel_error():
    class BrokenChatClient(FakeTGClient):
        async def get_chat(self, channel_username: str):
            if channel_username == "chan_broken":
                raise RuntimeError("CHANNEL_INVALID")
            return await super().get_chat(channel_username)

    logger = FakeLogger()
    found = asyncio.run(
        find_users(
            UserFinderConfig(channels=["chan_broken", "chan_a"]),
            "Хрюкало",
            _deps(
                BrokenChatClient({"chan_a": _chat(1001)}),
                logger,
                {1001: [_user(555)]},
                {},
            ),
        )
    )

    assert [u.tg_id for u in found] == [555]
    assert any("chan_broken" in msg for msg in logger.exceptions)


def test_find_users_aborts_on_fatal_session_error():
    class DeadSessionClient(FakeTGClient):
        def __init__(self, chats):
            super().__init__(chats)
            self.get_chat_calls: list[str] = []

        async def get_chat(self, channel_username: str):
            self.get_chat_calls.append(channel_username)
            raise FakeUnauthorized("SESSION_REVOKED")

    tg_client = DeadSessionClient({"chan_a": _chat(1001), "chan_b": _chat(1002)})
    logger = FakeLogger()

    with pytest.raises(FakeUnauthorized):
        asyncio.run(
            find_users(
                UserFinderConfig(channels=["chan_a", "chan_b"]),
                "Хрюкало",
                _deps(tg_client, logger, {}, {}),
            )
        )

    assert tg_client.get_chat_calls == ["chan_a"]
    assert logger.exceptions == []


def test_format_found_users_shows_ids_names_and_ready_commands():
    found = [
        FoundUser(
            tg_id=555,
            username="hryukalo",
            first_name="Хрюкало",
            last_name="Офф",
            channels=["rud01vb", "d_tyazhkun"],
            sources=["members"],
        ),
        FoundUser(
            tg_id=777,
            username=None,
            first_name=None,
            last_name=None,
            channels=["rud01vb"],
            sources=["history"],
        ),
    ]

    out = format_found_users(found)

    assert "@hryukalo" in out
    assert "Хрюкало Офф" in out
    assert "rud01vb, d_tyazhkun" in out
    assert "members" in out and "history" in out
    # a user without a username still gets a runnable command, by id
    assert "./scripts/run.sh user-comments 555" in out
    assert "./scripts/run.sh user-comments 777" in out


def test_find_users_rejects_empty_channels():
    with pytest.raises(RuntimeError):
        asyncio.run(
            find_users(
                UserFinderConfig(channels=[]),
                "Хрюкало",
                _deps(FakeTGClient({}), FakeLogger(), {}, {}),
            )
        )


@pytest.mark.parametrize("query", ["", "   "])
def test_find_users_rejects_empty_query(query: str):
    with pytest.raises(ValueError):
        asyncio.run(
            find_users(
                UserFinderConfig(channels=["chan_a"]),
                query,
                _deps(FakeTGClient({"chan_a": _chat(1001)}), FakeLogger(), {}, {}),
            )
        )
