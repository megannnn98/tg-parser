import asyncio
import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import fake_pyrogram
import pytest
from fake_pyrogram import FakeAuthKeyDuplicated, FakeUnauthorized


def _load_telegram_module(monkeypatch):
    monkeypatch.setenv("API_ID", "12345")
    monkeypatch.setenv("API_HASH", "hash123")
    monkeypatch.setenv("LIMIT", "2")

    monkeypatch.setitem(
        sys.modules,
        "pyrogram",
        SimpleNamespace(
            Client=object,
            errors=fake_pyrogram.errors,
            enums=fake_pyrogram.enums,
        ),
    )
    monkeypatch.setitem(sys.modules, "pyrogram.errors", fake_pyrogram.errors)
    monkeypatch.setitem(sys.modules, "pyrogram.enums", fake_pyrogram.enums)

    sys.modules.pop("config", None)
    sys.modules.pop("parser.telegram", None)
    return importlib.import_module("parser.telegram")


def test_get_client_uses_config_values(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, session_name, api_id, api_hash, sleep_threshold, workdir):
            captured["session_name"] = session_name
            captured["api_id"] = api_id
            captured["api_hash"] = api_hash
            captured["sleep_threshold"] = sleep_threshold
            captured["workdir"] = workdir

    monkeypatch.setattr(telegram, "Client", FakeClient)
    client = telegram.get_client()

    assert isinstance(client, FakeClient)
    assert captured == {
        "session_name": "my_session",
        "api_id": 12345,
        "api_hash": "hash123",
        "sleep_threshold": 60,
        "workdir": Path.cwd(),
    }


def test_get_chat_with_retry_waits_out_flood_wait(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)
    slept: list[int] = []
    warnings: list[str] = []

    class FakeLogger:
        def warning(self, msg):
            warnings.append(msg)

    class FakeTGClient:
        def __init__(self):
            self.calls = 0

        async def get_chat(self, username):
            self.calls += 1
            if self.calls == 1:
                raise fake_pyrogram.FakeFloodWait(2)
            return SimpleNamespace(username=username)

    async def sleep_fn(seconds):
        slept.append(seconds)

    async def _run():
        tg_client = FakeTGClient()
        chat = await telegram.get_chat_with_retry(
            tg_client, "lenin_crew", FakeLogger(), sleep_fn
        )
        return tg_client.calls, chat

    calls, chat = asyncio.run(_run())

    assert calls == 2
    assert slept == [3]
    assert chat.username == "lenin_crew"
    assert any("lenin_crew" in msg for msg in warnings)


def test_fetch_messages_filters_invalid_and_maps_fields(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)

    valid_msg = SimpleNamespace(
        text="Hello",
        from_user=SimpleNamespace(id=10, username="alice"),
        id=777,
        date="2025-02-15 10:00:00",
    )
    no_text_msg = SimpleNamespace(
        text=None,
        from_user=SimpleNamespace(id=11, username="bob"),
        id=778,
        date="2025-02-15 10:01:00",
    )
    no_user_msg = SimpleNamespace(
        text="World",
        from_user=None,
        id=779,
        date="2025-02-15 10:02:00",
    )

    class FakeTGClient:
        def __init__(self):
            self.calls = []

        async def get_chat_history(self, chat_id, limit):
            self.calls.append((chat_id, limit))
            for msg in [no_text_msg, no_user_msg, valid_msg]:
                yield msg

    async def _run():
        tg_client = FakeTGClient()
        result = []
        async for row in telegram.fetch_messages(tg_client, 42):
            result.append(row)
        return tg_client.calls, result

    calls, result = asyncio.run(_run())
    assert calls == [(42, 2)]
    assert result == [
        telegram.CollectedMessage(
            tg_id=10,
            username="alice",
            message_id=777,
            date="2025-02-15 10:00:00",
            text="Hello",
        )
    ]


def test_resolve_user_returns_all_name_fields(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)

    class FakeTGClient:
        def __init__(self):
            self.calls = []

        async def get_users(self, user_ref):
            self.calls.append(user_ref)
            return SimpleNamespace(
                id=555, username=None, first_name="Хрюкало", last_name="Офф"
            )

    async def _run():
        tg_client = FakeTGClient()
        resolved = await telegram.resolve_user(tg_client, 555)
        return tg_client.calls, resolved

    calls, resolved = asyncio.run(_run())
    assert calls == [555]
    assert resolved == telegram.TelegramUser(
        tg_id=555,
        username=None,
        first_name="Хрюкало",
        last_name="Офф",
    )


def test_resolve_user_raises_runtime_error_on_failure(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)

    class FakeTGClient:
        async def get_users(self, user_ref):
            raise KeyError("ID not found: nobody")

    async def _run():
        return await telegram.resolve_user(FakeTGClient(), "nobody")

    try:
        asyncio.run(_run())
    except RuntimeError as exc:
        assert "nobody" in str(exc)
    else:
        raise AssertionError("resolve_user must raise RuntimeError")


def test_resolve_user_lets_fatal_session_errors_through(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)

    class FakeTGClient:
        async def get_users(self, user_ref):
            raise FakeUnauthorized("SESSION_REVOKED")

    async def _run():
        return await telegram.resolve_user(FakeTGClient(), "vasya")

    with pytest.raises(FakeUnauthorized):
        asyncio.run(_run())


def test_fetch_user_messages_filters_and_maps_fields(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)

    valid_msg = SimpleNamespace(
        text="Hello",
        id=777,
        date="2025-02-15 10:00:00",
        from_user=SimpleNamespace(id=555),
    )
    no_text_msg = SimpleNamespace(
        text=None,
        id=778,
        date="2025-02-15 10:01:00",
        from_user=SimpleNamespace(id=555),
    )
    other_user_msg = SimpleNamespace(
        text="Not mine",
        id=779,
        date="2025-02-15 10:02:00",
        from_user=SimpleNamespace(id=999),
    )
    anonymous_msg = SimpleNamespace(
        text="Anonymous",
        id=780,
        date="2025-02-15 10:03:00",
        from_user=None,
    )

    class FakeTGClient:
        def __init__(self):
            self.calls = []

        async def search_messages(self, chat_id, from_user=None, limit=None):
            self.calls.append((chat_id, from_user, limit))
            for msg in [no_text_msg, other_user_msg, anonymous_msg, valid_msg]:
                yield msg

    async def _run():
        tg_client = FakeTGClient()
        result = []
        async for row in telegram.fetch_user_messages(tg_client, 42, 555):
            result.append(row)
        return tg_client.calls, result

    calls, result = asyncio.run(_run())
    assert calls == [(42, 555, 0)]
    assert result == [
        telegram.UserComment(
            message_id=777,
            date="2025-02-15 10:00:00",
            text="Hello",
        )
    ]


def test_find_chat_members_maps_fields_and_skips_memberless_entries(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)

    class FakeTGClient:
        def __init__(self):
            self.calls = []

        async def get_chat_members(self, chat_id, query=""):
            self.calls.append((chat_id, query))
            yield SimpleNamespace(user=None)  # anonymous admin: no user behind it
            yield SimpleNamespace(
                user=SimpleNamespace(
                    id=555,
                    username="hryukalo",
                    first_name="Хрюкало",
                    last_name="Офф",
                )
            )

    async def _run():
        tg_client = FakeTGClient()
        found = [
            row async for row in telegram.find_chat_members(tg_client, 42, "Хрюкало")
        ]
        return tg_client.calls, found

    calls, found = asyncio.run(_run())
    assert calls == [(42, "Хрюкало")]
    assert found == [
        telegram.TelegramUser(
            tg_id=555,
            username="hryukalo",
            first_name="Хрюкало",
            last_name="Офф",
        )
    ]


@pytest.mark.parametrize(
    "query",
    ["Хрюкало", "хрюкало", "  ХРЮКАЛО  ", "офф", "hryukalo", "Хрюкало Офф"],
)
def test_find_history_authors_matches_any_name_field(monkeypatch, query: str):
    telegram = _load_telegram_module(monkeypatch)

    target = SimpleNamespace(
        id=555, username="hryukalo", first_name="Хрюкало", last_name="Офф"
    )
    other = SimpleNamespace(
        id=999, username="petya", first_name="Пётр", last_name="Иванов"
    )

    class FakeTGClient:
        def __init__(self):
            self.calls = []

        async def get_chat_history(self, chat_id, limit):
            self.calls.append((chat_id, limit))
            for user in [other, target, None, target]:  # target repeats, None is anon
                yield SimpleNamespace(from_user=user)

    async def _run():
        tg_client = FakeTGClient()
        found = [
            row async for row in telegram.find_history_authors(tg_client, 42, query)
        ]
        return tg_client.calls, found

    calls, found = asyncio.run(_run())
    assert calls == [(42, 2)]  # LIMIT from config
    assert found == [
        telegram.TelegramUser(
            tg_id=555,
            username="hryukalo",
            first_name="Хрюкало",
            last_name="Офф",
        )
    ]


def test_find_history_authors_ignores_missing_name_parts(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)

    class FakeTGClient:
        async def get_chat_history(self, chat_id, limit):
            yield SimpleNamespace(
                from_user=SimpleNamespace(
                    id=1, username=None, first_name="Хрюкало", last_name=None
                )
            )

    async def _run():
        return [
            row async for row in telegram.find_history_authors(FakeTGClient(), 42, "хрю")
        ]

    assert asyncio.run(_run()) == [
        telegram.TelegramUser(
            tg_id=1,
            username=None,
            first_name="Хрюкало",
            last_name=None,
        )
    ]


def test_fetch_mentions_yields_forwards_and_text(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)

    class FakeTGClient:
        def __init__(self):
            self.calls = []

        async def get_chat_history(self, chat_id, limit):
            self.calls.append((chat_id, limit))
            # a repost of another channel
            yield SimpleNamespace(
                text="Смотрите",
                caption=None,
                forward_from_chat=SimpleNamespace(username="lenin_crew"),
            )
            # media post: the text lives in caption
            yield SimpleNamespace(
                text=None,
                caption="ссылка t.me/rud01vb",
                forward_from_chat=None,
            )
            # forward from a private channel: nothing to resolve by name
            yield SimpleNamespace(
                text="Аноним",
                caption=None,
                forward_from_chat=SimpleNamespace(username=None),
            )

    async def _run():
        tg_client = FakeTGClient()
        rows = [row async for row in telegram.fetch_mentions(tg_client, 42)]
        return tg_client.calls, rows

    calls, rows = asyncio.run(_run())
    assert calls == [(42, 2)]  # LIMIT from config
    assert rows == [
        telegram.MentionRow(forward_channel="lenin_crew", text="Смотрите"),
        telegram.MentionRow(forward_channel=None, text="ссылка t.me/rud01vb"),
        telegram.MentionRow(forward_channel=None, text="Аноним"),
    ]


def _chat(chat_type, linked=True, title="Ленин Крю", members=1234):
    return SimpleNamespace(
        type=chat_type,
        title=title,
        members_count=members,
        linked_chat=SimpleNamespace(id=-1001) if linked else None,
    )


def test_describe_channel_accepts_channel_with_discussion(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)

    class FakeTGClient:
        def __init__(self):
            self.calls = []

        async def get_chat(self, username):
            self.calls.append(username)
            return _chat(fake_pyrogram.enums.ChatType.CHANNEL)

    async def _run():
        tg_client = FakeTGClient()
        info = await telegram.describe_channel(tg_client, "lenin_crew")
        return tg_client.calls, info

    calls, info = asyncio.run(_run())
    assert calls == ["lenin_crew"]
    assert info == telegram.ChannelInfo(
        username="lenin_crew",
        title="Ленин Крю",
        members=1234,
    )


@pytest.mark.parametrize(
    ("chat_type", "linked", "reason"),
    [
        # a channel without a linked chat has no comments to collect
        (fake_pyrogram.enums.ChatType.CHANNEL, False, "no discussion"),
        # a discussion supergroup also has a linked_chat (the channel!), and
        # collecting it would store channel posts as if they were comments
        (fake_pyrogram.enums.ChatType.SUPERGROUP, True, "supergroup"),
        (fake_pyrogram.enums.ChatType.GROUP, True, "group"),
    ],
)
def test_describe_channel_rejects_unusable_chats(monkeypatch, chat_type, linked, reason):
    telegram = _load_telegram_module(monkeypatch)

    class FakeTGClient:
        async def get_chat(self, username):
            return _chat(chat_type, linked=linked)

    async def _run():
        return await telegram.describe_channel(FakeTGClient(), "whatever")

    assert asyncio.run(_run()) is None, reason


def test_describe_channel_returns_none_on_resolve_failure(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)

    class FakeTGClient:
        async def get_chat(self, username):
            raise KeyError("USERNAME_NOT_OCCUPIED")

    async def _run():
        return await telegram.describe_channel(FakeTGClient(), "ghost_channel")

    assert asyncio.run(_run()) is None


def test_describe_channel_lets_flood_wait_through(monkeypatch):
    # A rate limit says nothing about the channel: swallowing it would drop a
    # perfectly good candidate. The caller decides to wait and retry.
    telegram = _load_telegram_module(monkeypatch)

    class FakeTGClient:
        async def get_chat(self, username):
            raise fake_pyrogram.FakeFloodWait(21)

    async def _run():
        return await telegram.describe_channel(FakeTGClient(), "redyurt_tor")

    with pytest.raises(fake_pyrogram.FakeFloodWait):
        asyncio.run(_run())


def test_describe_channel_lets_fatal_session_errors_through(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)

    class FakeTGClient:
        async def get_chat(self, username):
            raise FakeUnauthorized("SESSION_REVOKED")

    async def _run():
        return await telegram.describe_channel(FakeTGClient(), "lenin_crew")

    with pytest.raises(FakeUnauthorized):
        asyncio.run(_run())


def test_fatal_tg_errors_come_from_pyrogram(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)

    assert FakeUnauthorized in telegram.FATAL_TG_ERRORS
    assert FakeAuthKeyDuplicated in telegram.FATAL_TG_ERRORS
