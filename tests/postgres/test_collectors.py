from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fake_pyrogram import FakeUnauthorized
from sqlalchemy import select

from db.models import Channel, Message, User
from parser.collector import CollectorConfig, CollectorDeps, collect_db
from parser.telegram import CollectedMessage, TelegramUser, UserComment
from parser.user_collector import (
    ChannelProgress,
    UserCollectorConfig,
    UserCollectorDeps,
    collect_user_comments,
)


class FakeTGClient:
    """get_chat answers from `chats`; an exception stored there is raised."""

    def __init__(self, chats: dict[str, object]):
        self.chats = chats
        self.get_chat_calls: list[str] = []
        self.searched: list[tuple[int, int]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get_chat(self, channel_username: str):
        self.get_chat_calls.append(channel_username)
        chat = self.chats[channel_username]
        if isinstance(chat, Exception):
            raise chat
        return chat


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


def _chat(linked_chat_id: int | None, chat_id: int = 1):
    linked = SimpleNamespace(id=linked_chat_id) if linked_chat_id else None
    return SimpleNamespace(id=chat_id, linked_chat=linked)


def _when(day: int) -> datetime:
    return datetime(2025, 2, day, 10, 0, tzinfo=timezone.utc)


def _comment(message_id: int, text: str, day: int = 1) -> UserComment:
    return UserComment(message_id=message_id, text=text, date=_when(day))


def _user_deps(tg_client, logger, messages, resolved=None, **callbacks):
    resolved = (
        TelegramUser(tg_id=555, username="vasya", first_name=None, last_name=None)
        if resolved is None
        else resolved
    )
    resolve_calls = []

    async def resolve_user_fn(_tg_client, user_ref):
        resolve_calls.append(user_ref)
        if isinstance(resolved, Exception):
            raise resolved
        return resolved

    async def fetch_user_messages_fn(_tg_client, chat_id, tg_id):
        tg_client.searched.append((chat_id, tg_id))
        found = messages.get(chat_id, [])
        if isinstance(found, Exception):
            raise found
        for msg in found:
            yield msg

    deps = UserCollectorDeps(
        tg_client_factory=lambda: tg_client,
        fetch_user_messages_fn=fetch_user_messages_fn,
        resolve_user_fn=resolve_user_fn,
        logger_factory=lambda _name: logger,
        **callbacks,
    )
    return deps, resolve_calls


async def _rows(sessions):
    async with sessions() as session:
        result = await session.execute(
            select(
                User.tg_id,
                Channel.username,
                Message.tg_message_id,
                Message.text,
                Message.date,
            )
            .join(User, User.id == Message.user_id)
            .join(Channel, Channel.id == Message.channel_id)
            .order_by(Channel.username, Message.tg_message_id)
        )
        return [tuple(row) for row in result]


async def _users(sessions):
    async with sessions() as session:
        result = await session.execute(
            select(
                User.tg_id, User.username, User.profile_collected_at.is_not(None)
            ).order_by(User.tg_id)
        )
        return [tuple(row) for row in result]


# --- user-comments -----------------------------------------------------------


def test_user_comments_stores_rows_and_skips_channel_without_discussion(run_db):
    tg_client = FakeTGClient(
        {"chan_a": _chat(1001, chat_id=77), "chan_no_discussion": _chat(None)}
    )
    logger = FakeLogger()
    messages = {1001: [_comment(10, "Первый КОММЕНТ", 1), _comment(11, "Второй", 2)]}

    async def scenario(sessions):
        deps, _ = _user_deps(tg_client, logger, messages)
        result = await collect_user_comments(
            sessions,
            UserCollectorConfig(channels=["chan_a", "chan_no_discussion"]),
            "@vasya",
            deps,
        )
        async with sessions() as session:
            channels = (
                await session.execute(
                    select(
                        Channel.username,
                        Channel.telegram_chat_id,
                        Channel.linked_chat_id,
                    )
                )
            ).all()
        return result, await _rows(sessions), await _users(sessions), channels

    result, rows, users, channels = run_db(scenario)

    # The text is stored as Telegram gave it.
    assert rows == [
        (555, "chan_a", 10, "Первый КОММЕНТ", _when(1)),
        (555, "chan_a", 11, "Второй", _when(2)),
    ]
    assert users == [(555, "vasya", True)]
    assert [tuple(c) for c in channels] == [("chan_a", 77, 1001)]
    assert (result.tg_id, result.username) == (555, "vasya")
    assert (result.channels_scanned, result.channels_failed) == (2, 0)
    assert (result.fetched, result.new, result.duplicates) == (2, 2, 0)
    assert tg_client.searched == [(1001, 555)]
    assert any("chan_no_discussion" in msg for msg in logger.warnings)


def test_user_comments_second_run_adds_nothing_and_does_not_resolve_again(run_db):
    messages = {1001: [_comment(10, "Первый")]}

    async def scenario(sessions):
        results = []
        resolve_calls = []
        # First by username, then by tg_id and by another case of the username.
        for user_ref in ("vasya", 555, "VASYA"):
            deps, calls = _user_deps(
                FakeTGClient({"chan_a": _chat(1001)}), FakeLogger(), messages
            )
            results.append(
                await collect_user_comments(
                    sessions, UserCollectorConfig(channels=["chan_a"]), user_ref, deps
                )
            )
            resolve_calls.append(calls)
        return results, resolve_calls, await _rows(sessions), await _users(sessions)

    results, resolve_calls, rows, users = run_db(scenario)

    assert [(r.fetched, r.new, r.duplicates) for r in results] == [
        (1, 1, 0),
        (1, 0, 1),
        (1, 0, 1),
    ]
    assert len(rows) == 1
    assert users == [(555, "vasya", True)]
    # Telegram rate-limits lookups: a stored profile is not resolved again.
    assert resolve_calls == [["vasya"], [], []]
    assert "duplicates: 1" in results[1].render()


def test_user_comments_keeps_stored_text_unless_asked_to_refresh(run_db):
    async def scenario(sessions):
        async def collect(text, refresh_text):
            deps, _ = _user_deps(
                FakeTGClient({"chan_a": _chat(1001)}),
                FakeLogger(),
                {1001: [_comment(10, text), _comment(10, text)]},
            )
            result = await collect_user_comments(
                sessions,
                UserCollectorConfig(channels=["chan_a"], refresh_text=refresh_text),
                555,
                deps,
            )
            return result.new, (await _rows(sessions))[0][3]

        return [
            await collect("привет", False),
            await collect("Привет", False),
            await collect("Привет", True),
        ]

    assert run_db(scenario) == [(1, "привет"), (0, "привет"), (0, "Привет")]


def test_user_comments_creates_nothing_when_user_unresolved(run_db):
    async def scenario(sessions):
        deps, _ = _user_deps(
            FakeTGClient({"chan_a": _chat(1001)}),
            FakeLogger(),
            {},
            resolved=RuntimeError("Cannot resolve user '@ghost'"),
        )
        with pytest.raises(RuntimeError):
            await collect_user_comments(
                sessions, UserCollectorConfig(channels=["chan_a"]), "@ghost", deps
            )
        return await _users(sessions)

    assert run_db(scenario) == []


def test_user_without_comments_still_becomes_a_profile(run_db):
    async def scenario(sessions):
        deps, _ = _user_deps(FakeTGClient({"chan_a": _chat(1001)}), FakeLogger(), {})
        result = await collect_user_comments(
            sessions, UserCollectorConfig(channels=["chan_a"]), 555, deps
        )
        return result, await _users(sessions)

    result, users = run_db(scenario)

    assert (result.fetched, result.new) == (0, 0)
    assert users == [(555, "vasya", True)]


def test_user_comments_keeps_going_after_channel_errors(run_db):
    tg_client = FakeTGClient(
        {
            "chan_first": _chat(1001),
            "chan_broken": RuntimeError("CHANNEL_INVALID"),
            "chan_fetch_fails": _chat(1002),
            "chan_last": _chat(1003),
        }
    )
    logger = FakeLogger()
    messages = {
        1001: [_comment(10, "Первый")],
        1002: RuntimeError("search failed"),
        1003: [_comment(10, "Последний")],
    }
    events: list[ChannelProgress] = []

    async def scenario(sessions):
        deps, _ = _user_deps(
            tg_client, logger, messages, on_channel_progress=events.append
        )
        result = await collect_user_comments(
            sessions,
            UserCollectorConfig(channels=list(tg_client.chats)),
            "@vasya",
            deps,
        )
        return result, await _rows(sessions)

    result, rows = run_db(scenario)

    # Both good channels are stored: a failure rolls back nothing of theirs.
    assert [(r[1], r[3]) for r in rows] == [
        ("chan_first", "Первый"),
        ("chan_last", "Последний"),
    ]
    assert (result.channels_scanned, result.channels_failed, result.new) == (4, 2, 2)
    assert logger.exceptions == [
        "[chan_broken] failed, skipped",
        "[chan_fetch_fails] failed, skipped",
    ]
    assert [(e.channel, e.status, e.saved) for e in events] == [
        ("chan_first", "started", 0),
        ("chan_first", "done", 1),
        ("chan_broken", "started", 0),
        ("chan_broken", "failed", 0),
        ("chan_fetch_fails", "started", 0),
        ("chan_fetch_fails", "failed", 0),
        ("chan_last", "started", 0),
        ("chan_last", "done", 1),
    ]
    assert events[3].error == "CHANNEL_INVALID"


def test_user_comments_aborts_on_dead_session_and_keeps_what_was_saved(run_db):
    tg_client = FakeTGClient(
        {
            "chan_a": _chat(1001),
            "chan_b": FakeUnauthorized("SESSION_REVOKED"),
            "chan_c": _chat(1003),
        }
    )
    logger = FakeLogger()
    messages = {1001: [_comment(10, "Первый"), _comment(11, "Второй")]}

    async def scenario(sessions):
        deps, _ = _user_deps(tg_client, logger, messages)
        with pytest.raises(FakeUnauthorized):
            await collect_user_comments(
                sessions,
                UserCollectorConfig(channels=["chan_a", "chan_b", "chan_c"]),
                "@vasya",
                deps,
            )
        return await _rows(sessions)

    rows = run_db(scenario)

    assert len(rows) == 2
    assert tg_client.get_chat_calls == ["chan_a", "chan_b"]
    assert logger.exceptions == []
    assert any("chan_b" in msg and "2 rows" in msg for msg in logger.errors)


def test_user_comments_reports_the_resolved_user(run_db):
    resolved_users = []

    async def scenario(sessions):
        deps, _ = _user_deps(
            FakeTGClient({"chan_a": _chat(1001)}),
            FakeLogger(),
            {},
            resolved=TelegramUser(
                tg_id=555, username=None, first_name="Хрюкало", last_name="Офф"
            ),
            on_user_resolved=resolved_users.append,
        )
        for _ in range(2):
            await collect_user_comments(
                sessions, UserCollectorConfig(channels=["chan_a"]), 555, deps
            )

    run_db(scenario)

    # The second run reads the stored profile and reports the same names.
    assert [(u.tg_id, u.first_name, u.last_name) for u in resolved_users] == [
        (555, "Хрюкало", "Офф"),
        (555, "Хрюкало", "Офф"),
    ]


def test_user_comments_rejects_empty_channels(run_db):
    async def scenario(sessions):
        with pytest.raises(RuntimeError, match="CHANNELS are empty"):
            await collect_user_comments(sessions, UserCollectorConfig(channels=[]), 555)

    run_db(scenario)


# --- collect -----------------------------------------------------------------


def _history_deps(chats, logger, history):
    async def fetch_messages_fn(_tg_client, chat_id):
        found = history[chat_id]
        for msg in found:
            if isinstance(msg, Exception):
                raise msg
            yield msg

    return CollectorDeps(
        tg_client_factory=lambda: FakeTGClient(chats),
        fetch_messages_fn=fetch_messages_fn,
        logger_factory=lambda _name: logger,
    )


def _msg(tg_id, message_id, username="vasya", text="hello"):
    return CollectedMessage(
        tg_id=tg_id, username=username, message_id=message_id, date=_when(1), text=text
    )


def test_collect_skips_failed_channels_and_stores_the_rest(run_db):
    logger = FakeLogger()
    chats = {
        "dead": RuntimeError("USERNAME_NOT_OCCUPIED"),
        "alive": _chat(100),
        "breaks_midway": _chat(200),
        "no_discussion": _chat(None),
    }
    history = {
        100: [_msg(1, 1), _msg(2, 2, username="petya")],
        # One full batch, then the history breaks.
        200: [_msg(1, 1), _msg(1, 2), RuntimeError("FLOOD")],
    }

    async def scenario(sessions):
        new_rows = await collect_db(
            sessions,
            CollectorConfig(channels=list(chats), batch_size=2),
            _history_deps(chats, logger, history),
        )
        async with sessions() as session:
            linked = dict(
                (
                    await session.execute(
                        select(Channel.username, Channel.linked_chat_id)
                    )
                ).all()
            )
        return new_rows, await _rows(sessions), await _users(sessions), linked

    new_rows, rows, users, linked = run_db(scenario)

    assert linked == {"alive": 100, "breaks_midway": 200}
    assert [(r[0], r[1], r[2]) for r in rows] == [
        (1, "alive", 1),
        (2, "alive", 2),
        # Stored before the failure, and kept.
        (1, "breaks_midway", 1),
        (1, "breaks_midway", 2),
    ]
    # Authors seen in a channel are users, but not collected profiles.
    assert users == [(1, "vasya", False), (2, "petya", False)]
    assert new_rows == 2
    assert sorted(logger.exceptions) == [
        "[breaks_midway] failed, skipped",
        "[dead] failed, skipped",
    ]
    assert "2 of 4 channels failed" in logger.warnings
    assert "[dead] done, new 0" not in logger.infos


def test_collect_twice_adds_nothing_and_follows_a_renamed_author(run_db):
    chats = {"alive": _chat(100)}

    async def scenario(sessions):
        counts = []
        for username in ("vasya", "vasya_new"):
            history = {100: [_msg(1, n, username=username) for n in range(1, 8)]}
            counts.append(
                await collect_db(
                    sessions,
                    CollectorConfig(channels=["alive"], batch_size=3),
                    _history_deps(chats, FakeLogger(), history),
                )
            )
        return counts, await _rows(sessions), await _users(sessions)

    counts, rows, users = run_db(scenario)

    assert counts == [7, 0]
    assert len(rows) == 7
    assert users == [(1, "vasya_new", False)]


def test_collect_aborts_on_dead_session(run_db):
    logger = FakeLogger()
    chats = {"first": FakeUnauthorized("session revoked")}

    async def scenario(sessions):
        with pytest.raises(FakeUnauthorized):
            await collect_db(
                sessions,
                CollectorConfig(channels=["first"]),
                _history_deps(chats, logger, {}),
            )

    run_db(scenario)

    assert logger.exceptions == []


def test_author_seen_in_a_channel_is_resolved_when_their_comments_are_requested(
    run_db,
):
    async def scenario(sessions):
        await collect_db(
            sessions,
            CollectorConfig(channels=["alive"]),
            _history_deps({"alive": _chat(100)}, FakeLogger(), {100: [_msg(555, 1)]}),
        )
        deps, resolve_calls = _user_deps(
            FakeTGClient({"alive": _chat(100)}),
            FakeLogger(),
            {},
            resolved=TelegramUser(
                tg_id=555, username="vasya", first_name="Вася", last_name=None
            ),
        )
        await collect_user_comments(
            sessions, UserCollectorConfig(channels=["alive"]), 555, deps
        )
        async with sessions() as session:
            first_name = await session.scalar(select(User.first_name))
        return resolve_calls, first_name

    # Only a collected profile is trusted without asking Telegram: an author
    # row carries no name.
    assert run_db(scenario) == ([555], "Вася")


def test_collect_does_not_demote_a_collected_profile(run_db):
    async def scenario(sessions):
        deps, _ = _user_deps(FakeTGClient({"alive": _chat(100)}), FakeLogger(), {})
        await collect_user_comments(
            sessions, UserCollectorConfig(channels=["alive"]), 555, deps
        )
        await collect_db(
            sessions,
            CollectorConfig(channels=["alive"]),
            _history_deps({"alive": _chat(100)}, FakeLogger(), {100: [_msg(555, 1)]}),
        )
        return await _users(sessions)

    assert run_db(scenario) == [(555, "vasya", True)]
