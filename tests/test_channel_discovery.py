import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fake_pyrogram import FakeFloodWait, FakeUnauthorized

from parser.channel_discovery import (
    DiscoveryConfig,
    DiscoveryDeps,
    append_channels,
    discover_channels,
    extract_usernames,
)
from parser.telegram import ChannelInfo, MentionRow


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # plain links, with and without scheme
        ("смотри https://t.me/lenin_crew", {"lenin_crew"}),
        ("t.me/lenin_crew", {"lenin_crew"}),
        ("http://t.me/lenin_crew подписывайтесь", {"lenin_crew"}),
        # a link to a specific post still names the channel
        ("t.me/lenin_crew/1234", {"lenin_crew"}),
        # mentions
        ("@spichka_media и @rud01vb", {"spichka_media", "rud01vb"}),
        # usernames are case-insensitive in Telegram
        ("T.ME/rud01vb", {"rud01vb"}),
        ("@Lenin_Crew", {"lenin_crew"}),
        # both forms at once, deduplicated
        ("@rud01vb и t.me/rud01vb", {"rud01vb"}),
        # invite links point at no resolvable username
        ("t.me/+AbCdEfGhIj", set()),
        ("https://t.me/joinchat/AAAAAFFF", set()),
        # service paths are not channels
        ("t.me/s/durov", set()),
        ("t.me/c/1234567890/5", set()),
        ("t.me/addstickers/pack", set()),
        # too short for a Telegram username (min 5)
        ("@abc", set()),
        ("t.me/abcd", set()),
        # an email is not a mention
        ("пишите на user@example.com", set()),
        # nothing to find
        ("обычный текст без ссылок", set()),
        ("", set()),
        (None, set()),
    ],
)
def test_extract_usernames(text, expected):
    assert extract_usernames(text) == expected


def test_extract_usernames_finds_several_in_one_text():
    text = (
        "репост из t.me/lenin_crew, ещё @spichka_media, "
        "и https://t.me/vihod_est/42 — вот"
    )
    assert extract_usernames(text) == {"lenin_crew", "spichka_media", "vihod_est"}


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


def _chat(chat_id: int, linked_chat_id: int | None):
    linked = SimpleNamespace(id=linked_chat_id) if linked_chat_id else None
    return SimpleNamespace(id=chat_id, linked_chat=linked)


def _deps(
    tg_client,
    logger,
    mentions: dict[int, list[dict]],
    usable: set[str] | None = None,
    scanned: list | None = None,
):
    async def fetch_mentions_fn(_tg_client, chat_id):
        if scanned is not None:
            scanned.append(chat_id)
        for row in mentions.get(chat_id, []):
            yield row

    async def describe_channel_fn(_tg_client, username):
        if usable is not None and username not in usable:
            return None
        return ChannelInfo(
            username=username,
            title=f"Title {username}",
            members=100,
        )

    return DiscoveryDeps(
        tg_client_factory=lambda: tg_client,
        fetch_mentions_fn=fetch_mentions_fn,
        describe_channel_fn=describe_channel_fn,
        logger_factory=lambda _name: logger,
    )


def _row(forward=None, text=None):
    return MentionRow(forward_channel=forward, text=text)


def test_discover_scans_both_the_channel_and_its_discussion():
    tg_client = FakeTGClient({"chan_a": _chat(100, 1001)})
    scanned: list = []
    mentions = {
        100: [_row(forward="from_post")],  # channel posts
        1001: [_row(text="а вот t.me/from_comment")],  # its discussion
    }

    found = asyncio.run(
        discover_channels(
            DiscoveryConfig(channels=["chan_a"]),
            _deps(tg_client, FakeLogger(), mentions, scanned=scanned),
        )
    )

    assert scanned == [100, 1001]
    assert set(found) == {"from_post", "from_comment"}


def test_discover_skips_already_known_channels_and_self_reposts():
    tg_client = FakeTGClient({"chan_a": _chat(100, 1001), "chan_b": _chat(200, 2001)})
    mentions = {
        # the discussion mirrors the channel's own posts: forward == chan_a
        1001: [_row(forward="chan_a"), _row(text="@chan_b и t.me/newbie")],
        100: [],
        200: [],
        2001: [],
    }

    found = asyncio.run(
        discover_channels(
            DiscoveryConfig(channels=["chan_a", "chan_b"]),
            _deps(tg_client, FakeLogger(), mentions),
        )
    )

    assert found == ["newbie"]


def test_discover_validates_most_mentioned_first_and_stops_at_target():
    tg_client = FakeTGClient({"chan_a": _chat(100, 1001)})
    mentions = {
        100: [
            _row(text="@popular_one"),
            _row(text="@popular_one"),
            _row(text="@popular_one @rare_one"),
            _row(text="@middle_one"),
            _row(text="@middle_one"),
        ],
        1001: [],
    }

    # target=3 with one known channel leaves room for exactly two more
    found = asyncio.run(
        discover_channels(
            DiscoveryConfig(channels=["chan_a"], target=3),
            _deps(tg_client, FakeLogger(), mentions),
        )
    )

    # rare_one is a valid candidate but the target is already met
    assert found == ["popular_one", "middle_one"]


def test_discover_drops_candidates_without_discussion():
    tg_client = FakeTGClient({"chan_a": _chat(100, 1001)})
    mentions = {100: [_row(text="@good_one @bad_one")], 1001: []}

    found = asyncio.run(
        discover_channels(
            DiscoveryConfig(channels=["chan_a"]),
            _deps(tg_client, FakeLogger(), mentions, usable={"good_one"}),
        )
    )

    assert found == ["good_one"]


def test_discover_scans_channel_without_discussion_and_warns():
    tg_client = FakeTGClient({"chan_a": _chat(100, None)})
    scanned: list = []
    mentions = {100: [_row(text="@newbie")]}
    logger = FakeLogger()

    found = asyncio.run(
        discover_channels(
            DiscoveryConfig(channels=["chan_a"]),
            _deps(tg_client, logger, mentions, scanned=scanned),
        )
    )

    # posts are still a source even when the channel has no discussion
    assert scanned == [100]
    assert found == ["newbie"]
    assert any("chan_a" in msg for msg in logger.warnings)


def test_discover_keeps_going_after_channel_error():
    class BrokenChatClient(FakeTGClient):
        async def get_chat(self, channel_username: str):
            if channel_username == "chan_broken":
                raise RuntimeError("CHANNEL_INVALID")
            return await super().get_chat(channel_username)

    tg_client = BrokenChatClient({"chan_a": _chat(100, 1001)})
    logger = FakeLogger()
    mentions = {100: [_row(text="@newbie")], 1001: []}

    found = asyncio.run(
        discover_channels(
            DiscoveryConfig(channels=["chan_broken", "chan_a"]),
            _deps(tg_client, logger, mentions),
        )
    )

    assert found == ["newbie"]
    assert any("chan_broken" in msg for msg in logger.exceptions)


def test_discover_aborts_on_fatal_session_error():
    class DeadSessionClient(FakeTGClient):
        async def get_chat(self, channel_username: str):
            raise FakeUnauthorized("SESSION_REVOKED")

    logger = FakeLogger()

    with pytest.raises(FakeUnauthorized):
        asyncio.run(
            discover_channels(
                DiscoveryConfig(channels=["chan_a"]),
                _deps(DeadSessionClient({}), logger, {}),
            )
        )

    assert logger.exceptions == []


def _deps_with_describe(tg_client, logger, mentions, describe_channel_fn, slept=None):
    async def sleep_fn(seconds):
        if slept is not None:
            slept.append(seconds)

    return DiscoveryDeps(
        tg_client_factory=lambda: tg_client,
        fetch_mentions_fn=_deps(tg_client, logger, mentions).fetch_mentions_fn,
        describe_channel_fn=describe_channel_fn,
        logger_factory=lambda _name: logger,
        sleep_fn=sleep_fn,
    )


def test_discover_waits_out_flood_wait_and_retries_the_candidate():
    tg_client = FakeTGClient({"chan_a": _chat(100, 1001)})
    logger = FakeLogger()
    mentions = {100: [_row(text="@wanted_one")], 1001: []}
    slept: list = []
    attempts: list = []

    async def describe_channel_fn(_tg_client, username):
        attempts.append(username)
        if len(attempts) == 1:
            raise FakeFloodWait(21)
        return ChannelInfo(username=username, title="Wanted", members=10)

    found = asyncio.run(
        discover_channels(
            DiscoveryConfig(channels=["chan_a"]),
            _deps_with_describe(
                tg_client, logger, mentions, describe_channel_fn, slept
            ),
        )
    )

    # the candidate survives the rate limit instead of being written off
    assert attempts == ["wanted_one", "wanted_one"]
    assert slept == [22]  # the 21s Telegram asked for, plus a second of slack
    assert found == ["wanted_one"]


def test_discover_gives_up_on_candidate_that_floods_twice():
    tg_client = FakeTGClient({"chan_a": _chat(100, 1001)})
    logger = FakeLogger()
    mentions = {100: [_row(text="@stubborn_one"), _row(text="@other_one")], 1001: []}
    attempts: list = []

    async def describe_channel_fn(_tg_client, username):
        attempts.append(username)
        if username == "stubborn_one":
            raise FakeFloodWait(5)
        return ChannelInfo(username=username, title="Other", members=10)

    found = asyncio.run(
        discover_channels(
            DiscoveryConfig(channels=["chan_a"]),
            _deps_with_describe(tg_client, logger, mentions, describe_channel_fn),
        )
    )

    # two attempts on the stubborn one, then the sweep moves on
    assert attempts.count("stubborn_one") == 2
    assert found == ["other_one"]
    assert any("stubborn_one" in msg for msg in logger.warnings)


def test_discover_logs_found_channels_when_session_dies_while_validating():
    tg_client = FakeTGClient({"chan_a": _chat(100, 1001)})
    logger = FakeLogger()
    mentions = {
        100: [
            _row(text="@popular_one"),
            _row(text="@popular_one"),
            _row(text="@second_one"),
        ],
        1001: [],
    }

    async def describe_channel_fn(_tg_client, username):
        if username == "popular_one":
            return ChannelInfo(username=username, title="Popular", members=10)
        raise FakeUnauthorized("SESSION_REVOKED")

    deps = DiscoveryDeps(
        tg_client_factory=lambda: tg_client,
        fetch_mentions_fn=_deps(tg_client, logger, mentions).fetch_mentions_fn,
        describe_channel_fn=describe_channel_fn,
        logger_factory=lambda _name: logger,
    )

    with pytest.raises(FakeUnauthorized):
        asyncio.run(discover_channels(DiscoveryConfig(channels=["chan_a"]), deps))

    # the sweep is lost, so what it already found must survive in the log
    assert any("popular_one" in msg for msg in logger.errors)


def test_discover_rejects_empty_channels():
    with pytest.raises(RuntimeError):
        asyncio.run(
            discover_channels(
                DiscoveryConfig(channels=[]),
                _deps(FakeTGClient({}), FakeLogger(), {}),
            )
        )


def test_append_channels_adds_new_and_keeps_order(tmp_path: Path):
    path = tmp_path / "channels.json"
    path.write_text(json.dumps(["rud01vb", "d_tyazhkun"], indent=2) + "\n")

    added = append_channels(path, ["newbie", "another"])

    assert added == 2
    assert json.loads(path.read_text()) == [
        "rud01vb",
        "d_tyazhkun",
        "newbie",
        "another",
    ]


def test_append_channels_does_not_duplicate_regardless_of_case(tmp_path: Path):
    path = tmp_path / "channels.json"
    path.write_text(json.dumps(["rud01vb"], indent=2) + "\n")

    added = append_channels(path, ["rud01vb", "newbie", "newbie"])

    assert added == 1
    assert json.loads(path.read_text()) == ["rud01vb", "newbie"]


def test_append_channels_leaves_file_untouched_when_nothing_new(tmp_path: Path):
    path = tmp_path / "channels.json"
    original = json.dumps(["rud01vb"], indent=2) + "\n"
    path.write_text(original)
    mtime_before = path.stat().st_mtime_ns

    assert append_channels(path, ["rud01vb"]) == 0
    # not merely "same content": the file must not be rewritten at all
    assert path.stat().st_mtime_ns == mtime_before
    assert path.read_text() == original
    assert list(tmp_path.iterdir()) == [path]  # no leftover temp file
