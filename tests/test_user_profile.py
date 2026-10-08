from datetime import datetime, timezone

from parser.user_profile import (
    UserComment,
    UserProfile,
    channel_shares,
    render_user_comments_text,
)


def _profile(username=None, display_name=None) -> UserProfile:
    return UserProfile(
        tg_id=7,
        username=username,
        display_name=display_name,
        total_messages=0,
        channel_count=0,
        channels=[],
    )


def test_channel_shares_are_donut_segments_in_the_given_order():
    first, second = channel_shares([("chan_a", 3), ("chan_b", 1)])

    assert (first.name, first.message_count, first.percent) == ("chan_a", 3, 75.0)
    assert (first.dasharray, first.dashoffset) == ("75.0 25.0", 0.0)
    assert (second.name, second.message_count, second.percent) == ("chan_b", 1, 25.0)
    # The second segment starts where the first one ends.
    assert (second.dasharray, second.dashoffset) == ("25.0 75.0", -75.0)
    assert first.color != second.color


def test_channel_shares_close_the_rounding_gap():
    shares = channel_shares([("a", 1), ("b", 1), ("c", 1)])

    assert [s.percent for s in shares] == [33.3, 33.3, 33.4]
    assert round(sum(s.percent for s in shares), 1) == 100


def test_channel_shares_reuse_colors_beyond_the_palette():
    shares = channel_shares([(f"chan_{i}", 1) for i in range(12)])

    assert shares[10].color == shares[0].color
    assert len({s.color for s in shares[:10]}) == 10


def test_channel_shares_of_no_comments_are_empty():
    assert channel_shares([]) == []


def test_display_username_prefers_username_then_name():
    assert _profile("vasya", "Вася Пупкин").display_username == "@vasya"
    assert _profile(None, "Хрюкало Офф").display_username == "Хрюкало Офф"
    assert _profile(None, None).display_username == "нет ника"


def test_render_user_comments_text_joins_entries_with_blank_line():
    when = datetime(2026, 8, 1, tzinfo=timezone.utc)
    comments = [
        UserComment(channel="chan_a", message_id=1, date=when, text="hello"),
        UserComment(channel="chan_b", message_id=2, date=when, text="multi\nline"),
    ]

    assert render_user_comments_text(comments) == "hello\n\nmulti\nline"
    assert render_user_comments_text([]) == ""
