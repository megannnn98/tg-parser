from datetime import datetime, timezone

import pytest
from web_auth import logged_in

from db import repositories as repo
from parser.political_coords import AggregatedCoords, AxisStats
from web.app import create_app


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _seed(run_db, users):
    """users: {tg_id: (username, first_name, [(channel, message id, text, date)])}."""

    async def scenario(sessions):
        async with sessions.begin() as session:
            for tg_id, (username, first_name, comments, collected) in users.items():
                user_id = await repo.upsert_user(session, tg_id, username, first_name)
                if collected:
                    await repo.mark_profiles_collected(
                        session, {user_id: _utc(2026, 9, 1)}
                    )
                rows = []
                for channel, message_id, text, date in comments:
                    rows.append(
                        {
                            "tg_message_id": message_id,
                            "user_id": user_id,
                            "channel_id": await repo.upsert_channel(session, channel),
                            "text": text,
                            "date": date,
                        }
                    )
                await repo.insert_messages(session, rows)

    run_db(scenario)


@pytest.fixture
def client(run_db, database_url, tmp_path):
    def make(users, tz="UTC"):
        _seed(run_db, users)
        app = create_app(
            database_url=database_url,
            channels=["chan_a"],
            frontend_dist=tmp_path / "missing",
            timezone=tz,
        )
        return logged_in(app)

    return make


VASYA = {
    7: (
        "vasya",
        None,
        [
            # Saturday 2026-08-01 and Sunday 2026-08-02, UTC.
            ("chan_a", 1, "hello", _utc(2026, 8, 1, 8, 0)),
            ("chan_a", 2, "world", _utc(2026, 8, 1, 14, 0)),
            ("chan_b", 3, "again", _utc(2026, 8, 2, 21, 30)),
        ],
        True,
    )
}


def test_profiles_lists_collected_users_only(client):
    users = {
        **VASYA,
        # No username: the name stands in. No comments: still a profile.
        8: (None, "Хрюкало", [], True),
        # Seen in a channel while collecting it, never requested.
        9: ("author", None, [("chan_a", 9, "x", _utc(2026, 8, 1, 8, 0))], False),
    }

    with client(users) as http:
        resp = http.get("/api/v1/profiles")

    assert resp.status_code == 200
    profiles = resp.json()
    assert [(p["tg_id"], p["display_username"]) for p in profiles] == [
        (7, "@vasya"),
        (8, "Хрюкало"),
    ]
    assert "db_name" not in profiles[0]
    assert (profiles[0]["total_messages"], profiles[0]["channel_count"]) == (3, 2)
    assert [(c["name"], c["message_count"]) for c in profiles[0]["channels"]] == [
        ("chan_a", 2),
        ("chan_b", 1),
    ]
    assert sum(c["percent"] for c in profiles[0]["channels"]) == 100
    assert (profiles[1]["total_messages"], profiles[1]["channels"]) == (0, [])


def test_user_detail_reports_profile_and_activity(client):
    with client(VASYA) as http:
        resp = http.get("/api/v1/users/7")

    assert resp.status_code == 200
    body = resp.json()
    assert body["profile"]["tg_id"] == 7
    assert body["profile"]["username"] == "vasya"
    assert body["profile"]["total_messages"] == 3
    assert body["profile"]["channel_count"] == 2
    hourly = {item["hour"]: item["count"] for item in body["hourly_activity"]}
    assert len(hourly) == 24
    assert {h: n for h, n in hourly.items() if n} == {8: 1, 14: 1, 21: 1}
    assert body["daily_activity"] == [
        {"date": "2026-08-01", "count": 2},
        {"date": "2026-08-02", "count": 1},
    ]
    assert len(body["weekly_activity"]) == 7 * 24
    assert [cell for cell in body["weekly_activity"] if cell["count"]] == [
        {"weekday": 5, "hour": 8, "count": 1},
        {"weekday": 5, "hour": 14, "count": 1},
        {"weekday": 6, "hour": 21, "count": 1},
    ]


def test_user_detail_counts_activity_in_the_configured_timezone(client):
    with client(VASYA, tz="Asia/Almaty") as http:
        body = http.get("/api/v1/users/7").json()

    # +05: Sunday 21:30 UTC is Monday 02:30.
    assert {
        item["hour"]: item["count"]
        for item in body["hourly_activity"]
        if item["count"]
    } == {13: 1, 19: 1, 2: 1}
    assert body["daily_activity"] == [
        {"date": "2026-08-01", "count": 2},
        {"date": "2026-08-03", "count": 1},
    ]
    assert {"weekday": 0, "hour": 2, "count": 1} in body["weekly_activity"]


def test_user_without_comments_has_an_empty_profile(client):
    with client({8: (None, None, [], True)}) as http:
        body = http.get("/api/v1/users/8").json()
        export = http.get("/api/v1/users/8/comments.txt")

    assert body["profile"]["display_username"] == "нет ника"
    assert body["profile"]["total_messages"] == 0
    assert body["profile"]["channels"] == []
    assert all(item["count"] == 0 for item in body["hourly_activity"])
    assert body["daily_activity"] == []
    assert all(item["count"] == 0 for item in body["weekly_activity"])
    assert (export.status_code, export.text) == (200, "")


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/users/404",
        "/api/v1/users/404/comments.txt",
        "/api/v1/users/404/position-comparisons",
    ],
)
def test_unknown_user_is_404(client, path):
    with client(VASYA) as http:
        assert http.get(path).status_code == 404
        assert http.post("/api/v1/users/404/political-coords").status_code == 404


def test_a_file_name_is_not_a_user_id(client):
    with client(VASYA) as http:
        assert http.get("/api/v1/users/vasya_7.db").status_code == 422


def test_comments_export_is_a_text_attachment_in_date_order(client):
    users = {
        7: (
            "vasya",
            None,
            [
                ("chan_b", 2, "second", _utc(2026, 8, 2)),
                ("chan_a", 1, "first", _utc(2026, 8, 1)),
            ],
            True,
        ),
        8: (None, "Хрюкало Офф", [("chan_a", 5, "other", _utc(2026, 8, 1))], True),
    }

    with client(users) as http:
        resp = http.get("/api/v1/users/7/comments.txt")
        unnamed = http.get("/api/v1/users/8/comments.txt")

    assert resp.status_code == 200
    assert resp.text == "first\n\nsecond"
    assert resp.headers["content-type"].startswith("text/plain")
    assert "attachment" in resp.headers["content-disposition"]
    assert "vasya_7.txt" in resp.headers["content-disposition"]
    # ASCII fallback first, the real name percent-encoded after it.
    assert 'filename="8.txt"' in unnamed.headers["content-disposition"]
    assert "%D1%85" in unnamed.headers["content-disposition"]


def test_political_analysis_receives_the_users_comments(client, monkeypatch):
    received = []

    async def fake_analyze(comments):
        received.append(comments)
        return AggregatedCoords(
            total_messages=len(comments),
            signal_count=1,
            axes={"economic": AxisStats(left_count=1, right_count=0)},
        )

    monkeypatch.setattr("web.app.analyze_political_coords", fake_analyze)
    users = {
        **VASYA,
        9: ("other", None, [("chan_a", 9, "not mine", _utc(2026, 8, 1))], True),
    }

    with client(users) as http:
        resp = http.post("/api/v1/users/7/political-coords")

    assert resp.status_code == 200
    assert received == [["hello", "world", "again"]]
    body = resp.json()
    assert (body["total_messages"], body["signal_count"]) == (3, 1)
    assert body["axes"]["economic"] == {"left_count": 1, "right_count": 0}
    assert "Итого: 1 из 3" in body["bars"]
