import sqlite3
from pathlib import Path

from parser.user_profile import (
    fetch_daily_activity,
    fetch_hourly_activity,
    load_user_profile,
)
from web.app import templates


class _Request:
    def url_for(self, name: str, **kwargs: str) -> str:
        if name == "index":
            return "/"
        if name == "export_user_comments":
            return f"/users/{kwargs['db_name']}/comments.txt"
        raise AssertionError(f"unexpected route: {name}")


def test_daily_chart_scrolls_to_latest_days(tmp_path: Path):
    db_path = tmp_path / "vasya_7.db"
    day_count = 31
    bar_width = 18
    gap = 8
    chart_width = max(680, 62 + day_count * (bar_width + gap) + 40)
    _create_user_db(
        db_path,
        [
            (7, "vasya", "chan_a", day, "hello", f"2026-08-{day:02d} 08:00:00")
            for day in range(1, day_count + 1)
        ],
    )
    profile = load_user_profile(db_path)

    html = templates.env.get_template("profile.html").render(
        request=_Request(),
        profile=profile,
        hourly_activity=fetch_hourly_activity(db_path, profile.tg_id),
        daily_activity=fetch_daily_activity(db_path, profile.tg_id),
    )

    assert 'class="daily-chart-scroll" id="daily-scroll"' in html
    assert (
        f'style="width: max(100%, {chart_width}px); display: block; height: auto;"'
        in html
    )
    assert ".daily-bar:focus-visible" in html
    assert "outline: 2px solid var(--text);" in html
    assert 'class="daily-bar"' in html
    assert 'data-date="2026-08-01"' in html
    assert 'data-count="1"' in html
    assert 'aria-label="2026-08-01: 1 сообщений"' in html
    assert 'class="daily-bar daily-bar--selected"' in html
    assert 'aria-pressed="true"' in html
    assert "2026-08-31: 1 сообщений" in html
    assert 'id="daily-chart-selection"' in html
    assert "function selectDailyBar(bar)" in html
    assert "let keyboardSelectedBar = null;" in html
    assert "event.detail === 0 && keyboardSelectedBar === bar" in html
    assert 'bar.dataset.date + ": " + bar.dataset.count + " сообщений"' in html
    assert "dailyScroll.scrollLeft = dailyScroll.scrollWidth" in html


def _create_user_db(
    db_path: Path,
    rows: list[tuple[int, str | None, str, int, str, str]],
) -> None:
    with sqlite3.connect(db_path) as db:
        db.execute(
            """
            CREATE TABLE user_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER NOT NULL,
                username TEXT,
                channel TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                date TEXT NOT NULL,
                UNIQUE(channel, message_id)
            )
            """
        )
        db.executemany(
            """
            INSERT INTO user_messages
            (tg_id, username, channel, message_id, text, date)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
