import asyncio
import json
import sqlite3
import time
from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from parser.user_collector import ChannelProgress
from web.app import _resolve_user_db, create_app
from web.jobs import JobRegistry


def _create_user_db(db_path: Path, rows: list[tuple[int, str | None, str, int, str, str]]):
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


def test_resolve_user_db_allows_direct_db_file(tmp_path: Path):
    db_path = tmp_path / "vasya_7.db"
    db_path.touch()

    assert _resolve_user_db(tmp_path, "vasya_7.db") == db_path.resolve()


@pytest.mark.parametrize(
    "db_name",
    [
        "../vasya_7.db",
        "nested/vasya_7.db",
        "/tmp/vasya_7.db",
        "vasya_7.sqlite",
    ],
)
def test_resolve_user_db_rejects_unsafe_names(tmp_path: Path, db_name: str):
    with pytest.raises(HTTPException) as exc_info:
        _resolve_user_db(tmp_path, db_name)

    assert exc_info.value.status_code == 404


class _AlwaysBusyRegistry:
    def start(self, *_args, **_kwargs):
        from web.jobs import JobAlreadyRunningError

        raise JobAlreadyRunningError("busy")

    def get(self, _job_id):
        return None


def _wait_for_final_status(client: TestClient, job_id: str, timeout: float = 2.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/collect/{job_id}/status").json()
        if body["state"] != "running":
            return body
        time.sleep(0.01)

    raise AssertionError(f"job {job_id} did not finish within {timeout}s")


def test_start_collect_runs_job_and_status_reports_done(tmp_path: Path):
    async def fake_collect(data_dir, cfg, user_ref, deps):
        return tmp_path / "vasya_555.db", 3

    registry = JobRegistry(collect_fn=fake_collect)
    app = create_app(data_dir=tmp_path, channels=["chan_a"], job_registry=registry)

    with TestClient(app) as client:
        resp = client.post("/collect", json={"username": "@vasya"})
        assert resp.status_code == 202
        job_id = resp.json()["job_id"]

        body = _wait_for_final_status(client, job_id)

    assert body["state"] == "done"
    assert body["db_name"] == "vasya_555.db"
    assert body["saved_total"] == 3


def test_start_collect_reports_error_status_on_failure(tmp_path: Path):
    async def fake_collect(data_dir, cfg, user_ref, deps):
        raise RuntimeError("Cannot resolve user '@ghost'")

    registry = JobRegistry(collect_fn=fake_collect)
    app = create_app(data_dir=tmp_path, channels=["chan_a"], job_registry=registry)

    with TestClient(app) as client:
        resp = client.post("/collect", json={"username": "@ghost"})
        job_id = resp.json()["job_id"]

        body = _wait_for_final_status(client, job_id)

    assert body["state"] == "error"
    assert body["error"] == "Cannot resolve user '@ghost'"


def test_start_collect_rejects_empty_username(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.post("/collect", json={"username": "   "})

    assert resp.status_code == 400


def test_start_collect_rejects_request_while_a_job_is_running(tmp_path: Path):
    app = create_app(
        data_dir=tmp_path, channels=["chan_a"], job_registry=_AlwaysBusyRegistry()
    )

    with TestClient(app) as client:
        resp = client.post("/collect", json={"username": "@vasya"})

    assert resp.status_code == 409


def _wait_for_channel_started(
    client: TestClient, job_id: str, timeout: float = 2.0
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/collect/{job_id}/status").json()
        if any(ch["status"] == "started" for ch in body["channels"]):
            return body
        time.sleep(0.01)

    raise AssertionError(f"job {job_id} channel never started within {timeout}s")


def test_cancel_collect_stops_running_job(tmp_path: Path):
    async def blocking_collect(data_dir, cfg, user_ref, deps):
        deps.on_channel_progress(ChannelProgress(channel="chan_a", status="started"))
        await asyncio.sleep(10)
        return tmp_path / "vasya_555.db", 0

    registry = JobRegistry(collect_fn=blocking_collect)
    app = create_app(data_dir=tmp_path, channels=["chan_a"], job_registry=registry)

    with TestClient(app) as client:
        resp = client.post("/collect", json={"username": "@vasya"})
        job_id = resp.json()["job_id"]

        _wait_for_channel_started(client, job_id)

        cancel_resp = client.post(f"/collect/{job_id}/cancel")
        assert cancel_resp.status_code == 200
        assert cancel_resp.json() == {"cancelled": True}

        body = _wait_for_final_status(client, job_id)

    assert body["state"] == "error"
    assert body["error"] == "Сбор был прерван"


def test_cancel_collect_returns_404_for_unknown_job(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.post("/collect/does-not-exist/cancel")

    assert resp.status_code == 404


def test_collect_status_returns_404_for_unknown_job(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.get("/collect/does-not-exist/status")

    assert resp.status_code == 404


def test_index_renders_current_channels(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=["chan_a", "chan_b"])

    with TestClient(app) as client:
        resp = client.get("/")

    assert resp.status_code == 200
    assert "chan_a" in resp.text
    assert "chan_b" in resp.text


def test_save_channels_list_persists_and_updates_app_state(tmp_path: Path):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps(["old_channel"]))
    app = create_app(
        data_dir=tmp_path, channels=["old_channel"], channels_path=channels_path
    )

    with TestClient(app) as client:
        resp = client.post("/channels", json={"channels_text": "chan_a\n@chan_b\n"})

    assert resp.status_code == 200
    assert resp.json() == {"channels": ["chan_a", "chan_b"]}
    assert json.loads(channels_path.read_text()) == ["chan_a", "chan_b"]
    assert app.state.channels == ["chan_a", "chan_b"]


def test_show_user_renders_refresh_and_export_controls(tmp_path: Path):
    db_path = tmp_path / "vasya_7.db"
    _create_user_db(db_path, [(7, "vasya", "chan_a", 1, "hello", "2026-08-01")])
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.get("/users/vasya_7.db")

    assert resp.status_code == 200
    assert 'id="refresh-button"' in resp.text
    assert "/users/vasya_7.db/comments.txt" in resp.text


def test_show_user_includes_activity_charts(tmp_path: Path):
    db_path = tmp_path / "vasya_7.db"
    _create_user_db(
        db_path,
        [
            (7, "vasya", "chan_a", 1, "hello", "2026-08-01 08:00:00"),
            (7, "vasya", "chan_a", 2, "world", "2026-08-01 14:00:00"),
            (7, "vasya", "chan_b", 3, "again", "2026-08-02 14:30:00"),
        ],
    )
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.get("/users/vasya_7.db")

    assert resp.status_code == 200
    assert "Активность" in resp.text
    assert "По часам" in resp.text
    assert "По дням" in resp.text
    assert 'aria-label="График активности по часам"' in resp.text
    assert 'aria-label="График активности по дням"' in resp.text
    assert '1 сообщений в 8:00' in resp.text
    assert '2 сообщений в 14:00' in resp.text
    assert '2 сообщений за 2026-08-01' in resp.text
    assert '1 сообщений за 2026-08-02' in resp.text


def test_show_user_handles_empty_activity(tmp_path: Path):
    db_path = tmp_path / "vasya_7.db"
    _create_user_db(db_path, [(7, "vasya", "chan_a", 1, "hello", "2026-08-01")])
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.get("/users/vasya_7.db")

    assert resp.status_code == 200
    assert "Активность" in resp.text


def test_export_user_comments_returns_text_with_attachment_header(tmp_path: Path):
    db_path = tmp_path / "vasya_7.db"
    _create_user_db(
        db_path,
        [
            (7, "vasya", "chan_a", 1, "hello", "2026-08-01"),
            (7, "vasya", "chan_b", 2, "world", "2026-08-02"),
        ],
    )
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.get("/users/vasya_7.db/comments.txt")

    assert resp.status_code == 200
    assert resp.text == "hello\n\nworld"
    assert resp.headers["content-type"].startswith("text/plain")
    content_disposition = resp.headers["content-disposition"]
    assert "attachment" in content_disposition
    assert "vasya_7.txt" in content_disposition


def test_export_user_comments_returns_404_for_unknown_db(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.get("/users/ghost_1.db/comments.txt")

    assert resp.status_code == 404


def test_save_channels_list_rejects_invalid_line(tmp_path: Path):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps(["old_channel"]))
    app = create_app(
        data_dir=tmp_path, channels=["old_channel"], channels_path=channels_path
    )

    with TestClient(app) as client:
        resp = client.post("/channels", json={"channels_text": "chan a"})

    assert resp.status_code == 400
    # Nothing is written and app.state.channels is untouched on validation failure.
    assert json.loads(channels_path.read_text()) == ["old_channel"]
    assert app.state.channels == ["old_channel"]
