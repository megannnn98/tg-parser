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
        body = client.get(f"/api/v1/collect/{job_id}/status").json()
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
        resp = client.post("/api/v1/collect", json={"username": "@vasya"})
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
        resp = client.post("/api/v1/collect", json={"username": "@ghost"})
        job_id = resp.json()["job_id"]

        body = _wait_for_final_status(client, job_id)

    assert body["state"] == "error"
    assert body["error"] == "Cannot resolve user '@ghost'"


def test_start_collect_rejects_empty_username(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.post("/api/v1/collect", json={"username": "   "})

    assert resp.status_code == 400


def test_start_collect_rejects_request_while_a_job_is_running(tmp_path: Path):
    app = create_app(
        data_dir=tmp_path, channels=["chan_a"], job_registry=_AlwaysBusyRegistry()
    )

    with TestClient(app) as client:
        resp = client.post("/api/v1/collect", json={"username": "@vasya"})

    assert resp.status_code == 409


def _wait_for_channel_started(
    client: TestClient, job_id: str, timeout: float = 2.0
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/api/v1/collect/{job_id}/status").json()
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
        resp = client.post("/api/v1/collect", json={"username": "@vasya"})
        job_id = resp.json()["job_id"]

        _wait_for_channel_started(client, job_id)

        cancel_resp = client.post(f"/api/v1/collect/{job_id}/cancel")
        assert cancel_resp.status_code == 200
        assert cancel_resp.json() == {"cancelled": True}

        body = _wait_for_final_status(client, job_id)

    assert body["state"] == "error"
    assert body["error"] == "Сбор был прерван"


def test_cancel_collect_returns_404_for_unknown_job(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.post("/api/v1/collect/does-not-exist/cancel")

    assert resp.status_code == 404


def test_collect_status_returns_404_for_unknown_job(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.get("/api/v1/collect/does-not-exist/status")

    assert resp.status_code == 404



def test_save_channels_list_persists_and_updates_app_state(tmp_path: Path):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps(["old_channel"]))
    app = create_app(
        data_dir=tmp_path, channels=["old_channel"], channels_path=channels_path
    )

    with TestClient(app) as client:
        resp = client.post("/api/v1/channels", json={"channels_text": "chan_a\n@chan_b\n"})

    assert resp.status_code == 200
    assert resp.json() == {"channels": ["chan_a", "chan_b"]}
    assert json.loads(channels_path.read_text()) == ["chan_a", "chan_b"]
    assert app.state.channels == ["chan_a", "chan_b"]





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
        resp = client.get("/api/v1/users/vasya_7.db/comments.txt")

    assert resp.status_code == 200
    assert resp.text == "hello\n\nworld"
    assert resp.headers["content-type"].startswith("text/plain")
    content_disposition = resp.headers["content-disposition"]
    assert "attachment" in content_disposition
    assert "vasya_7.txt" in content_disposition


def test_export_user_comments_returns_404_for_unknown_db(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.get("/api/v1/users/ghost_1.db/comments.txt")

    assert resp.status_code == 404


def test_save_channels_list_rejects_invalid_line(tmp_path: Path):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps(["old_channel"]))
    app = create_app(
        data_dir=tmp_path, channels=["old_channel"], channels_path=channels_path
    )

    with TestClient(app) as client:
        resp = client.post("/api/v1/channels", json={"channels_text": "chan a"})

    assert resp.status_code == 400
    # Nothing is written and app.state.channels is untouched on validation failure.
    assert json.loads(channels_path.read_text()) == ["old_channel"]
    assert app.state.channels == ["old_channel"]


def test_api_v1_lists_profiles(tmp_path: Path):
    _create_user_db(
        tmp_path / "vasya_7.db",
        [
            (7, "vasya", "chan_a", 1, "hello", "2026-08-01"),
            (7, "vasya", "chan_b", 2, "world", "2026-08-02"),
        ],
    )
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.get("/api/v1/profiles")

    assert resp.status_code == 200
    [profile] = resp.json()
    assert profile["db_name"] == "vasya_7.db"
    assert profile["tg_id"] == 7
    assert profile["display_username"] == "@vasya"
    assert profile["total_messages"] == 2
    assert profile["channel_count"] == 2
    assert {c["name"] for c in profile["channels"]} == {"chan_a", "chan_b"}


def test_api_v1_returns_channels(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=["chan_a", "chan_b"])

    with TestClient(app) as client:
        resp = client.get("/api/v1/channels")

    assert resp.status_code == 200
    assert resp.json() == {"channels": ["chan_a", "chan_b"]}


def test_api_v1_saves_channels(tmp_path: Path):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps(["old_channel"]))
    app = create_app(
        data_dir=tmp_path, channels=["old_channel"], channels_path=channels_path
    )

    with TestClient(app) as client:
        resp = client.post("/api/v1/channels", json={"channels_text": "chan_a\n"})

    assert resp.status_code == 200
    assert resp.json() == {"channels": ["chan_a"]}
    assert app.state.channels == ["chan_a"]


def test_api_v1_user_detail_includes_profile_and_activity(tmp_path: Path):
    _create_user_db(
        tmp_path / "vasya_7.db",
        [
            (7, "vasya", "chan_a", 1, "hello", "2026-08-01 08:00:00"),
            (7, "vasya", "chan_a", 2, "world", "2026-08-01 14:00:00"),
            (7, "vasya", "chan_b", 3, "again", "2026-08-02 14:30:00"),
        ],
    )
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.get("/api/v1/users/vasya_7.db")

    assert resp.status_code == 200
    body = resp.json()
    assert body["profile"]["display_username"] == "@vasya"
    assert body["profile"]["total_messages"] == 3
    hourly = {item["hour"]: item["count"] for item in body["hourly_activity"]}
    assert hourly[8] == 1
    assert hourly[14] == 2
    assert body["daily_activity"] == [
        {"date": "2026-08-01", "count": 2},
        {"date": "2026-08-02", "count": 1},
    ]
    weekly = body["weekly_activity"]
    assert len(weekly) == 7 * 24
    # 2026-08-01 is a Saturday, 2026-08-02 a Sunday.
    assert [cell for cell in weekly if cell["count"]] == [
        {"weekday": 5, "hour": 8, "count": 1},
        {"weekday": 5, "hour": 14, "count": 1},
        {"weekday": 6, "hour": 14, "count": 1},
    ]


@pytest.mark.parametrize("db_name", ["rotor8_5448422967.db", "5448422967.db"])
def test_api_v1_empty_user_profile_is_available(tmp_path: Path, db_name: str):
    _create_user_db(tmp_path / db_name, [])
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.get(f"/api/v1/users/{db_name}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["profile"]["tg_id"] == 5448422967
        assert body["profile"]["total_messages"] == 0
        assert body["profile"]["channel_count"] == 0
        assert body["profile"]["channels"] == []
        assert all(item["count"] == 0 for item in body["hourly_activity"])
        assert body["daily_activity"] == []
        assert all(item["count"] == 0 for item in body["weekly_activity"])
        assert [p["db_name"] for p in client.get("/api/v1/profiles").json()] == [db_name]
        export = client.get(f"/api/v1/users/{db_name}/comments.txt")
        assert export.status_code == 200
        assert export.text == ""


def test_api_v1_user_detail_returns_404_for_unknown_db(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.get("/api/v1/users/ghost_1.db")

    assert resp.status_code == 404


def test_api_v1_exports_user_comments(tmp_path: Path):
    _create_user_db(
        tmp_path / "vasya_7.db", [(7, "vasya", "chan_a", 1, "hello", "2026-08-01")]
    )
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.get("/api/v1/users/vasya_7.db/comments.txt")

    assert resp.status_code == 200
    assert resp.text == "hello"
    assert "vasya_7.txt" in resp.headers["content-disposition"]


def test_api_v1_collect_runs_job_and_reports_status(tmp_path: Path):
    async def fake_collect(data_dir, _config, _user_ref, _deps):
        return data_dir / "vasya_7.db", 3

    registry = JobRegistry(collect_fn=fake_collect)
    app = create_app(data_dir=tmp_path, channels=["chan_a"], job_registry=registry)

    with TestClient(app) as client:
        resp = client.post("/api/v1/collect", json={"username": "@vasya"})
        assert resp.status_code == 202
        job_id = resp.json()["job_id"]

        deadline = time.monotonic() + 2.0
        while True:
            status = client.get(f"/api/v1/collect/{job_id}/status").json()
            if status["state"] != "running" or time.monotonic() > deadline:
                break
            time.sleep(0.01)

        cancel = client.post(f"/api/v1/collect/{job_id}/cancel")

    assert status["state"] == "done"
    assert status["db_name"] == "vasya_7.db"
    assert status["saved_total"] == 3
    assert cancel.json() == {"cancelled": False}


def test_openapi_documents_only_api_v1_routes(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    paths = set(app.openapi()["paths"])

    assert "/api/v1/profiles" in paths
    assert "/api/v1/users/{db_name}" in paths
    assert "/api/v1/collect/{job_id}/status" in paths
    assert all(path.startswith("/api/v1/") for path in paths)


def test_openapi_operation_ids_are_route_names(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    operation = app.openapi()["paths"]["/api/v1/profiles"]["get"]

    assert operation["operationId"] == "list_profiles"


def _create_dist(root: Path) -> Path:
    (root / "assets").mkdir(parents=True)
    (root / "index.html").write_text("<div id=root></div>")
    (root / "assets" / "app.js").write_text("console.log(1)")
    return root


@pytest.mark.parametrize("url", ["/", "/index.html", "/users/vasya_7.db", "/some/page"])
def test_frontend_pages_get_index_html(tmp_path: Path, url: str):
    dist = _create_dist(tmp_path / "dist")
    app = create_app(data_dir=tmp_path, channels=[], frontend_dist=dist)

    with TestClient(app) as client:
        resp = client.get(url)

    assert resp.status_code == 200
    assert resp.text == "<div id=root></div>"
    assert resp.headers["cache-control"] == "no-cache"


def test_frontend_serves_build_files(tmp_path: Path):
    dist = _create_dist(tmp_path / "dist")
    app = create_app(data_dir=tmp_path, channels=[], frontend_dist=dist)

    with TestClient(app) as client:
        resp = client.get("/assets/app.js")

    assert resp.status_code == 200
    assert resp.text == "console.log(1)"


def test_frontend_never_serves_files_outside_the_build(tmp_path: Path):
    dist = _create_dist(tmp_path / "dist")
    (tmp_path / "secret.txt").write_text("secret")
    app = create_app(data_dir=tmp_path, channels=[], frontend_dist=dist)

    with TestClient(app) as client:
        resp = client.get("/%2e%2e/secret.txt")

    assert "secret" not in resp.text


def test_unknown_api_route_is_404_not_a_page(tmp_path: Path):
    dist = _create_dist(tmp_path / "dist")
    app = create_app(data_dir=tmp_path, channels=[], frontend_dist=dist)

    with TestClient(app) as client:
        resp = client.get("/api/v1/nope")

    assert resp.status_code == 404
    assert resp.json() == {"detail": "Not Found"}


def test_missing_frontend_build_says_how_to_get_it(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=[], frontend_dist=tmp_path / "missing")

    with TestClient(app) as client:
        resp = client.get("/")

    assert resp.status_code == 503
    assert "npm run build" in resp.text


def test_old_paths_are_gone(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=[], frontend_dist=tmp_path / "missing")

    with TestClient(app) as client:
        resp = client.post("/collect", json={"username": "@vasya"})

    assert resp.status_code in (404, 405)


def test_committed_openapi_schema_is_current(tmp_path: Path):
    committed = Path(__file__).parents[1] / "frontend" / "openapi" / "openapi.json"
    app = create_app(data_dir=tmp_path, channels=[])

    # Stale: run `npm run generate:api` in frontend/ and commit the result.
    assert json.loads(committed.read_text()) == app.openapi()
