import asyncio
import json
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from web_auth import logged_in

from parser.user_collector import ChannelProgress, UserCollectResult
from web.app import create_app
from web.jobs import JobRegistry

# Never connected to: these tests stay off the database.
_NO_DATABASE = "postgresql+asyncpg://nobody@127.0.0.1:1/none"


def _result(tg_id: int, new: int) -> UserCollectResult:
    return UserCollectResult(
        tg_id=tg_id,
        username="vasya",
        channels_scanned=1,
        channels_failed=0,
        fetched=new,
        new=new,
    )


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
        return _result(555, 3)

    registry = JobRegistry(collect_fn=fake_collect)
    app = create_app(database_url=_NO_DATABASE, channels=["chan_a"], job_registry=registry)

    with logged_in(app) as client:
        resp = client.post("/api/v1/collect", json={"username": "@vasya"})
        assert resp.status_code == 202
        job_id = resp.json()["job_id"]

        body = _wait_for_final_status(client, job_id)

    assert body["state"] == "done"
    assert body["tg_id"] == 555
    assert body["saved_total"] == 3


def test_start_collect_reports_error_status_on_failure(tmp_path: Path):
    async def fake_collect(data_dir, cfg, user_ref, deps):
        raise RuntimeError("Cannot resolve user '@ghost'")

    registry = JobRegistry(collect_fn=fake_collect)
    app = create_app(database_url=_NO_DATABASE, channels=["chan_a"], job_registry=registry)

    with logged_in(app) as client:
        resp = client.post("/api/v1/collect", json={"username": "@ghost"})
        job_id = resp.json()["job_id"]

        body = _wait_for_final_status(client, job_id)

    assert body["state"] == "error"
    assert body["error"] == "Cannot resolve user '@ghost'"


def test_start_collect_says_when_telegram_login_is_missing(tmp_path: Path):
    async def fake_collect(data_dir, cfg, user_ref, deps):
        # What Pyrogram's prompt for a phone number ends with when nobody can answer.
        raise EOFError("EOF when reading a line")

    registry = JobRegistry(collect_fn=fake_collect)
    app = create_app(database_url=_NO_DATABASE, channels=["chan_a"], job_registry=registry)

    with logged_in(app) as client:
        resp = client.post("/api/v1/collect", json={"username": "@vasya"})
        job_id = resp.json()["job_id"]

        body = _wait_for_final_status(client, job_id)

    assert body["state"] == "error"
    assert body["error"] == (
        "На сервере не выполнен вход в Telegram. Выполните его командой "
        "`python -m scripts.login` (см. README) и повторите."
    )


def test_start_collect_rejects_empty_username(tmp_path: Path):
    app = create_app(database_url=_NO_DATABASE, channels=["chan_a"])

    with logged_in(app) as client:
        resp = client.post("/api/v1/collect", json={"username": "   "})

    assert resp.status_code == 400


def test_start_collect_rejects_request_while_a_job_is_running(tmp_path: Path):
    app = create_app(
        database_url=_NO_DATABASE,
        channels=["chan_a"], job_registry=_AlwaysBusyRegistry()
    )

    with logged_in(app) as client:
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
        return _result(555, 0)

    registry = JobRegistry(collect_fn=blocking_collect)
    app = create_app(database_url=_NO_DATABASE, channels=["chan_a"], job_registry=registry)

    with logged_in(app) as client:
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
    app = create_app(database_url=_NO_DATABASE, channels=["chan_a"])

    with logged_in(app) as client:
        resp = client.post("/api/v1/collect/does-not-exist/cancel")

    assert resp.status_code == 404


def test_collect_status_returns_404_for_unknown_job(tmp_path: Path):
    app = create_app(database_url=_NO_DATABASE, channels=["chan_a"])

    with logged_in(app) as client:
        resp = client.get("/api/v1/collect/does-not-exist/status")

    assert resp.status_code == 404



def test_save_channels_list_persists_and_updates_app_state(tmp_path: Path):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps(["old_channel"]))
    app = create_app(
        database_url=_NO_DATABASE,
        channels=["old_channel"], channels_path=channels_path
    )

    with logged_in(app) as client:
        resp = client.post("/api/v1/channels", json={"channels_text": "chan_a\n@chan_b\n"})

    assert resp.status_code == 200
    assert resp.json() == {"channels": ["chan_a", "chan_b"]}
    assert json.loads(channels_path.read_text()) == ["chan_a", "chan_b"]
    assert app.state.channels == ["chan_a", "chan_b"]





def test_save_channels_list_rejects_invalid_line(tmp_path: Path):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps(["old_channel"]))
    app = create_app(
        database_url=_NO_DATABASE,
        channels=["old_channel"], channels_path=channels_path
    )

    with logged_in(app) as client:
        resp = client.post("/api/v1/channels", json={"channels_text": "chan a"})

    assert resp.status_code == 400
    # Nothing is written and app.state.channels is untouched on validation failure.
    assert json.loads(channels_path.read_text()) == ["old_channel"]
    assert app.state.channels == ["old_channel"]


def test_api_v1_returns_channels(tmp_path: Path):
    app = create_app(database_url=_NO_DATABASE, channels=["chan_a", "chan_b"])

    with logged_in(app) as client:
        resp = client.get("/api/v1/channels")

    assert resp.status_code == 200
    assert resp.json() == {"channels": ["chan_a", "chan_b"]}


def test_api_v1_saves_channels(tmp_path: Path):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps(["old_channel"]))
    app = create_app(
        database_url=_NO_DATABASE,
        channels=["old_channel"], channels_path=channels_path
    )

    with logged_in(app) as client:
        resp = client.post("/api/v1/channels", json={"channels_text": "chan_a\n"})

    assert resp.status_code == 200
    assert resp.json() == {"channels": ["chan_a"]}
    assert app.state.channels == ["chan_a"]


def test_api_v1_collect_runs_job_and_reports_status(tmp_path: Path):
    async def fake_collect(data_dir, _config, _user_ref, _deps):
        return _result(7, 3)

    registry = JobRegistry(collect_fn=fake_collect)
    app = create_app(database_url=_NO_DATABASE, channels=["chan_a"], job_registry=registry)

    with logged_in(app) as client:
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
    assert status["tg_id"] == 7
    assert status["saved_total"] == 3
    assert cancel.json() == {"cancelled": False}


def test_openapi_documents_only_api_v1_routes(tmp_path: Path):
    app = create_app(database_url=_NO_DATABASE, channels=["chan_a"])

    paths = set(app.openapi()["paths"])

    assert "/api/v1/profiles" in paths
    assert "/api/v1/users/{tg_id}" in paths
    assert "/api/v1/collect/{job_id}/status" in paths
    assert all(path.startswith("/api/v1/") for path in paths)


def test_openapi_operation_ids_are_route_names(tmp_path: Path):
    app = create_app(database_url=_NO_DATABASE, channels=["chan_a"])

    operation = app.openapi()["paths"]["/api/v1/profiles"]["get"]

    assert operation["operationId"] == "list_profiles"


def _create_dist(root: Path) -> Path:
    (root / "assets").mkdir(parents=True)
    (root / "index.html").write_text("<div id=root></div>")
    (root / "assets" / "app.js").write_text("console.log(1)")
    return root


@pytest.mark.parametrize("url", ["/", "/index.html", "/users/7", "/some/page"])
def test_frontend_pages_get_index_html(tmp_path: Path, url: str):
    dist = _create_dist(tmp_path / "dist")
    app = create_app(database_url=_NO_DATABASE, channels=[], frontend_dist=dist)

    with logged_in(app) as client:
        resp = client.get(url)

    assert resp.status_code == 200
    assert resp.text == "<div id=root></div>"
    assert resp.headers["cache-control"] == "no-cache"


def test_frontend_serves_build_files(tmp_path: Path):
    dist = _create_dist(tmp_path / "dist")
    app = create_app(database_url=_NO_DATABASE, channels=[], frontend_dist=dist)

    with logged_in(app) as client:
        resp = client.get("/assets/app.js")

    assert resp.status_code == 200
    assert resp.text == "console.log(1)"


def test_frontend_never_serves_files_outside_the_build(tmp_path: Path):
    dist = _create_dist(tmp_path / "dist")
    (tmp_path / "secret.txt").write_text("secret")
    app = create_app(database_url=_NO_DATABASE, channels=[], frontend_dist=dist)

    with logged_in(app) as client:
        resp = client.get("/%2e%2e/secret.txt")

    assert "secret" not in resp.text


def test_unknown_api_route_is_404_not_a_page(tmp_path: Path):
    dist = _create_dist(tmp_path / "dist")
    app = create_app(database_url=_NO_DATABASE, channels=[], frontend_dist=dist)

    with logged_in(app) as client:
        resp = client.get("/api/v1/nope")

    assert resp.status_code == 404
    assert resp.json() == {"detail": "Not Found"}


def test_missing_frontend_build_says_how_to_get_it(tmp_path: Path):
    app = create_app(database_url=_NO_DATABASE, channels=[], frontend_dist=tmp_path / "missing")

    with logged_in(app) as client:
        resp = client.get("/")

    assert resp.status_code == 503
    assert "npm run build" in resp.text


def test_old_paths_are_gone(tmp_path: Path):
    app = create_app(database_url=_NO_DATABASE, channels=[], frontend_dist=tmp_path / "missing")

    with logged_in(app) as client:
        resp = client.post("/collect", json={"username": "@vasya"})

    assert resp.status_code in (404, 405)


def test_committed_openapi_schema_is_current(tmp_path: Path):
    committed = Path(__file__).parents[1] / "frontend" / "openapi" / "openapi.json"
    app = create_app(database_url=_NO_DATABASE, channels=[])

    # Stale: run `npm run generate:api` in frontend/ and commit the result.
    assert json.loads(committed.read_text()) == app.openapi()
