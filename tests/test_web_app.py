import time
from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from web.app import _resolve_user_db, create_app
from web.jobs import JobRegistry


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


def test_collect_status_returns_404_for_unknown_job(tmp_path: Path):
    app = create_app(data_dir=tmp_path, channels=["chan_a"])

    with TestClient(app) as client:
        resp = client.get("/collect/does-not-exist/status")

    assert resp.status_code == 404
