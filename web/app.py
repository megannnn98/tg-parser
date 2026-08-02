from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from config import CHANNELS
from parser.user_profile import UserProfileError, list_user_profiles, load_user_profile
from parser.utils import parse_user_ref
from web.jobs import JobAlreadyRunningError, JobRegistry


templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


class CollectRequest(BaseModel):
    username: str


def create_app(
    data_dir: Path | None = None,
    channels: list[str] | None = None,
    job_registry: JobRegistry | None = None,
) -> FastAPI:
    app = FastAPI(title="Telegram user profiles")
    app.state.data_dir = data_dir or Path(os.getenv("DATA_DIR", "data"))
    app.state.channels = channels if channels is not None else CHANNELS
    app.state.job_registry = job_registry if job_registry is not None else JobRegistry()

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        profiles = list_user_profiles(app.state.data_dir)
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "profiles": profiles,
                "data_dir": app.state.data_dir,
            },
        )

    @app.post("/collect", status_code=202)
    async def start_collect(payload: CollectRequest):
        try:
            user_ref = parse_user_ref(payload.username)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        try:
            job = app.state.job_registry.start(
                app.state.data_dir, app.state.channels, user_ref
            )
        except JobAlreadyRunningError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        return {"job_id": job.job_id}

    @app.get("/collect/{job_id}/status")
    def collect_status(job_id: str):
        job = app.state.job_registry.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Unknown job")

        return job.snapshot()

    @app.get("/users/{db_name}", response_class=HTMLResponse)
    def show_user(request: Request, db_name: str):
        db_path = _resolve_user_db(app.state.data_dir, db_name)
        try:
            profile = load_user_profile(db_path)
        except UserProfileError as exc:
            raise HTTPException(
                status_code=404,
                detail="User database not found",
            ) from exc

        return templates.TemplateResponse(
            request,
            "profile.html",
            {
                "profile": profile,
            },
        )

    return app


def _resolve_user_db(data_dir: Path, db_name: str) -> Path:
    base = data_dir.resolve()
    candidate = (base / db_name).resolve()
    if candidate.parent != base or candidate.suffix != ".db":
        raise HTTPException(status_code=404, detail="Unknown user database")
    return candidate


app = create_app()
