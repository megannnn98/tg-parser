from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from config import CHANNELS, CHANNELS_PATH
from parser.channels_store import InvalidChannelError, parse_channels_text, save_channels
from parser.user_profile import (
    UserProfileError,
    fetch_daily_activity,
    fetch_hourly_activity,
    fetch_user_comments,
    list_user_profiles,
    load_user_profile,
    render_user_comments_text,
)
from parser.utils import parse_user_ref
from web.jobs import JobAlreadyRunningError, JobRegistry


templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


class CollectRequest(BaseModel):
    username: str


class ChannelsRequest(BaseModel):
    channels_text: str


def create_app(
    data_dir: Path | None = None,
    channels: list[str] | None = None,
    channels_path: Path | None = None,
    job_registry: JobRegistry | None = None,
) -> FastAPI:
    app = FastAPI(title="Telegram user profiles")
    app.state.data_dir = data_dir or Path(os.getenv("DATA_DIR", "data"))
    app.state.channels = channels if channels is not None else CHANNELS
    app.state.channels_path = channels_path or CHANNELS_PATH
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
                "channels": app.state.channels,
            },
        )

    @app.post("/channels")
    def save_channels_list(payload: ChannelsRequest):
        try:
            channels = parse_channels_text(payload.channels_text)
        except InvalidChannelError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        save_channels(app.state.channels_path, channels)
        app.state.channels = channels
        return {"channels": channels}

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

    @app.post("/collect/{job_id}/cancel")
    def cancel_collect(job_id: str):
        job = app.state.job_registry.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Unknown job")

        cancelled = app.state.job_registry.cancel(job_id)
        return {"cancelled": cancelled}

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

        hourly = fetch_hourly_activity(db_path, profile.tg_id)
        daily = fetch_daily_activity(db_path, profile.tg_id)

        return templates.TemplateResponse(
            request,
            "profile.html",
            {
                "profile": profile,
                "hourly_activity": hourly,
                "daily_activity": daily,
            },
        )

    @app.get("/users/{db_name}/comments.txt")
    def export_user_comments(db_name: str):
        db_path = _resolve_user_db(app.state.data_dir, db_name)
        try:
            profile = load_user_profile(db_path)
        except UserProfileError as exc:
            raise HTTPException(
                status_code=404,
                detail="User database not found",
            ) from exc

        comments = fetch_user_comments(db_path, profile.tg_id)
        text = render_user_comments_text(comments)

        filename = f"{Path(db_name).stem}.txt"
        return PlainTextResponse(
            text,
            media_type="text/plain; charset=utf-8",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="{profile.tg_id}.txt"; '
                    f"filename*=UTF-8''{quote(filename)}"
                )
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
