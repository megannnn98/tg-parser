from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse

from config import CHANNELS, CHANNELS_PATH
from parser.channels_store import (
    InvalidChannelError,
    parse_channels_text,
    save_channels,
)
from parser.political_coords import (
    PoliticalCoordsError,
    analyze_political_coords,
)
from parser.user_profile import (
    UserProfile,
    UserProfileError,
    fetch_daily_activity,
    fetch_hourly_activity,
    fetch_user_comments,
    fetch_weekly_activity,
    list_user_profiles,
    load_user_profile,
    render_user_comments_text,
)
from parser.utils import parse_user_ref
from web.frontend import frontend_dist as default_frontend_dist
from web.frontend import mount_frontend
from web.jobs import JobAlreadyRunningError, JobRegistry
from web.schemas import (
    AxisCounts,
    ChannelsRequest,
    ChannelsResponse,
    CollectRequest,
    JobCancelled,
    JobStarted,
    JobStatus,
    PoliticalCoords,
    Profile,
    UserDetail,
)

# Operation ids are the function names: they name the generated frontend client.
api = APIRouter(generate_unique_id_function=lambda route: route.name)


@api.get("/profiles", response_model=list[Profile])
def list_profiles(request: Request):
    return [Profile.of(p) for p in list_user_profiles(request.app.state.data_dir)]


@api.get("/channels", response_model=ChannelsResponse)
def get_channels(request: Request):
    return ChannelsResponse(channels=request.app.state.channels)


@api.post("/channels", response_model=ChannelsResponse)
def save_channels_list(request: Request, payload: ChannelsRequest):
    try:
        channels = parse_channels_text(payload.channels_text)
    except InvalidChannelError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    save_channels(request.app.state.channels_path, channels)
    request.app.state.channels = channels
    return ChannelsResponse(channels=channels)


@api.post("/collect", status_code=202, response_model=JobStarted)
async def start_collect(request: Request, payload: CollectRequest):
    try:
        user_ref = parse_user_ref(payload.username)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    state = request.app.state
    try:
        job = state.job_registry.start(state.data_dir, state.channels, user_ref)
    except JobAlreadyRunningError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return JobStarted(job_id=job.job_id)


@api.get("/collect/{job_id}/status", response_model=JobStatus)
def collect_status(request: Request, job_id: str):
    job = request.app.state.job_registry.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Unknown job")

    return job.snapshot()


@api.post("/collect/{job_id}/cancel", response_model=JobCancelled)
def cancel_collect(request: Request, job_id: str):
    registry = request.app.state.job_registry
    if registry.get(job_id) is None:
        raise HTTPException(status_code=404, detail="Unknown job")

    return JobCancelled(cancelled=registry.cancel(job_id))


@api.get("/users/{db_name}", response_model=UserDetail)
def get_user(request: Request, db_name: str):
    db_path = _resolve_user_db(request.app.state.data_dir, db_name)
    profile = _load_profile(db_path)
    return UserDetail.of(
        profile,
        fetch_hourly_activity(db_path, profile.tg_id),
        fetch_daily_activity(db_path, profile.tg_id),
        fetch_weekly_activity(db_path, profile.tg_id),
    )


@api.get(
    "/users/{db_name}/comments.txt",
    response_class=PlainTextResponse,
    responses={200: {"content": {"text/plain": {}}}},
)
def export_user_comments(request: Request, db_name: str):
    db_path = _resolve_user_db(request.app.state.data_dir, db_name)
    profile = _load_profile(db_path)

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


@api.post("/users/{db_name}/political-coords", response_model=PoliticalCoords)
async def analyze_political(request: Request, db_name: str):
    db_path = _resolve_user_db(request.app.state.data_dir, db_name)
    profile = _load_profile(db_path)

    try:
        result = await analyze_political_coords(db_path, profile.tg_id)
    except PoliticalCoordsError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return PoliticalCoords(
        total_messages=result.total_messages,
        signal_count=result.signal_count,
        bars=result.render_bars(),
        axes={
            key: AxisCounts(left_count=stats.left_count, right_count=stats.right_count)
            for key, stats in result.axes.items()
        },
    )


def create_app(
    data_dir: Path | None = None,
    channels: list[str] | None = None,
    channels_path: Path | None = None,
    job_registry: JobRegistry | None = None,
    frontend_dist: Path | None = None,
) -> FastAPI:
    app = FastAPI(title="Telegram user profiles")
    app.state.data_dir = data_dir or Path(os.getenv("DATA_DIR", "data"))
    app.state.channels = channels if channels is not None else CHANNELS
    app.state.channels_path = channels_path or CHANNELS_PATH
    app.state.job_registry = job_registry if job_registry is not None else JobRegistry()

    app.include_router(api, prefix="/api/v1")
    mount_frontend(app, frontend_dist or default_frontend_dist())

    return app


def _load_profile(db_path: Path) -> UserProfile:
    try:
        return load_user_profile(db_path)
    except UserProfileError as exc:
        raise HTTPException(status_code=404, detail="User database not found") from exc


def _resolve_user_db(data_dir: Path, db_name: str) -> Path:
    base = data_dir.resolve()
    candidate = (base / db_name).resolve()
    if candidate.parent != base or candidate.suffix != ".db":
        raise HTTPException(status_code=404, detail="Unknown user database")
    return candidate


app = create_app()
