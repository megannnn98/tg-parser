from __future__ import annotations

import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import PlainTextResponse

from config import APP_TIMEZONE, CHANNELS, CHANNELS_PATH
from db.analysis_store import AnalysisStore
from db.engine import Database
from parser.channels_store import (
    InvalidChannelError,
    parse_channels_text,
    save_channels,
)
from parser.political_coords import (
    PoliticalCoordsError,
    analyze_political_coords,
)
from parser.user_profile import render_user_comments_text
from parser.utils import parse_user_ref, user_export_filename
from parser.position_analysis import PositionAnalysis
from parser.local_text_analysis import LocalTextAnalysis
from parser.position_comparison import AnalysisProgress, PositionResults
from embeddings.e5 import PRODUCTION_MODEL, E5Encoder
from services.profiles import ProfileNotFound, ProfileService
from services.search import SemanticSearch
from web.position_jobs import PositionJobs
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
    SearchHit,
    SearchResponse,
    UserDetail,
)

# Operation ids are the function names: they name the generated frontend client.
api = APIRouter(generate_unique_id_function=lambda route: route.name)


def _profiles(request: Request) -> ProfileService:
    state = request.app.state
    return ProfileService(state.database.sessions, state.timezone)


@api.get("/profiles", response_model=list[Profile])
async def list_profiles(request: Request):
    return [Profile.of(p) for p in await _profiles(request).list_profiles()]


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
        job = state.job_registry.start(
            state.database.sessions, state.channels, user_ref
        )
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


@api.get("/users/{tg_id}", response_model=UserDetail)
async def get_user(request: Request, tg_id: int):
    try:
        detail = await _profiles(request).detail(tg_id)
    except ProfileNotFound as exc:
        raise _user_not_found() from exc
    return UserDetail.of(detail.profile, detail.hourly, detail.daily, detail.weekly)


@api.get(
    "/users/{tg_id}/comments.txt",
    response_class=PlainTextResponse,
    responses={200: {"content": {"text/plain": {}}}},
)
async def export_user_comments(request: Request, tg_id: int):
    try:
        profile, comments = await _profiles(request).comments(tg_id)
    except ProfileNotFound as exc:
        raise _user_not_found() from exc

    filename = user_export_filename(tg_id, profile.username, profile.display_name)
    return PlainTextResponse(
        render_user_comments_text(comments),
        media_type="text/plain; charset=utf-8",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{tg_id}.txt"; '
                f"filename*=UTF-8''{quote(filename)}"
            )
        },
    )


@api.post("/users/{tg_id}/political-coords", response_model=PoliticalCoords)
async def analyze_political(request: Request, tg_id: int):
    try:
        _profile, comments = await _profiles(request).comments(tg_id)
    except ProfileNotFound as exc:
        raise _user_not_found() from exc

    try:
        result = await analyze_political_coords([c.text for c in comments])
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


@api.get("/users/{tg_id}/position-comparisons", response_model=PositionResults)
async def get_position_comparisons(request: Request, tg_id: int):
    try:
        # Polled while an analysis runs: one lookup, not the whole profile.
        await _profiles(request).require(tg_id)
    except ProfileNotFound as exc:
        raise _user_not_found() from exc
    return await run_in_threadpool(
        request.app.state.position_jobs.service.results, tg_id
    )


@api.get("/users/{tg_id}/search", response_model=SearchResponse)
async def search_user_comments(
    request: Request,
    tg_id: int,
    q: str = Query(min_length=1, max_length=500),
    limit: int = Query(20, ge=1, le=100),
    context: bool = True,
):
    """The user's comments closest in meaning to `q`.

    `context=false` ranks every comment by itself, without its chunk: the
    plain message search the default ranking is compared with.
    """
    query = q.strip()
    if not query:
        raise HTTPException(status_code=422, detail="The query is empty")
    try:
        state = request.app.state
        result = await SemanticSearch(
            state.database.sessions, state.search_encoder, state.search_lock
        ).search(tg_id, query, limit=limit, use_context=context)
    except ProfileNotFound as exc:
        raise _user_not_found() from exc
    return SearchResponse(
        query=query,
        results=[
            SearchHit(
                message_id=hit.message_id,
                tg_message_id=hit.tg_message_id,
                channel=hit.channel,
                date=hit.date,
                text=hit.text,
                message_score=hit.message_score,
                chunk_score=hit.chunk_score,
                score=hit.score,
            )
            for hit in result.hits
        ],
        indexed_messages=result.indexed_messages,
        total_messages=result.total_messages,
        used_context=result.used_context,
    )


@api.post("/position-analysis", status_code=202, response_model=AnalysisProgress)
async def start_position_analysis(request: Request):
    try:
        return request.app.state.position_jobs.start()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


class _LockedEncoder:
    """The search encoder behind the lock that the search takes."""

    def __init__(self, encoder, lock):
        self.spec = encoder.spec
        self._encoder = encoder
        self._lock = lock

    def encode_passages(self, texts):
        with self._lock:
            return self._encoder.encode_passages(texts)


async def _embed_profile_comments(app) -> int:
    # Imported here: the embedding tables need numpy, the web application
    # must start without it.
    from embeddings.pipeline import embed_profile_messages

    return await embed_profile_messages(
        app.state.database.sessions,
        _LockedEncoder(app.state.search_encoder, app.state.search_lock),
    )


def create_app(
    database_url: str | None = None,
    channels: list[str] | None = None,
    channels_path: Path | None = None,
    job_registry: JobRegistry | None = None,
    frontend_dist: Path | None = None,
    position_service: PositionAnalysis | None = None,
    timezone: str = APP_TIMEZONE,
    search_encoder=None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            await app.state.position_jobs.close()
            await app.state.database.close()
            if app.state.analysis_store is not None:
                app.state.analysis_store.close()

    ZoneInfo(timezone)  # An unknown zone fails here, not on the first chart.

    app = FastAPI(title="Telegram user profiles", lifespan=lifespan)
    # No connection is made until a request needs the database.
    app.state.database = Database(database_url)
    app.state.timezone = timezone
    app.state.channels = channels if channels is not None else CHANNELS
    app.state.channels_path = channels_path or CHANNELS_PATH
    app.state.job_registry = job_registry if job_registry is not None else JobRegistry()
    app.state.analysis_store = None
    if position_service is None:
        app.state.analysis_store = AnalysisStore(database_url)
        position_service = LocalTextAnalysis(
            app.state.analysis_store,
            embed_missing=lambda: _embed_profile_comments(app),
        )
    app.state.position_jobs = PositionJobs(position_service)
    # The model the embedding tables were filled with; its weights are loaded
    # by the first search, not at startup.
    app.state.search_encoder = search_encoder or E5Encoder(
        PRODUCTION_MODEL, cache_dir=Path(os.getenv("DATA_DIR", "data")) / "e5-cache"
    )
    app.state.search_lock = threading.Lock()

    app.include_router(api, prefix="/api/v1")
    mount_frontend(app, frontend_dist or default_frontend_dist())

    return app


def _user_not_found() -> HTTPException:
    return HTTPException(status_code=404, detail="User not found")


app = create_app()
