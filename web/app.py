from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from parser.user_profile import UserProfileError, list_user_profiles, load_user_profile


templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def create_app(data_dir: Path | None = None) -> FastAPI:
    app = FastAPI(title="Telegram user profiles")
    app.state.data_dir = data_dir or Path(os.getenv("DATA_DIR", "data"))

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
