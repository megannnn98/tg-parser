"""The React frontend (`frontend/`), served by the API itself from its build."""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse, Response

DEFAULT_DIST = Path(__file__).resolve().parents[1] / "frontend" / "dist"

_NOT_BUILT = (
    "Фронтенд не собран: {dist} не найден.\n"
    "Соберите его (cd frontend && npm ci && npm run build) или распакуйте туда "
    "артефакт frontend-dist из GitHub Actions; путь можно задать в FRONTEND_DIST.\n"
)


def frontend_dist() -> Path:
    return Path(os.getenv("FRONTEND_DIST", str(DEFAULT_DIST)))


def mount_frontend(app: FastAPI, dist: Path) -> None:
    """Every GET no API route answers: a file of the build, or `index.html` for any
    other address (the router picks the page in the browser). Register it last."""
    root = dist.resolve()

    @app.get("/{path:path}", include_in_schema=False)
    def frontend(path: str) -> Response:
        if path == "api" or path.startswith("api/"):
            raise HTTPException(status_code=404, detail="Not Found")
        index = root / "index.html"
        if not index.is_file():
            return PlainTextResponse(_NOT_BUILT.format(dist=root), status_code=503)
        file = (root / path).resolve()
        if path and file != index and file.is_file() and file.is_relative_to(root):
            return FileResponse(file)
        # Revalidated on every load, by any address: a cached index.html would
        # name the hashed assets of an old build, gone after an update.
        return FileResponse(index, headers={"Cache-Control": "no-cache"})
