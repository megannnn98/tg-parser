from __future__ import annotations

import os

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

load_dotenv()


def database_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set, expected "
            "postgresql+asyncpg://user:password@host:5432/database"
        )
    return url


def create_engine(url: str | None = None, **kwargs) -> AsyncEngine:
    return create_async_engine(url or database_url(), **kwargs)


def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


class Database:
    """Creates the engine on first use, so an application starts without it."""

    def __init__(self, url: str | None = None, **engine_kwargs):
        self._url = url
        self._engine_kwargs = engine_kwargs
        self._engine: AsyncEngine | None = None

    @property
    def sessions(self) -> async_sessionmaker[AsyncSession]:
        if self._engine is None:
            self._engine = create_engine(self._url, **self._engine_kwargs)
        return session_factory(self._engine)

    async def close(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
