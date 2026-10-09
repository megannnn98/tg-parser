"""Integration tests against a throwaway PostgreSQL (TEST_DATABASE_URL)."""
import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from db.engine import create_engine, session_factory
from db.models import Base

ROOT = Path(__file__).resolve().parents[2]


async def _reset_schema(url: str) -> None:
    engine = create_engine(url, poolclass=NullPool)
    async with engine.begin() as connection:
        await connection.execute(text("DROP SCHEMA public CASCADE"))
        await connection.execute(text("CREATE SCHEMA public"))
    await engine.dispose()


@pytest.fixture(scope="session")
def database_url() -> str:
    url = os.getenv("TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.skip("TEST_DATABASE_URL is not set: PostgreSQL tests did not run")
    # The schema is dropped below, so a real database must never get here.
    if not make_url(url).database.endswith("_test"):
        raise RuntimeError("TEST_DATABASE_URL must name a database ending in _test")

    asyncio.run(_reset_schema(url))
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env={**os.environ, "DATABASE_URL": url},
        check=True,
    )
    return url


@pytest.fixture
def run_db(database_url):
    """Runs `scenario(sessions)` on empty tables and returns its result."""
    tables = ", ".join(table.name for table in Base.metadata.sorted_tables)

    def run(scenario):
        async def wrapper():
            engine = create_engine(database_url, poolclass=NullPool)
            try:
                async with engine.begin() as connection:
                    await connection.execute(
                        text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE")
                    )
                return await scenario(session_factory(engine))
            finally:
                await engine.dispose()

        return asyncio.run(wrapper())

    return run
