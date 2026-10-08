"""Checkpoints and cached inference of the position analysis, in PostgreSQL.

Everything here is derived data: dropping the table only costs a recomputation.
The analysis calls its store from worker threads, so this module is synchronous
(psycopg) while the rest of the application uses the async engine.
"""
from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import dataclass

from sqlalchemy import create_engine, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine, make_url

from db.engine import database_url
from db.models import AnalysisCache, Channel, Message, User

# Any constant shared by all processes of the application.
_ANALYSIS_LOCK_ID = 7_265_114_001
_BATCH_KEYS = 500


def digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True).encode()
    ).hexdigest()


@dataclass(frozen=True)
class SourceProfile:
    tg_id: int
    display_username: str


class AnalysisStore:
    location = "PostgreSQL, таблица analysis_cache"

    def __init__(self, url: str | None = None):
        self._url = url
        self._engine: Engine | None = None

    @property
    def engine(self) -> Engine:
        # Created on first use: the web application starts without a database.
        if self._engine is None:
            url = make_url(self._url or database_url()).set(
                drivername="postgresql+psycopg"
            )
            self._engine = create_engine(url, pool_pre_ping=True)
        return self._engine

    def close(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    # --- cache ---------------------------------------------------------------

    def get(self, namespace: str, key: str):
        return self.get_many(namespace, [key]).get(key)

    def put(self, namespace: str, key: str, value: object) -> None:
        self.put_many(namespace, {key: value})

    def put_many(self, namespace: str, values: dict) -> None:
        if not values:
            return
        stmt = insert(AnalysisCache).values(
            [
                {"namespace": namespace, "key": key, "value": value}
                for key, value in values.items()
            ]
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[AnalysisCache.namespace, AnalysisCache.key],
            set_={"value": stmt.excluded.value},
        )
        with self.engine.begin() as connection:
            connection.execute(stmt)

    def get_all(self, namespace: str) -> dict:
        stmt = select(AnalysisCache.key, AnalysisCache.value).where(
            AnalysisCache.namespace == namespace
        )
        with self.engine.connect() as connection:
            return dict(connection.execute(stmt).tuples().all())

    def get_many(self, namespace: str, keys) -> dict:
        keys = list(dict.fromkeys(keys))
        result: dict = {}
        if not keys:
            return result
        with self.engine.connect() as connection:
            for start in range(0, len(keys), _BATCH_KEYS):
                stmt = select(AnalysisCache.key, AnalysisCache.value).where(
                    AnalysisCache.namespace == namespace,
                    AnalysisCache.key.in_(keys[start : start + _BATCH_KEYS]),
                )
                result.update(connection.execute(stmt).tuples().all())
        return result

    def count(self, namespace: str) -> int:
        stmt = select(func.count()).where(AnalysisCache.namespace == namespace)
        with self.engine.connect() as connection:
            return connection.scalar(stmt)

    @contextmanager
    def analysis_lock(self):
        """Held by the one running analysis, across processes and workers."""
        connection = self.engine.connect()
        try:
            acquired = connection.scalar(
                select(func.pg_try_advisory_lock(_ANALYSIS_LOCK_ID))
            )
            # The lock belongs to the session, not to this transaction.
            connection.commit()
            if not acquired:
                raise RuntimeError("Анализ позиций уже выполняется")
            try:
                yield
            finally:
                connection.execute(select(func.pg_advisory_unlock(_ANALYSIS_LOCK_ID)))
                connection.commit()
        finally:
            connection.close()

    # --- sources -------------------------------------------------------------

    def load_sources(self) -> tuple[dict[int, SourceProfile], list[dict]]:
        """Collected profiles and their comments, each author's oldest first."""
        users = select(
            User.id, User.tg_id, User.username, User.first_name, User.last_name
        ).where(User.profile_collected_at.is_not(None))
        comments = (
            select(
                User.tg_id,
                Channel.username,
                Message.tg_message_id,
                Message.text,
                Message.date,
            )
            .join(User, User.id == Message.user_id)
            .join(Channel, Channel.id == Message.channel_id)
            .where(User.profile_collected_at.is_not(None))
            .order_by(User.tg_id, Message.date, Channel.username, Message.tg_message_id)
        )
        with self.engine.connect() as connection:
            profiles = {
                tg_id: SourceProfile(
                    tg_id=tg_id,
                    display_username=(
                        f"@{username}"
                        if username
                        else " ".join(p for p in (first_name, last_name) if p)
                        or "нет ника"
                    ),
                )
                for _, tg_id, username, first_name, last_name in connection.execute(
                    users
                )
            }
            rows = [
                {
                    "tg_id": tg_id,
                    "channel": channel,
                    "message_id": message_id,
                    "text": text,
                    "date": date.isoformat(),
                }
                for tg_id, channel, message_id, text, date in connection.execute(
                    comments
                )
            ]
        return profiles, rows

    def manifest(self) -> str:
        """Changes when a profile or a comment of a profile is added."""
        stmt = (
            select(
                func.count(User.id.distinct()),
                func.count(Message.id),
                func.max(Message.id),
            )
            .select_from(User)
            .outerjoin(Message, Message.user_id == User.id)
            .where(User.profile_collected_at.is_not(None))
        )
        with self.engine.connect() as connection:
            return digest(list(connection.execute(stmt).one()))
