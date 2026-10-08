"""Durable inference checkpoints; source comment databases remain read-only."""
from __future__ import annotations

from contextlib import closing, contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


class PositionStore:
    def __init__(self, data_dir: Path):
        self.path = data_dir / "position-analysis.sqlite3"

    @contextmanager
    def connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    namespace TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
                    PRIMARY KEY(namespace, key)
                )
            """)
            yield db

    def get(self, namespace: str, key: str):
        if not self.path.exists():
            return None
        with self.connect() as db:
            row = db.execute(
                "SELECT value FROM cache WHERE namespace=? AND key=?", (namespace, key)
            ).fetchone()
        return json.loads(row[0]) if row else None

    def put(self, namespace: str, key: str, value: object):
        self.put_many(namespace, {key: value})

    def put_many(self, namespace: str, values: dict):
        with self.connect() as db:
            db.executemany(
                "INSERT OR REPLACE INTO cache VALUES (?, ?, ?)",
                [(namespace, key, json.dumps(value, ensure_ascii=False)) for key, value in values.items()],
            )

    def get_all(self, namespace: str) -> dict:
        if not self.path.exists():
            return {}
        with self.connect() as db:
            rows = db.execute("SELECT key, value FROM cache WHERE namespace=?", (namespace,)).fetchall()
        return {key: json.loads(value) for key, value in rows}

    def get_many(self, namespace: str, keys) -> dict:
        keys = list(dict.fromkeys(keys))
        if not keys or not self.path.exists():
            return {}
        result = {}
        with self.connect() as db:
            for start in range(0, len(keys), 500):
                batch = keys[start:start + 500]
                placeholders = ",".join("?" for _ in batch)
                for key, value in db.execute(
                    f"SELECT key, value FROM cache WHERE namespace=? AND key IN ({placeholders})",
                    [namespace, *batch],
                ):
                    result[key] = json.loads(value)
        return result

    def count(self, namespace: str) -> int:
        if not self.path.exists():
            return 0
        with self.connect() as db:
            return db.execute("SELECT count(*) FROM cache WHERE namespace=?", (namespace,)).fetchone()[0]

    @contextmanager
    def analysis_lock(self):
        # Cross-process lock: multiple uvicorn workers must not pay for the same run.
        import fcntl

        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.with_suffix(".lock").open("a") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("Анализ позиций уже выполняется") from exc
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)
