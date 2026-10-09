"""An in-memory stand-in for db.analysis_store.AnalysisStore.

The position analysis tests exercise the analysis, not PostgreSQL; the real
store has its own tests in tests/postgres.
"""
import json
import threading
from contextlib import contextmanager

from db.analysis_store import SourceProfile, digest
from parser.position_analysis import date_key


class MemoryStore:
    location = "memory"

    def __init__(self):
        self._cache: dict[tuple[str, str], str] = {}
        self._comments: dict[tuple[int, str, int], dict] = {}
        self._profiles: dict[int, SourceProfile] = {}
        # Stands for message_embeddings: {comment id: vector}.
        self.vectors: dict[int, list[float]] = {}
        self._lock = threading.Lock()

    # Values go through JSON, as they do in the real store.
    def get(self, namespace, key):
        value = self._cache.get((namespace, key))
        return None if value is None else json.loads(value)

    def put(self, namespace, key, value):
        self.put_many(namespace, {key: value})

    def put_many(self, namespace, values):
        for key, value in values.items():
            self._cache[(namespace, key)] = json.dumps(value, ensure_ascii=False)

    def get_all(self, namespace):
        return {
            key: json.loads(value)
            for (name, key), value in self._cache.items()
            if name == namespace
        }

    def get_many(self, namespace, keys):
        return {
            key: json.loads(self._cache[(namespace, key)])
            for key in dict.fromkeys(keys)
            if (namespace, key) in self._cache
        }

    def count(self, namespace):
        return sum(1 for name, _ in self._cache if name == namespace)

    @contextmanager
    def analysis_lock(self):
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("Анализ позиций уже выполняется")
        try:
            yield
        finally:
            self._lock.release()

    def add_source(self, user_id, rows):
        self._profiles[user_id] = SourceProfile(
            tg_id=user_id, display_username=f"@user{user_id}"
        )
        for message_id, text, date in rows:
            key = (user_id, "channel", message_id)
            self._comments[key] = {
                "id": self._comments.get(key, {}).get("id", len(self._comments) + 1),
                "tg_id": user_id,
                "channel": "channel",
                "message_id": message_id,
                "text": text,
                "date": date,
            }

    def load_sources(self):
        return dict(self._profiles), sorted(
            (dict(comment) for comment in self._comments.values()),
            key=lambda c: (
                c["tg_id"],
                date_key(c["date"]),
                c["channel"],
                c["message_id"],
            ),
        )

    def embed(self, encode) -> int:
        """Gives every comment without a vector `encode(text)`, as an embedding run does."""
        missing = [c for c in self._comments.values() if c["id"] not in self.vectors]
        for comment in missing:
            self.vectors[comment["id"]] = encode(comment["text"])
        return len(missing)

    def comment_vectors(self, spec):
        return dict(self.vectors)

    def count_comment_vectors(self, spec):
        return len(self.vectors)

    def manifest(self):
        return digest([sorted(self._profiles), sorted(self._comments)])


_stores: dict[object, MemoryStore] = {}


def store_for(root) -> MemoryStore:
    """One store per test directory, shared by every service the test creates."""
    return _stores.setdefault(root, MemoryStore())


def create_source(root, user_id, rows):
    """rows: (message id, text, date) of one author's comments."""
    store_for(root).add_source(user_id, rows)
