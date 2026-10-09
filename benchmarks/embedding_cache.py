"""Experimental embeddings on disk, outside the database.

The benchmark compares many chunkings and several models before any vector
column exists; its embeddings live in data/benchmark and can be deleted.
"""
from __future__ import annotations

import hashlib
import time
from pathlib import Path

import numpy as np

from embeddings.e5 import E5Encoder


def _key(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()


class PassageCache:
    """Passage embeddings of one model revision, keyed by the text."""

    def __init__(self, encoder: E5Encoder, directory: Path):
        self.encoder = encoder
        spec = encoder.spec
        directory.mkdir(parents=True, exist_ok=True)
        stem = f"{spec.name.replace('/', '_')}@{spec.revision[:12]}"
        self._keys_path = directory / f"{stem}.keys.txt"
        self._vectors_path = directory / f"{stem}.vectors.npy"
        self._rows: dict[str, int] = {}
        self._vectors = np.empty((0, spec.dimensions), dtype=np.float32)
        if self._keys_path.exists():
            keys = self._keys_path.read_text().split()
            self._vectors = np.load(self._vectors_path)
            self._rows = {key: row for row, key in enumerate(keys)}
        # Seconds spent encoding and texts encoded, for throughput reports.
        self.encode_seconds = 0.0
        self.encoded = 0

    def passages(self, texts: list[str]) -> np.ndarray:
        keys = [_key(text) for text in texts]
        missing = {key: text for key, text in zip(keys, texts) if key not in self._rows}
        if missing:
            started = time.perf_counter()
            vectors = self.encoder.encode_passages(list(missing.values()))
            self.encode_seconds += time.perf_counter() - started
            self.encoded += len(missing)
            base = len(self._rows)
            self._rows.update((key, base + i) for i, key in enumerate(missing))
            self._vectors = np.concatenate([self._vectors, vectors])
            np.save(self._vectors_path, self._vectors)
            self._keys_path.write_text("\n".join(self._rows))
        return self._vectors[[self._rows[key] for key in keys]]
