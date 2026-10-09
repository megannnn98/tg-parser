"""Semantic search over one user's comments.

The result is always the original Telegram messages. A chunk is never
returned: it only lends its similarity to the messages it holds, so that a
short reply ("да", "согласен") is found through the conversation around it.

This is the "context" ranking measured in docs/embedding-chunking-research.md,
where it raised MRR from 0.81 to 0.89 over message-only search:

    score = (1 - CONTEXT_WEIGHT) * message_score + CONTEXT_WEIGHT * chunk_score

Both scores are cosine similarities to the query. chunk_score is that of the
best chunk holding the message; a message in no chunk is scored by itself.
"""
from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass

from chunking.strategies import STRATEGY_VERSIONS
from db import chunk_repository, repositories
from embeddings.e5 import E5Spec
from services.profiles import ProfileNotFound

# The benchmark used equal weights and did not tune them.
CONTEXT_WEIGHT = 0.5

# The chunk set whose embeddings give the context: what
# `python -m scripts.build_chunks` was run with for production.
CHUNK_STRATEGY = "token_budget"
CHUNK_PARAMETERS = {"max_tokens": 256, "overlap": 0, "same_channel": True}


@dataclass(frozen=True)
class SearchResult:
    hits: list  # db.embedding_repository.RankedMessage, best first
    # How many of the user's messages have a vector of the search model. Fewer
    # than `total_messages` means `python -m scripts.embed` has work to do.
    indexed_messages: int
    total_messages: int
    # False when no chunk set was found and messages were ranked alone.
    used_context: bool


def chunk_tokenizer(spec: E5Spec) -> str:
    """How scripts/build_chunks.py names the tokenizer of a chunk set."""
    return f"{spec.name}@{spec.revision}"


class SemanticSearch:
    def __init__(self, sessions, encoder, encode_lock: threading.Lock | None = None):
        self._sessions = sessions
        # Must be the model the stored vectors were made with: vectors of two
        # models are not comparable. The stored model row is looked up by the
        # encoder's own name, revision, pooling and prefix.
        self._encoder = encoder
        # One query at a time through the model. Shared by the services of one
        # application, which are created per request.
        self._encode_lock = encode_lock or threading.Lock()

    def _encode_query(self, query: str):
        with self._encode_lock:
            return self._encoder.encode_queries([query])[0]

    async def search(
        self, tg_id: int, query: str, limit: int = 20, use_context: bool = True
    ) -> SearchResult:
        """The user's messages closest in meaning to `query`, best first.

        Raises ProfileNotFound for an unknown user.
        """
        # Needs numpy through pgvector; the web application starts without it.
        from db import embedding_repository

        spec = self._encoder.spec
        async with self._sessions() as session:
            user = await repositories.get_user_by_tg_id(session, tg_id)
            if user is None:
                raise ProfileNotFound(tg_id)
            total = await repositories.count_user_messages(session, user.id)
            model_id = await embedding_repository.find_model_id(
                session,
                name=spec.name,
                revision=spec.revision,
                pooling=spec.pooling,
                normalized=spec.normalized,
                input_prefix=spec.passage_prefix,
            )
            indexed = 0
            if model_id is not None:
                indexed = await embedding_repository.count_user_message_embeddings(
                    session, model_id, user.id
                )
            if indexed == 0:
                # Nothing to search: do not load the model for it.
                return SearchResult([], 0, total, used_context=False)
            chunk_set_id = None
            if use_context:
                chunk_set_id = await chunk_repository.find_chunk_set_id(
                    session,
                    CHUNK_STRATEGY,
                    STRATEGY_VERSIONS[CHUNK_STRATEGY],
                    CHUNK_PARAMETERS,
                    chunk_tokenizer(spec),
                )

        vector = await asyncio.to_thread(self._encode_query, query)

        async with self._sessions() as session:
            hits = await embedding_repository.rank_messages(
                session,
                model_id,
                user.id,
                vector,
                limit,
                chunk_set_id=chunk_set_id,
                context_weight=CONTEXT_WEIGHT if chunk_set_id is not None else 0.0,
            )
        return SearchResult(hits, indexed, total, used_context=chunk_set_id is not None)
