"""Computes the embeddings that are missing. Neither Telegram nor the chunk
builder is involved: this reads stored texts and writes vectors."""
from __future__ import annotations

import asyncio

from db import embedding_repository as repo


async def _model_id(sessions, spec) -> int:
    async with sessions.begin() as session:
        return await repo.get_or_create_model(
            session,
            name=spec.name,
            revision=spec.revision,
            dimensions=spec.dimensions,
            pooling=spec.pooling,
            normalized=spec.normalized,
            max_tokens=spec.max_tokens,
            input_prefix=spec.passage_prefix,
        )


async def _embed_missing(sessions, encoder, fetch, store, batch_size: int) -> int:
    """Embeds batch after batch until nothing is missing; one transaction each."""
    stored = 0
    while True:
        async with sessions() as session:
            batch = await fetch(session, batch_size)
        if not batch:
            return stored
        # The model blocks; keep the event loop free meanwhile.
        vectors = await asyncio.to_thread(
            encoder.encode_passages, [text for _, text in batch]
        )
        async with sessions.begin() as session:
            stored += await store(
                session,
                [(owner_id, vector) for (owner_id, _), vector in zip(batch, vectors)],
            )


async def embed_messages(
    sessions,
    encoder,
    batch_size: int = 256,
    force: bool = False,
    user_id: int | None = None,
) -> int:
    """Embeds the messages that have no vector of this model yet.

    `force` first drops the model's message vectors, so all are computed again.
    `user_id` (users.id) limits both to one user's messages. Returns the number
    of vectors stored.
    """
    model_id = await _model_id(sessions, encoder.spec)
    if force:
        async with sessions.begin() as session:
            await repo.delete_message_embeddings(session, model_id, user_id)
    return await _embed_missing(
        sessions,
        encoder,
        lambda session, limit: repo.messages_without_embedding(
            session, model_id, limit, user_id
        ),
        lambda session, rows: repo.insert_message_embeddings(session, model_id, rows),
        batch_size,
    )


async def embed_chunks(
    sessions,
    encoder,
    chunk_set_id: int,
    batch_size: int = 256,
    force: bool = False,
    user_id: int | None = None,
) -> int:
    """Embeds the chunks of one set that have no vector of this model yet."""
    model_id = await _model_id(sessions, encoder.spec)
    if force:
        async with sessions.begin() as session:
            await repo.delete_chunk_embeddings(
                session, model_id, chunk_set_id, user_id
            )
    return await _embed_missing(
        sessions,
        encoder,
        lambda session, limit: repo.chunks_without_embedding(
            session, model_id, chunk_set_id, limit, user_id
        ),
        lambda session, rows: repo.insert_chunk_embeddings(session, model_id, rows),
        batch_size,
    )
