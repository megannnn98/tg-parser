"""SQL of embedding models and vectors. Callers own the transaction."""
from __future__ import annotations

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.embedding_models import (
    EMBEDDING_DIMENSIONS,
    ChunkEmbedding,
    EmbeddingModel,
    MessageEmbedding,
)
from db.models import Chunk, Message

# A row carries a whole vector: keep statements small.
_BATCH_ROWS = 200


async def get_or_create_model(
    session: AsyncSession,
    name: str,
    revision: str,
    dimensions: int,
    pooling: str,
    normalized: bool,
    max_tokens: int,
    input_prefix: str,
) -> int:
    if dimensions != EMBEDDING_DIMENSIONS:
        raise ValueError(
            f"{name} gives {dimensions}-dimensional vectors, the embedding tables "
            f"hold {EMBEDDING_DIMENSIONS}: a model of another size needs a "
            "migration with tables of its own"
        )
    if not revision:
        raise ValueError("An embedding model must be pinned to a revision")
    stmt = insert(EmbeddingModel).values(
        name=name,
        revision=revision,
        dimensions=dimensions,
        pooling=pooling,
        normalized=normalized,
        max_tokens=max_tokens,
        input_prefix=input_prefix,
    )
    stmt = stmt.on_conflict_do_update(
        constraint="uq_embedding_models_identity",
        # A no-op update, so that RETURNING also yields the existing row.
        set_={"name": stmt.excluded.name},
    ).returning(EmbeddingModel.id)
    return (await session.execute(stmt)).scalar_one()


async def messages_without_embedding(
    session: AsyncSession,
    model_id: int,
    limit: int,
    user_id: int | None = None,
    after_id: int = 0,
) -> list[tuple[int, str]]:
    """Messages with no vector of the model, by id, starting after `after_id`.

    The caller passes the last id it saw, so a long run does not walk over the
    rows it has already embedded again for every batch.
    """
    embedded = select(MessageEmbedding.message_id).where(
        MessageEmbedding.model_id == model_id,
        MessageEmbedding.message_id == Message.id,
    )
    stmt = select(Message.id, Message.text).where(
        ~embedded.exists(), Message.id > after_id
    )
    if user_id is not None:
        stmt = stmt.where(Message.user_id == user_id)
    stmt = stmt.order_by(Message.id).limit(limit)
    return [tuple(row) for row in await session.execute(stmt)]


async def chunks_without_embedding(
    session: AsyncSession,
    model_id: int,
    chunk_set_id: int,
    limit: int,
    user_id: int | None = None,
    after_id: int = 0,
) -> list[tuple[int, str]]:
    embedded = select(ChunkEmbedding.chunk_id).where(
        ChunkEmbedding.model_id == model_id, ChunkEmbedding.chunk_id == Chunk.id
    )
    stmt = select(Chunk.id, Chunk.text).where(
        Chunk.chunk_set_id == chunk_set_id, ~embedded.exists(), Chunk.id > after_id
    )
    if user_id is not None:
        stmt = stmt.where(Chunk.user_id == user_id)
    stmt = stmt.order_by(Chunk.id).limit(limit)
    return [tuple(row) for row in await session.execute(stmt)]


async def _insert(session, table, key: str, model_id: int, rows) -> int:
    inserted = 0
    for start in range(0, len(rows), _BATCH_ROWS):
        stmt = (
            insert(table)
            .values(
                [
                    {key: owner_id, "model_id": model_id, "embedding": vector}
                    for owner_id, vector in rows[start : start + _BATCH_ROWS]
                ]
            )
            # Two runs may compute the same vector; the first one stored wins.
            .on_conflict_do_nothing()
            .returning(getattr(table, key))
        )
        inserted += len((await session.execute(stmt)).all())
    return inserted


async def insert_message_embeddings(
    session: AsyncSession, model_id: int, rows: list[tuple[int, object]]
) -> int:
    """rows: (messages.id, vector). Returns the number of vectors stored."""
    return await _insert(session, MessageEmbedding, "message_id", model_id, rows)


async def insert_chunk_embeddings(
    session: AsyncSession, model_id: int, rows: list[tuple[int, object]]
) -> int:
    return await _insert(session, ChunkEmbedding, "chunk_id", model_id, rows)


async def delete_message_embeddings(
    session: AsyncSession, model_id: int, user_id: int | None = None
) -> None:
    stmt = delete(MessageEmbedding).where(MessageEmbedding.model_id == model_id)
    if user_id is not None:
        stmt = stmt.where(
            MessageEmbedding.message_id.in_(
                select(Message.id).where(Message.user_id == user_id)
            )
        )
    await session.execute(stmt)


async def delete_chunk_embeddings(
    session: AsyncSession, model_id: int, chunk_set_id: int, user_id: int | None = None
) -> None:
    chunks = select(Chunk.id).where(Chunk.chunk_set_id == chunk_set_id)
    if user_id is not None:
        chunks = chunks.where(Chunk.user_id == user_id)
    await session.execute(
        delete(ChunkEmbedding).where(
            ChunkEmbedding.model_id == model_id, ChunkEmbedding.chunk_id.in_(chunks)
        )
    )


async def count_message_embeddings(session: AsyncSession, model_id: int) -> int:
    return await session.scalar(
        select(func.count())
        .select_from(MessageEmbedding)
        .where(MessageEmbedding.model_id == model_id)
    )


async def search_messages(
    session: AsyncSession, model_id: int, user_id: int, query_vector, limit: int
) -> list[tuple[int, float]]:
    """The user's messages nearest to the query: (messages.id, cosine similarity).

    An exact scan of the user's vectors, ordered by cosine distance.
    """
    distance = MessageEmbedding.embedding.cosine_distance(query_vector)
    stmt = (
        select(Message.id, 1 - distance)
        .join(MessageEmbedding, MessageEmbedding.message_id == Message.id)
        .where(MessageEmbedding.model_id == model_id, Message.user_id == user_id)
        .order_by(distance, Message.id)
        .limit(limit)
    )
    return [(message_id, float(score)) for message_id, score in await session.execute(stmt)]


async def search_chunks(
    session: AsyncSession,
    model_id: int,
    chunk_set_id: int,
    user_id: int,
    query_vector,
    limit: int,
) -> list[tuple[int, float]]:
    """The user's chunks of one set nearest to the query: (chunks.id, similarity)."""
    distance = ChunkEmbedding.embedding.cosine_distance(query_vector)
    stmt = (
        select(Chunk.id, 1 - distance)
        .join(ChunkEmbedding, ChunkEmbedding.chunk_id == Chunk.id)
        .where(
            ChunkEmbedding.model_id == model_id,
            Chunk.chunk_set_id == chunk_set_id,
            Chunk.user_id == user_id,
        )
        .order_by(distance, Chunk.id)
        .limit(limit)
    )
    return [(chunk_id, float(score)) for chunk_id, score in await session.execute(stmt)]
