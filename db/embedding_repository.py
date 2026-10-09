"""SQL of embedding models and vectors. Callers own the transaction."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import delete, func, null, select, true
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.embedding_models import (
    EMBEDDING_DIMENSIONS,
    ChunkEmbedding,
    EmbeddingModel,
    MessageEmbedding,
)
from db.models import Channel, Chunk, ChunkMessageLink, Message

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


@dataclass(frozen=True)
class RankedMessage:
    message_id: int
    channel: str
    tg_message_id: int
    date: datetime
    text: str
    # Cosine similarity of the message itself to the query.
    message_score: float
    # Of the best chunk holding the message; None when it is in no chunk yet.
    chunk_score: float | None
    score: float


async def find_model_id(
    session: AsyncSession,
    name: str,
    revision: str,
    pooling: str,
    normalized: bool,
    input_prefix: str,
) -> int | None:
    """The stored model with exactly this identity, if any vectors were made by it."""
    return await session.scalar(
        select(EmbeddingModel.id).where(
            EmbeddingModel.name == name,
            EmbeddingModel.revision == revision,
            EmbeddingModel.pooling == pooling,
            EmbeddingModel.normalized == normalized,
            EmbeddingModel.input_prefix == input_prefix,
        )
    )


async def count_user_message_embeddings(
    session: AsyncSession, model_id: int, user_id: int
) -> int:
    return await session.scalar(
        select(func.count())
        .select_from(MessageEmbedding)
        .join(Message, Message.id == MessageEmbedding.message_id)
        .where(MessageEmbedding.model_id == model_id, Message.user_id == user_id)
    )


async def rank_messages(
    session: AsyncSession,
    model_id: int,
    user_id: int,
    query_vector,
    limit: int,
    chunk_set_id: int | None = None,
    context_weight: float = 0.0,
) -> list[RankedMessage]:
    """The user's messages nearest to the query, optionally helped by their chunks.

    score = (1 - context_weight) * message_score + context_weight * chunk_score

    where chunk_score is the similarity of the best chunk of `chunk_set_id`
    that holds the message. A message in no chunk is scored by itself alone.
    Without a chunk set this is plain message search. Exact scan, one user.
    """
    message_score = 1 - MessageEmbedding.embedding.cosine_distance(query_vector)
    stmt = (
        select(
            Message.id,
            Channel.username,
            Message.tg_message_id,
            Message.date,
            Message.text,
            message_score.label("message_score"),
        )
        .join(MessageEmbedding, MessageEmbedding.message_id == Message.id)
        .join(Channel, Channel.id == Message.channel_id)
        .where(MessageEmbedding.model_id == model_id, Message.user_id == user_id)
    )
    if chunk_set_id is None or context_weight == 0:
        score = message_score
        stmt = stmt.add_columns(null())
    else:
        # The best chunk of each message, looked up by index for every message
        # (LATERAL). Joining an aggregate over all the user's chunks instead
        # reads better, but the planner underestimates it and falls into a
        # nested loop without an index: 11 s instead of 0.2 s on 24,000 messages.
        context = (
            select(
                func.max(
                    1 - ChunkEmbedding.embedding.cosine_distance(query_vector)
                ).label("score")
            )
            .select_from(ChunkMessageLink)
            .join(Chunk, Chunk.id == ChunkMessageLink.chunk_id)
            .join(ChunkEmbedding, ChunkEmbedding.chunk_id == Chunk.id)
            .where(
                ChunkMessageLink.message_id == Message.id,
                Chunk.chunk_set_id == chunk_set_id,
                ChunkEmbedding.model_id == model_id,
            )
            .lateral("context")
        )
        stmt = stmt.outerjoin(context, true())
        score = (1 - context_weight) * message_score + context_weight * func.coalesce(
            context.c.score, message_score
        )
        stmt = stmt.add_columns(context.c.score)
    stmt = stmt.add_columns(score.label("score")).order_by(
        score.desc(), Message.id
    ).limit(limit)
    return [
        RankedMessage(
            message_id=row[0],
            channel=row[1],
            tg_message_id=row[2],
            date=row[3],
            text=row[4],
            message_score=float(row[5]),
            chunk_score=None if row[6] is None else float(row[6]),
            score=float(row[7]),
        )
        for row in await session.execute(stmt)
    ]
