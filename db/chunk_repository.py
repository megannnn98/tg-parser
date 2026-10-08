"""SQL of chunk sets and chunks. Callers own the transaction."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Chunk, ChunkMessageLink, ChunkSet, Message

_BATCH_ROWS = 2000


@dataclass(frozen=True)
class SourceMessage:
    id: int
    channel_id: int
    tg_message_id: int
    date: datetime
    text: str


def parameters_hash(parameters: dict) -> str:
    canonical = json.dumps(parameters, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def get_or_create_chunk_set(
    session: AsyncSession,
    strategy: str,
    strategy_version: int,
    parameters: dict,
    tokenizer: str,
) -> int:
    values = {
        "strategy": strategy,
        "strategy_version": strategy_version,
        "parameters": parameters,
        "parameters_hash": parameters_hash(parameters),
        "tokenizer": tokenizer,
    }
    stmt = insert(ChunkSet).values(values)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_chunk_sets_identity",
        # A no-op update, so that RETURNING also yields the existing row.
        set_={"strategy": stmt.excluded.strategy},
    ).returning(ChunkSet.id)
    return (await session.execute(stmt)).scalar_one()


async def find_chunk_sets(
    session: AsyncSession, strategy: str | None = None
) -> list[ChunkSet]:
    stmt = select(ChunkSet).order_by(ChunkSet.id)
    if strategy is not None:
        stmt = stmt.where(ChunkSet.strategy == strategy)
    return list(await session.scalars(stmt))


async def users_with_stale_chunks(session: AsyncSession, chunk_set_id: int) -> list[int]:
    """Users whose messages are not all covered by the set's chunks.

    Any chunking puts every message of a user into at least one chunk, so a
    user is up to date exactly when the two counts agree.
    """
    have = (
        select(Message.user_id, func.count().label("messages"))
        .group_by(Message.user_id)
        .subquery()
    )
    covered = (
        select(
            Chunk.user_id,
            func.count(ChunkMessageLink.message_id.distinct()).label("messages"),
        )
        .join(ChunkMessageLink, ChunkMessageLink.chunk_id == Chunk.id)
        .where(Chunk.chunk_set_id == chunk_set_id)
        .group_by(Chunk.user_id)
        .subquery()
    )
    stmt = (
        select(have.c.user_id)
        .outerjoin(covered, covered.c.user_id == have.c.user_id)
        .where(func.coalesce(covered.c.messages, 0) != have.c.messages)
        .order_by(have.c.user_id)
    )
    return list(await session.scalars(stmt))


async def users_with_messages(session: AsyncSession) -> list[int]:
    stmt = select(Message.user_id).distinct().order_by(Message.user_id)
    return list(await session.scalars(stmt))


async def messages_of_user(session: AsyncSession, user_id: int) -> list[SourceMessage]:
    stmt = select(
        Message.id,
        Message.channel_id,
        Message.tg_message_id,
        Message.date,
        Message.text,
    ).where(Message.user_id == user_id)
    return [SourceMessage(*row) for row in await session.execute(stmt)]


async def replace_user_chunks(
    session: AsyncSession, chunk_set_id: int, user_id: int, chunks: list[dict]
) -> None:
    """Replaces the user's chunks in the set.

    Each chunk: channel_id, date_from, date_to, text, token_count and
    message_ids in order. Embeddings of the replaced chunks go with them.
    """
    await session.execute(
        delete(Chunk).where(
            Chunk.chunk_set_id == chunk_set_id, Chunk.user_id == user_id
        )
    )
    for start in range(0, len(chunks), _BATCH_ROWS):
        batch = chunks[start : start + _BATCH_ROWS]
        # Executed with a parameter list, not VALUES: sort_by_parameter_order
        # then guarantees that the ids come back in the order of `batch`.
        result = await session.execute(
            insert(Chunk).returning(Chunk.id, sort_by_parameter_order=True),
            [
                {
                    "chunk_set_id": chunk_set_id,
                    "user_id": user_id,
                    "channel_id": chunk["channel_id"],
                    "date_from": chunk["date_from"],
                    "date_to": chunk["date_to"],
                    "text": chunk["text"],
                    "token_count": chunk["token_count"],
                    "message_count": len(chunk["message_ids"]),
                }
                for chunk in batch
            ],
        )
        chunk_ids = list(result.scalars())
        links = [
            {"chunk_id": chunk_id, "message_id": message_id, "position": position}
            for chunk_id, chunk in zip(chunk_ids, batch)
            for position, message_id in enumerate(chunk["message_ids"])
        ]
        for link_start in range(0, len(links), _BATCH_ROWS * 4):
            await session.execute(
                insert(ChunkMessageLink).values(
                    links[link_start : link_start + _BATCH_ROWS * 4]
                )
            )


async def chunks_of_user(
    session: AsyncSession, chunk_set_id: int, user_id: int
) -> list[tuple[Chunk, list[int]]]:
    """The user's chunks oldest first, each with its message ids in order."""
    chunks = list(
        await session.scalars(
            select(Chunk)
            .where(Chunk.chunk_set_id == chunk_set_id, Chunk.user_id == user_id)
            .order_by(Chunk.date_from, Chunk.id)
        )
    )
    links: dict[int, list[int]] = {chunk.id: [] for chunk in chunks}
    stmt = (
        select(ChunkMessageLink.chunk_id, ChunkMessageLink.message_id)
        .where(ChunkMessageLink.chunk_id.in_(links))
        .order_by(ChunkMessageLink.chunk_id, ChunkMessageLink.position)
    )
    for chunk_id, message_id in await session.execute(stmt):
        links[chunk_id].append(message_id)
    return [(chunk, links[chunk.id]) for chunk in chunks]
