"""Builds chunks from stored messages. Telegram is not involved."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from chunking.strategies import (
    STRATEGY_VERSIONS,
    ChunkMessage,
    build_chunks,
    chunk_token_count,
)
from db import chunk_repository as repo

# Messages of a chunk are joined with this; strategies count it as one token.
SEPARATOR = "\n"


@dataclass(frozen=True)
class BuildResult:
    chunk_set_id: int
    users_rebuilt: int
    chunks_written: int


async def build_chunk_set(
    sessions,
    strategy: str,
    parameters: dict,
    tokenizer_name: str,
    count_tokens: Callable[[list[str]], list[int]],
    force: bool = False,
) -> BuildResult:
    """Brings the chunks of (strategy, parameters) up to date.

    Only users with messages not yet covered are rebuilt, unless `force`.
    Each user is one transaction.
    """
    # Validates the strategy and its parameters before anything is written.
    build_chunks(strategy, parameters, [])

    async with sessions.begin() as session:
        chunk_set_id = await repo.get_or_create_chunk_set(
            session, strategy, STRATEGY_VERSIONS[strategy], parameters, tokenizer_name
        )
    async with sessions() as session:
        if force:
            user_ids = await repo.users_with_messages(session)
        else:
            user_ids = await repo.users_with_stale_chunks(session, chunk_set_id)

    written = 0
    for user_id in user_ids:
        async with sessions.begin() as session:
            messages = await repo.messages_of_user(session, user_id)
            tokens = count_tokens([message.text for message in messages])
            texts = {message.id: message.text for message in messages}
            chunks = build_chunks(
                strategy,
                parameters,
                [
                    ChunkMessage(
                        id=message.id,
                        channel_id=message.channel_id,
                        tg_message_id=message.tg_message_id,
                        date=message.date,
                        tokens=count,
                    )
                    for message, count in zip(messages, tokens)
                ],
            )
            await repo.replace_user_chunks(
                session,
                chunk_set_id,
                user_id,
                [
                    {
                        "channel_id": (
                            chunk[0].channel_id
                            if len({m.channel_id for m in chunk}) == 1
                            else None
                        ),
                        "date_from": chunk[0].date,
                        "date_to": chunk[-1].date,
                        "text": SEPARATOR.join(texts[m.id] for m in chunk),
                        "token_count": chunk_token_count(chunk),
                        "message_ids": [m.id for m in chunk],
                    }
                    for chunk in chunks
                ],
            )
            written += len(chunks)

    return BuildResult(
        chunk_set_id=chunk_set_id, users_rebuilt=len(user_ids), chunks_written=written
    )
