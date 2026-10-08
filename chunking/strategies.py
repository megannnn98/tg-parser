"""Chunking strategies: how one user's comments are grouped for one embedding.

Pure functions over message ids, dates and token counts. A chunk is an ordered
list of whole messages, so it can be rebuilt from the strategy, its parameters
and the messages alone.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

# Messages of a chunk are joined with a newline, about one token.
SEPARATOR_TOKENS = 1

# Bump a strategy's version when the same parameters start to give other chunks.
STRATEGY_VERSIONS = {
    "message": 1,
    "fixed_messages": 1,
    "token_budget": 1,
    "time_window": 1,
    "hybrid": 1,
    "hybrid_short": 1,
}

# name -> (required parameters, optional parameters)
_PARAMETERS: dict[str, tuple[set[str], set[str]]] = {
    "message": (set(), set()),
    "fixed_messages": ({"max_messages", "same_channel"}, set()),
    "token_budget": ({"max_tokens", "overlap", "same_channel"}, set()),
    "time_window": ({"max_gap_seconds", "same_channel"}, {"max_tokens"}),
    "hybrid": (
        {"max_gap_seconds", "max_tokens", "max_messages", "same_channel"},
        set(),
    ),
    "hybrid_short": (
        {"min_standalone_tokens", "max_tokens", "max_gap_seconds", "same_channel"},
        set(),
    ),
}


@dataclass(frozen=True)
class ChunkMessage:
    id: int
    channel_id: int
    tg_message_id: int
    date: datetime
    tokens: int


def chunk_token_count(chunk: list[ChunkMessage]) -> int:
    return sum(m.tokens for m in chunk) + SEPARATOR_TOKENS * (len(chunk) - 1)


def _validate(name: str, params: dict) -> None:
    if name not in _PARAMETERS:
        raise ValueError(f"Unknown chunking strategy {name!r}")
    required, optional = _PARAMETERS[name]
    missing = required - params.keys()
    unknown = params.keys() - required - optional
    if missing or unknown:
        raise ValueError(
            f"Strategy {name!r}: missing {sorted(missing)}, unknown {sorted(unknown)}"
        )
    for key in ("max_messages", "max_tokens", "max_gap_seconds"):
        if key in params and params[key] <= 0:
            raise ValueError(f"Strategy {name!r}: {key} must be positive")
    if not 0 <= params.get("overlap", 0) < 1:
        raise ValueError(f"Strategy {name!r}: overlap must be in [0, 1)")


def build_chunks(
    name: str, params: dict, messages: list[ChunkMessage]
) -> list[list[ChunkMessage]]:
    """Groups one user's messages; each chunk lists its messages oldest first."""
    _validate(name, params)
    ordered = sorted(messages, key=lambda m: (m.date, m.channel_id, m.tg_message_id))
    if name == "message":
        return [[message] for message in ordered]

    if params["same_channel"]:
        # Each channel is a timeline of its own: a chunk never crosses channels.
        by_channel: dict[int, list[ChunkMessage]] = defaultdict(list)
        for message in ordered:
            by_channel[message.channel_id].append(message)
        timelines = [by_channel[channel] for channel in sorted(by_channel)]
    else:
        timelines = [ordered]

    chunks: list[list[ChunkMessage]] = []
    for timeline in timelines:
        chunks.extend(_group(timeline, params))
    return chunks


def _group(timeline: list[ChunkMessage], params: dict) -> list[list[ChunkMessage]]:
    max_messages = params.get("max_messages")
    max_tokens = params.get("max_tokens")
    max_gap = params.get("max_gap_seconds")
    min_standalone = params.get("min_standalone_tokens")
    overlap_tokens = params.get("overlap", 0) * (max_tokens or 0)

    def fits(chunk: list[ChunkMessage], message: ChunkMessage) -> bool:
        if max_messages is not None and len(chunk) + 1 > max_messages:
            return False
        if max_tokens is not None and (
            chunk_token_count(chunk) + SEPARATOR_TOKENS + message.tokens > max_tokens
        ):
            return False
        return True

    chunks: list[list[ChunkMessage]] = []
    current: list[ChunkMessage] = []

    def close() -> None:
        nonlocal current
        if current:
            chunks.append(current)
        current = []

    for message in timeline:
        if min_standalone is not None and message.tokens >= min_standalone:
            close()
            chunks.append([message])
            continue

        if current and max_gap is not None:
            gap = (message.date - current[-1].date).total_seconds()
            if gap >= max_gap:
                close()

        if current and not fits(current, message):
            previous = current
            close()
            # The tail of the closed chunk that fits both the overlap and,
            # together with the new message, the budget. It is never the whole
            # chunk: that one was closed because the message did not fit.
            tail: list[ChunkMessage] = []
            for candidate in reversed(previous if overlap_tokens else []):
                grown = [candidate, *tail]
                if chunk_token_count(grown) > overlap_tokens or not fits(
                    grown, message
                ):
                    break
                tail = grown
            current = tail

        current.append(message)

    close()
    return chunks
