"""Computes the embeddings that are missing for a model.

    python -m scripts.embed --messages
    python -m scripts.embed --chunk-set 3

A repeated run computes only what has no vector of this model revision yet;
--force drops the model's vectors of that kind and computes them again.
Telegram is not contacted, and the comment texts are never changed.
"""
from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from db import repositories
from db.engine import create_engine, session_factory
from embeddings.e5 import SPECS, E5Encoder
from embeddings.pipeline import embed_chunks, embed_messages


def _model_cache() -> Path:
    # Read here, not from config.py: that module needs the Telegram
    # credentials, and this script never talks to Telegram.
    return Path(os.getenv("DATA_DIR", "data")) / "e5-cache"


async def _main(args: argparse.Namespace) -> None:
    encoder = E5Encoder(SPECS[args.model], cache_dir=_model_cache())
    engine = create_engine()
    sessions = session_factory(engine)
    try:
        user_id = None
        if args.user is not None:
            async with sessions() as session:
                user = await repositories.get_user_by_tg_id(session, args.user)
            if user is None:
                raise SystemExit(f"No user with tg_id {args.user}")
            user_id = user.id
        if args.messages:
            stored = await embed_messages(
                sessions, encoder, force=args.force, user_id=user_id
            )
            print(f"message embeddings stored: {stored}")
        if args.chunk_set is not None:
            stored = await embed_chunks(
                sessions, encoder, args.chunk_set, force=args.force, user_id=user_id
            )
            print(f"chunk embeddings stored: {stored}")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--messages", action="store_true")
    parser.add_argument("--chunk-set", type=int, default=None, metavar="ID")
    parser.add_argument("--model", default="intfloat/multilingual-e5-base",
                        choices=sorted(SPECS))
    parser.add_argument("--user", type=int, default=None, metavar="TG_ID",
                        help="only this user's messages and chunks, for a trial run")
    parser.add_argument("--force", action="store_true")
    arguments = parser.parse_args()
    if not arguments.messages and arguments.chunk_set is None:
        parser.error("nothing to do: pass --messages and/or --chunk-set ID")
    asyncio.run(_main(arguments))
