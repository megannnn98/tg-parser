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
from pathlib import Path

from config import DATA_DIR
from db.engine import create_engine, session_factory
from embeddings.e5 import SPECS, E5Encoder
from embeddings.pipeline import embed_chunks, embed_messages


async def _main(args: argparse.Namespace) -> None:
    encoder = E5Encoder(SPECS[args.model], cache_dir=Path(DATA_DIR) / "e5-cache")
    engine = create_engine()
    sessions = session_factory(engine)
    try:
        if args.messages:
            stored = await embed_messages(sessions, encoder, force=args.force)
            print(f"message embeddings stored: {stored}")
        if args.chunk_set is not None:
            stored = await embed_chunks(
                sessions, encoder, args.chunk_set, force=args.force
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
    parser.add_argument("--force", action="store_true")
    arguments = parser.parse_args()
    if not arguments.messages and arguments.chunk_set is None:
        parser.error("nothing to do: pass --messages and/or --chunk-set ID")
    asyncio.run(_main(arguments))
