"""Builds or updates the chunks of one strategy from the stored comments.

    python -m scripts.build_chunks --strategy token_budget \
        --params '{"max_tokens": 256, "overlap": 0, "same_channel": true}'

Only users with comments not yet covered are rebuilt; --force rebuilds
everyone. Several strategies and parameter sets can be stored side by side.
Telegram is not contacted.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from chunking.builder import build_chunk_set
from db import repositories
from db.engine import create_engine, session_factory
from embeddings.e5 import SPECS, E5Encoder


def _model_cache() -> Path:
    # Read here, not from config.py: that module needs the Telegram
    # credentials, and this script never talks to Telegram.
    return Path(os.getenv("DATA_DIR", "data")) / "e5-cache"


async def _main(args: argparse.Namespace) -> None:
    # Token budgets are counted with the tokenizer of the embedding model.
    encoder = E5Encoder(SPECS[args.model], cache_dir=_model_cache())
    engine = create_engine()
    sessions = session_factory(engine)
    try:
        only_user_ids = None
        if args.user is not None:
            async with sessions() as session:
                user = await repositories.get_user_by_tg_id(session, args.user)
            if user is None:
                raise SystemExit(f"No user with tg_id {args.user}")
            only_user_ids = [user.id]
        result = await build_chunk_set(
            sessions,
            args.strategy,
            json.loads(args.params),
            tokenizer_name=f"{encoder.spec.name}@{encoder.spec.revision}",
            count_tokens=encoder.count_tokens,
            force=args.force,
            only_user_ids=only_user_ids,
        )
    finally:
        await engine.dispose()
    print(
        f"chunk set: {result.chunk_set_id}\n"
        f"users rebuilt: {result.users_rebuilt}\n"
        f"chunks written: {result.chunks_written}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--params", default="{}", help="JSON object")
    parser.add_argument("--model", default="intfloat/multilingual-e5-base",
                        choices=sorted(SPECS))
    parser.add_argument("--user", type=int, default=None, metavar="TG_ID",
                        help="only this user, for a trial run")
    parser.add_argument("--force", action="store_true")
    asyncio.run(_main(parser.parse_args()))
