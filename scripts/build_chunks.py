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
from pathlib import Path

from chunking.builder import build_chunk_set
from config import DATA_DIR
from db.engine import create_engine, session_factory
from embeddings.e5 import SPECS, E5Encoder


async def _main(args: argparse.Namespace) -> None:
    # Token budgets are counted with the tokenizer of the embedding model.
    encoder = E5Encoder(SPECS[args.model], cache_dir=Path(DATA_DIR) / "e5-cache")
    engine = create_engine()
    try:
        result = await build_chunk_set(
            session_factory(engine),
            args.strategy,
            json.loads(args.params),
            tokenizer_name=f"{encoder.spec.name}@{encoder.spec.revision}",
            count_tokens=encoder.count_tokens,
            force=args.force,
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
    parser.add_argument("--force", action="store_true")
    asyncio.run(_main(parser.parse_args()))
