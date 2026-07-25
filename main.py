import asyncio
from parser.collector import collect_db, CollectorConfig
from parser.utils import parse_args
from config import CHANNELS, DB_PATH
from pathlib import Path

async def main():
    args = parse_args()
    db_path = Path(DB_PATH)

    if args.mode == "collect":
        cfg = CollectorConfig(channels=CHANNELS)
        await collect_db(db_path, cfg)
        return

if __name__ == "__main__":
    asyncio.run(main())
