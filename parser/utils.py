import re
from pathlib import Path
from config import DB_PATH
import argparse
import unicodedata
import sqlite3
import aiosqlite

def normalize(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    return text.lower()

def get_db_path() -> Path:
    return Path(DB_PATH)

async def list_channels() -> list[str]:
    db_path = Path(DB_PATH)
    if not db_path.exists():
        return []

    try:
        async with aiosqlite.connect(str(db_path)) as conn:
            async with conn.execute(
                "SELECT name FROM channels ORDER BY name ASC"
            ) as cur:
                rows = await cur.fetchall()
                return [r[0] for r in rows]
    except sqlite3.Error:
        return []

def parse_args():
    parser = argparse.ArgumentParser(
        description="Telegram parser"
    )

    parser.add_argument(
        "mode",
        nargs="?",
        default="collect",
        choices=["collect", "haters"],
        help="Run mode"
    )

    return parser.parse_args()
