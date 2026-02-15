import re
from pathlib import Path
from config import DB_PATH
import argparse
import unicodedata
import sqlite3

def normalize(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    return text.lower()

def get_db_path() -> Path:
    return Path(DB_PATH)

def list_channels() -> list[str]:
    db_path = Path(DB_PATH)
    if not db_path.exists():
        return []

    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.execute("SELECT name FROM channels ORDER BY name ASC")
        rows = cur.fetchall()
        return [r[0] for r in rows]
    except sqlite3.Error:
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass

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
