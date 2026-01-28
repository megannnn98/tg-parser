import re
from pathlib import Path
from config import DATA_DIR
import argparse

def sanitize_channel_name(name: str) -> str:
    name = name.strip().lstrip("@").lower()
    return re.sub(r"[^a-z0-9_]", "_", name)

def parse_channels(raw: str) -> list[str]:
    return [c.strip().lstrip("@") for c in raw.split(",") if c.strip()]

def db_path_for_channel(channel: str) -> Path:
    safe = sanitize_channel_name(channel)
    return Path(DATA_DIR) / f"{safe}.db"

def list_channels() -> list[str]:
    data_dir = Path(DATA_DIR)
    if not data_dir.exists():
        return []

    channels = []
    for db in data_dir.glob("*.db"):
        channels.append(db.stem)  # ru_doy.db -> ru_doy

    return sorted(channels)

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
