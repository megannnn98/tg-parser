import re
from pathlib import Path
from config import DATA_DIR
import argparse
import unicodedata

def normalize(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    return text.lower()

def sanitize_channel_name(name: str) -> str:
    name = name.strip().lstrip("@").lower()
    return re.sub(r"[^a-z0-9_]", "_", name)

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
