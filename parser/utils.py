import argparse
import unicodedata

def normalize(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    return text.lower()

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
