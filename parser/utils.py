import argparse
import unicodedata

def normalize(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    return text.lower()

def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        description="Telegram parser"
    )

    parser.add_argument(
        "mode",
        nargs="?",
        default="collect",
        choices=["collect", "comments"],
        help="Run mode"
    )
    parser.add_argument(
        "--username",
        default=None,
        help="Telegram username to collect comments for (required for comments mode)"
    )

    return parser.parse_args(argv)
