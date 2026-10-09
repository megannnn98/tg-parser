import argparse
import re

# Usernames and display names come from Telegram and end up in a file name:
# keep letters of any alphabet, digits and underscores, drop everything else.
_WHITESPACE = re.compile(r"\s+")
_UNSAFE_FILENAME_CHARS = re.compile(r"[^\w]+")
_REPEATED_UNDERSCORES = re.compile(r"_{2,}")

def parse_user_ref(value: str) -> int | str:
    ref = value.strip().lstrip("@")
    if not ref:
        raise ValueError("User reference is empty")
    if ref.isdigit():
        return int(ref)
    return ref


def _slugify(value: str | None) -> str:
    if not value:
        return ""
    slug = _WHITESPACE.sub("_", value.strip().casefold())
    slug = _UNSAFE_FILENAME_CHARS.sub("", slug)
    return _REPEATED_UNDERSCORES.sub("_", slug).strip("_")


def join_name(first_name: str | None, last_name: str | None) -> str:
    return " ".join(part for part in (first_name, last_name) if part)


def user_export_filename(
    tg_id: int,
    username: str | None,
    first_name: str | None = None,
    last_name: str | None = None,
) -> str:
    for candidate in (username, join_name(first_name, last_name)):
        slug = _slugify(candidate)
        if slug:
            return f"{slug}_{tg_id}.txt"

    return f"{tg_id}.txt"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Telegram parser"
    )

    parser.add_argument(
        "mode",
        nargs="?",
        default="collect",
        choices=[
            "collect",
            "user-comments",
            "find-user",
            "discover-channels",
            "web",
        ],
        help="Run mode"
    )

    parser.add_argument(
        "user",
        nargs="?",
        default=None,
        help=(
            "Target user (@username or tg_id) for user-comments, "
            "or a search query (part of a name or username) for find-user"
        )
    )

    parser.add_argument(
        "--refresh-text",
        action="store_true",
        help=(
            "user-comments: also take the text of already stored comments "
            "from Telegram again"
        ),
    )

    args = parser.parse_args()

    if args.mode == "user-comments" and not args.user:
        parser.error("user-comments requires a user argument (@username or tg_id)")

    if args.mode == "find-user" and not args.user:
        parser.error("find-user requires a search query (part of a name or username)")

    return args
