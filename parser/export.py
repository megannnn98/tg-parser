import csv
from pathlib import Path


def write_comments_csv(
    data_dir: Path,
    channel: str,
    timestamp: str,
    rows: list[tuple[str, str]],
) -> Path:
    if "/" in channel or "\\" in channel or channel in (".", ".."):
        raise ValueError(f"Unsafe channel name for file path: {channel!r}")

    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / f"comments_{channel}_{timestamp}.csv"

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(("date", "text"))
        writer.writerows(rows)

    return path
