from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Annotated

import structlog
import typer

from comment_cleaner.config import load_config
from comment_cleaner.exporters import write_jsonl, write_user_batches
from comment_cleaner.group_users import group_users_from_jsonl
from comment_cleaner.models import ProcessedMessage
from comment_cleaner.pipeline import process_messages
from comment_cleaner.privacy import Pseudonymizer

app = typer.Typer(
    name="comment-cleaner",
    help="Preprocessing pipeline for Russian-language Telegram comments",
    add_completion=False,
)

logger = logging.getLogger(__name__)


@app.command()
def validate(
    config_path: Annotated[
        str | None,
        typer.Option("--config", "-c", help="Path to YAML configuration file"),
    ] = None,
) -> None:
    cfg = load_config(config_path)
    input_path = Path(cfg.input_path)

    if not input_path.exists():
        typer.echo(f"Error: input file not found: {input_path}", err=True)
        raise typer.Exit(code=1)

    typer.echo("Configuration loaded successfully.")
    typer.echo(f"  Input: {cfg.input_path}")
    typer.echo(f"  Input type: {cfg.input_type}")
    typer.echo(f"  Output: {cfg.output_path}")

    if cfg.input_type == "jsonl":
        typer.echo(f"  File exists: {input_path.exists()}")
        typer.echo(f"  File size: {input_path.stat().st_size:,} bytes")

    typer.echo("Validation complete — configuration is valid.")


@app.command()
def clean(
    config_path: Annotated[
        str | None,
        typer.Option("--config", "-c", help="Path to YAML configuration file"),
    ] = None,
    pseudonymize: Annotated[
        bool,
        typer.Option("--pseudonymize", help="Enable pseudonymization of user IDs"),
    ] = False,
    progress: Annotated[
        bool,
        typer.Option("--progress/--no-progress", help="Show progress bar"),
    ] = True,
) -> None:
    cfg = load_config(config_path)

    output_path = Path(cfg.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    pseudonymizer = Pseudonymizer(salt_env_var=cfg.salt_env_variable)
    if pseudonymize and not pseudonymizer.is_active:
        salt_var = cfg.salt_env_variable
        typer.echo(
            f"Warning: pseudonymization requested but {salt_var} env variable is not set",
            err=True,
        )
        typer.echo("IDs will NOT be pseudonymized.", err=True)

    typer.echo(f"Processing: {cfg.input_path}")
    typer.echo(f"  Input type: {cfg.input_type}")
    typer.echo(f"  Output: {cfg.output_path}")

    batch: list[ProcessedMessage] = []
    total_written = 0
    batch_size = cfg.batch_size

    show_progress = progress and cfg.show_progress

    for msg in process_messages(cfg):
        if msg is None:
            continue

        if pseudonymize and pseudonymizer.is_active:
            msg_dict = msg.model_dump()
            msg_dict = pseudonymizer.pseudonymize_message(msg_dict)
            msg = ProcessedMessage.model_validate(msg_dict)

        batch.append(msg)

        if len(batch) >= batch_size:
            total_written += write_jsonl(batch, output_path)
            batch.clear()
            if show_progress:
                typer.echo(f"  Written: {total_written} records", err=False)

    if batch:
        total_written += write_jsonl(batch, output_path)

    typer.echo(f"\nDone. Total written: {total_written} records")
    typer.echo(f"Output: {output_path}")


@app.command()
def deduplicate(
    config_path: Annotated[
        str | None,
        typer.Option("--config", "-c", help="Path to YAML configuration file"),
    ] = None,
    input_file: Annotated[
        str | None,
        typer.Option("--input", "-i", help="Input JSONL file (overrides config)"),
    ] = None,
    output_file: Annotated[
        str | None,
        typer.Option("--output", "-o", help="Output JSONL file (overrides config)"),
    ] = None,
    mode: Annotated[
        str,
        typer.Option("--mode", help="Duplicate mode: keep | mark | collapse"),
    ] = "mark",
    threshold: Annotated[
        int,
        typer.Option("--threshold", help="Fuzzy matching threshold (0-100)"),
    ] = 95,
) -> None:
    from comment_cleaner.processors.duplicate_detector import DuplicateDetector

    cfg = load_config(config_path)
    input_path = input_file or cfg.input_path
    output_path_str = output_file or cfg.output_path
    output_path = Path(output_path_str)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cfg._data["duplicates"]["mode"] = mode
    cfg._data["duplicates"]["fuzzy_threshold"] = threshold

    detector = DuplicateDetector(cfg)

    batch: list[ProcessedMessage] = []
    total = 0
    dup_count = 0

    with open(input_path, encoding="utf-8") as fin:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                msg = ProcessedMessage.model_validate(data)
                msg = detector.process(msg)
                if msg.is_duplicate:
                    dup_count += 1
                if mode == "collapse" and msg.is_duplicate:
                    continue
                batch.append(msg)
                total += 1

                if len(batch) >= cfg.batch_size:
                    write_jsonl(batch, output_path)
                    batch.clear()
            except Exception as exc:
                logger.warning("Error processing record: %s", exc)

    if batch:
        write_jsonl(batch, output_path)

    typer.echo(f"Done. Processed {total} records, {dup_count} duplicates found.")
    typer.echo(f"Output: {output_path}")


@app.command(name="group-users")
def group_users(
    config_path: Annotated[
        str | None,
        typer.Option("--config", "-c", help="Path to YAML configuration file"),
    ] = None,
    input_file: Annotated[
        str,
        typer.Option("--input", "-i", help="Input cleaned JSONL file"),
    ] = "output/cleaned.jsonl",
    output_file: Annotated[
        str,
        typer.Option("--output", "-o", help="Output user batches JSONL file"),
    ] = "output/user_batches.jsonl",
    max_messages: Annotated[
        int | None,
        typer.Option("--max-messages", help="Maximum messages per user batch"),
    ] = None,
    max_chars: Annotated[
        int | None,
        typer.Option("--max-chars", help="Maximum characters per user batch"),
    ] = None,
    exclude_duplicates: Annotated[
        bool,
        typer.Option("--exclude-duplicates/--include-duplicates"),
    ] = True,
    exclude_low_info: Annotated[
        bool,
        typer.Option("--exclude-low-info/--include-low-info"),
    ] = False,
    exclude_system: Annotated[
        bool,
        typer.Option("--exclude-system/--include-system"),
    ] = True,
    exclude_bots: Annotated[
        bool,
        typer.Option("--exclude-bots/--include-bots"),
    ] = True,
) -> None:
    batches = group_users_from_jsonl(
        input_path=input_file,
        max_messages_per_user=max_messages,
        max_chars_per_user=max_chars,
        exclude_duplicates=exclude_duplicates,
        exclude_low_information=exclude_low_info,
        exclude_system=exclude_system,
        exclude_bots=exclude_bots,
    )

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _ = write_user_batches(batches, output_path)

    total_messages = sum(b.comments_count for b in batches)
    typer.echo(f"Done. {len(batches)} users, {total_messages} messages total.")
    typer.echo(f"Output: {output_path}")


@app.command()
def stats(
    config_path: Annotated[
        str | None,
        typer.Option("--config", "-c", help="Path to YAML configuration file"),
    ] = None,
    input_file: Annotated[
        str | None,
        typer.Option("--input", "-i", help="Input JSONL file"),
    ] = None,
) -> None:
    cfg = load_config(config_path)
    input_path = input_file or cfg.output_path

    path = Path(input_path)
    if not path.exists():
        typer.echo(f"Error: file not found: {path}", err=True)
        raise typer.Exit(code=1)

    total = 0
    empty = 0
    with_url = 0
    with_mentions = 0
    with_quotes = 0
    with_reply = 0
    reply_missing = 0
    exact_dup = 0
    norm_dup = 0
    low_info = 0
    bot_msgs = 0
    sys_msgs = 0
    with_slang = 0
    sarcasm = 0
    length_dist: dict[str, int] = {}

    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                msg = ProcessedMessage.model_validate(data)
            except Exception:
                continue

            total += 1

            if not msg.cleaned_text.strip():
                empty += 1
            if msg.features.contains_url:
                with_url += 1
            if msg.features.contains_mention:
                with_mentions += 1
            if msg.features.contains_quote:
                with_quotes += 1
            if msg.features.contains_reply_context:
                with_reply += 1
            if msg.reply_context_missing:
                reply_missing += 1
            if msg.duplicate_type == "exact":
                exact_dup += 1
            elif msg.duplicate_type == "normalized":
                norm_dup += 1
            if msg.features.low_information:
                low_info += 1
            if msg.features.is_bot_message:
                bot_msgs += 1
            if msg.features.is_system_message:
                sys_msgs += 1
            if msg.detected_terms:
                with_slang += 1
            if msg.features.possible_sarcasm:
                sarcasm += 1

            length = len(msg.original_text)
            if length == 0:
                bucket = "0"
            elif length <= 10:
                bucket = "1-10"
            elif length <= 50:
                bucket = "11-50"
            elif length <= 200:
                bucket = "51-200"
            elif length <= 500:
                bucket = "201-500"
            elif length <= 1000:
                bucket = "501-1000"
            elif length <= 5000:
                bucket = "1001-5000"
            else:
                bucket = "5000+"
            length_dist[bucket] = length_dist.get(bucket, 0) + 1

    typer.echo("\n=== Processing Statistics ===")
    typer.echo(f"  Total records:                  {total}")
    typer.echo(f"  Empty messages:                 {empty}")
    typer.echo(f"  Messages with URLs:             {with_url}")
    typer.echo(f"  Messages with mentions:         {with_mentions}")
    typer.echo(f"  Messages with quotes:           {with_quotes}")
    typer.echo(f"  Messages with reply context:    {with_reply}")
    typer.echo(f"  Messages without reply context: {reply_missing}")
    typer.echo(f"  Exact duplicates:               {exact_dup}")
    typer.echo(f"  Normalized duplicates:          {norm_dup}")
    typer.echo(f"  Low information messages:       {low_info}")
    typer.echo(f"  Bot messages:                   {bot_msgs}")
    typer.echo(f"  System messages:                {sys_msgs}")
    typer.echo(f"  Messages with political slang:  {with_slang}")
    typer.echo(f"  Possible sarcasm detected:      {sarcasm}")
    typer.echo("\n  Length distribution:")
    for bucket in ["0", "1-10", "11-50", "51-200", "201-500", "501-1000", "1001-5000", "5000+"]:
        count = length_dist.get(bucket, 0)
        pct = (count / total * 100) if total > 0 else 0
        typer.echo(f"    {bucket:>10s}: {count:>8d} ({pct:5.1f}%)")


def main() -> None:
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    app()


if __name__ == "__main__":
    main()
