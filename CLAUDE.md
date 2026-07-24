# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Telegram scraper that collects comments/messages from the discussion threads linked to a list of channels (`channels.json`) into a local SQLite DB, plus a simple "haters" analytics query that finds users who frequently use configured hate words in a given channel. Uses Pyrogram as the Telegram client.

## Commands

Everything runs inside the `telegram-parser` Docker image (built from `ci/Dockerfile`); there's no documented bare-metal setup.

```bash
docker build -t telegram-parser -f ci/Dockerfile .

# Run the app (mode defaults to "collect" if omitted)
./scripts/run.sh collect
./scripts/run.sh haters

# Run the full test suite
./scripts/tests.sh

# Run a single test file / test (same container, direct pytest invocation)
docker run --rm -v "$(pwd):/app" --entrypoint python telegram-parser -m pytest -vv tests/test_collector.py::test_collect_channel_enqueues_user_even_with_null_username
```

`scripts/run.sh` mounts the repo into the container and passes `--env-file .env.docker`; first-time Telegram login requires an interactive TTY (`-it`, which the script auto-detects), since Pyrogram will prompt for login and persist the session to `my_session.session`.

CI (`.github/workflows/ci.yml`) runs `python -m compileall .` and `python -m pytest -q` directly (no Docker) against a prebuilt `ghcr.io/<USERNAME>/tg-parser-ci` image's Python, then on `main` auto-bumps the version with Commitizen (`.cz.toml`) using Conventional Commits.

Commit messages must follow Conventional Commits — enforced by commitlint via pre-commit (`.pre-commit-config.yaml`, `commit-msg` stage).

## Configuration

All config is env-driven via `config.py` (loaded through `python-dotenv` from `.env` / `.env.docker`):

- `API_ID`, `API_HASH` — Telegram API credentials (required, no defaults)
- `SESSION_NAME` — Pyrogram session file base name (default `my_session`)
- `DATA_DIR` (default `data`), `DB_PATH` (default `<DATA_DIR>/app.db`)
- `LIMIT` — max messages fetched per channel per run (default 1000)
- `LOG_LEVEL` (default `INFO`, read directly in `parser/logger.py`)

`CHANNELS` is loaded from `channels.json` at import time, not from env.

## Architecture

Entry point `main.py` dispatches on `args.mode` (`collect` | `haters`, parsed in `parser/utils.py:parse_args`) — there is no shared "app" object, each mode is a standalone async flow.

**Collect pipeline** (`parser/collector.py`) is producer/consumer over a single bounded `asyncio.Queue`:
- `collect_db` spins up one `_db_writer` consumer task and fans out `collect_channel` producers over `cfg.channels`, bounded by `asyncio.Semaphore(cfg.concurrency)`.
- Each `collect_channel` resolves the channel's linked discussion chat via `tg_client.get_chat`, then streams messages from `parser/telegram.py:fetch_messages` and pushes typed tuples onto the queue (`"channel"`, `"user"`, `"message"` — see `_QUEUE_*` constants). A channel with no linked discussion chat is skipped with a warning.
- `_db_writer` batches queued items into in-memory buffers and flushes them as a single transaction (`BEGIN` / `commit` / `rollback` on error) once any buffer reaches `cfg.batch_size`, then does a final flush on shutdown (`None` sentinel).
- Dependency injection is done via the `CollectorDeps` dataclass (`tg_client_factory`, `fetch_messages_fn`, `logger_factory`), which is how tests substitute fakes for Pyrogram — see `TgClient` Protocol in the same file for the minimal client interface expected.
- `collect_db` also handles `AuthKeyUnregistered` by removing stale session files (`parser/telegram.py:reset_session_files`) and retrying login once, and translates Pyrogram's interactive-login `EOFError` (no TTY) into an explicit `RuntimeError`.

**Storage** (`parser/storage.py`) is raw `aiosqlite` (no ORM). Schema: `users(tg_id PK, username)`, `channels(name PK)`, `messages(id, user FK, channel FK, text, date)`, with indexes on `channel`, `(user, channel)`, and `date`. WAL mode + `synchronous=NORMAL` are set in `get_db`. Message text is normalized (NFKC + lowercase, `parser/utils.py:normalize`) before insert. Both single-row (`save_message`, `upsert_user`, `upsert_channel`) and batch (`save_messages_many`, `upsert_users_many`, `upsert_channels_many`) variants exist; the collector pipeline uses the batch variants exclusively.

**Analytics** (`parser/analytics.py:get_haters`) runs a single SQL query per channel that joins `messages`/`users`, computes hate-word hit counts via `LIKE` pattern matching, and returns users above 0% hate rate sorted by percentage/count.

`parser/measure_time.py` provides a `@measure_time(name=...)` decorator (works on both sync and async functions) that logs elapsed wall time; used on `collect_db`.

## Testing conventions

Tests mock at the `CollectorDeps`/`TgClient` boundary rather than hitting real Telegram or SQLite-on-disk-through-the-full-stack assumptions — see `tests/test_collector.py` for the pattern (fake client + fake async-generator `fetch_messages_fn` + queue draining with `get_nowait()`, driven via `asyncio.run`).
