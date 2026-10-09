#!/usr/bin/env bash
# The whole backend suite in Docker, PostgreSQL tests included, against the
# throwaway database of docker-compose.yml.
set -e

export UID
export GID="${GID:-$(id -g)}"

docker compose --profile test up -d --wait postgres-test
exec docker compose --profile cli --profile test run --rm \
  -e TEST_DATABASE_URL=postgresql+asyncpg://telegram:telegram@postgres-test:5432/telegram_comments_test \
  --entrypoint python app -m pytest -vv -ra -p no:cacheprovider tests "$@"
