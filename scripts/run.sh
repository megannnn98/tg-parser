#!/usr/bin/env bash
# Runs a mode in Docker next to the PostgreSQL of docker-compose.yml:
#   ./scripts/run.sh migrate                  apply the database migrations
#   ./scripts/run.sh web                      the web UI on ${WEB_PORT:-8000}
#   ./scripts/run.sh user-comments @username  any other mode of main.py
set -e

# Files written to the mounted project stay owned by the caller.
export UID
export GID="${GID:-$(id -g)}"

case "${1:-}" in
  migrate)
    exec docker compose --profile cli run --rm --entrypoint alembic app upgrade head
    ;;
  web)
    shift
    exec docker compose run --rm --service-ports web "$@"
    ;;
  *)
    exec docker compose --profile cli run --rm app "$@"
    ;;
esac
