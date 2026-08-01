#!/usr/bin/env bash
set -e

if [[ "${1:-}" == "web" ]]; then
  shift
  docker run \
    --user "$(id -u):$(id -g)" \
    --env-file .env.docker \
    -p "${WEB_PORT:-8000}:8000" \
    -v "$(pwd):/app" \
    --entrypoint uvicorn \
    telegram-parser web.app:app --host 0.0.0.0 --port 8000 "$@"
  exit
fi

docker run \
  --user "$(id -u):$(id -g)" \
  --env-file .env.docker \
  -v "$(pwd):/app" \
  telegram-parser "$@"
