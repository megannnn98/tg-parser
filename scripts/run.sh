#!/usr/bin/env bash
set -e

if [[ -n "$1" ]]; then
  docker run \
    --env-file .env.docker \
    -v "$(pwd):/app" \
    telegram-parser "$1"
else
  docker run \
    --env-file .env.docker \
    -v "$(pwd):/app" \
    telegram-parser
fi
