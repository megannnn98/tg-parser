#!/usr/bin/env bash
set -e

docker_tty_flags=()
if [[ -t 0 && -t 1 ]]; then
  docker_tty_flags=(-it)
fi

if [[ -n "$1" ]]; then
  docker run \
    "${docker_tty_flags[@]}" \
    --env-file .env.docker \
    -v "$(pwd):/app" \
    telegram-parser "$1"
else
  docker run \
    "${docker_tty_flags[@]}" \
    --env-file .env.docker \
    -v "$(pwd):/app" \
    telegram-parser
fi
