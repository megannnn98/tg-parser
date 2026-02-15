#!/usr/bin/env bash
docker run --rm -v "$(pwd):/app" --entrypoint python telegram-parser -m pytest -vv -ra tests
