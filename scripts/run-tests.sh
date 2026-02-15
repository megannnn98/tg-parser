#!/usr/bin/env bash
docker run --rm -v "$(pwd):/app" --entrypoint python telegram-parser -m pytest -q tests/test-utils.py
