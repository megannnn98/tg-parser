#!/bin/sh
# Prepares the data volume on its first start, then runs the command.
set -e

mkdir -p "$SESSION_DIR"
# The list edited on the site lives in the volume; the image only seeds it.
[ -f "$CHANNELS_PATH" ] || cp /app/channels.json "$CHANNELS_PATH"

exec "$@"
