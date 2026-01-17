#!/usr/bin/env bash
set -e

PWD=`pwd`

docker run --rm -v "$(pwd):/app" telegram-parser
