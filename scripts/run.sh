#!/usr/bin/env bash
set -e

PWD=`pwd`

docker run --env-file .env.docker -v "$(pwd):/app" telegram-parser
