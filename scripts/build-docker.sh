#!/usr/bin/env bash
set -e

PWD=`pwd`
docker build -f ${PWD}/ci/Dockerfile -t telegram-parser .
