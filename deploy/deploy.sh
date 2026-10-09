#!/usr/bin/env bash
# Rolls the server to the newest commit of a branch. Run from a work computer:
#   ssh root@SERVER 'bash -s' < deploy/deploy.sh
#   ssh root@SERVER 'BRANCH=feat/auth-deploy bash -s' < deploy/deploy.sh
# Steps: working tree check -> backup -> git pull -> build -> migration -> web.
# Recreating `web` ends a comment collection or an analysis that is running.
set -euo pipefail

APP_DIR="${APP_DIR:-/root/telegram-comments}"
BRANCH="${BRANCH:-main}"
cd "$APP_DIR"

COMPOSE=(docker compose -f docker-compose.prod.yml)
PG=telegram-comments-prod-postgres-1
WEB=telegram-comments-prod-web-1

echo "== 1. working tree"
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "STOP: uncommitted changes in $APP_DIR. Nothing was changed."
  git status --short
  exit 1
fi
[ -f .env ] || { echo "STOP: $APP_DIR/.env is missing. Nothing was changed."; exit 1; }

echo "== 2. backup"
if docker inspect "$PG" >/dev/null 2>&1; then
  mkdir -p backups
  BACKUP="backups/telegram_comments-$(date +%F-%H%M)-pre-deploy.dump"
  docker exec "$PG" sh -c 'pg_dump -U "$POSTGRES_USER" -Fc "$POSTGRES_DB"' </dev/null > "$BACKUP"
  ls -la "$BACKUP"
  [ -s "$BACKUP" ] || { echo "STOP: the backup is empty."; exit 1; }
else
  echo "no database container yet: first deployment, nothing to back up"
fi

echo "== 3. code"
BEFORE=$(git rev-parse --short HEAD)
git fetch --quiet origin "$BRANCH"
git checkout --quiet "$BRANCH"
git pull --ff-only origin "$BRANCH"
AFTER=$(git rev-parse --short HEAD)
echo "was $BEFORE, now $AFTER"

echo "== 4. image"
"${COMPOSE[@]}" build web </dev/null 2>&1 | tail -3

echo "== 5. database and migration"
"${COMPOSE[@]}" up -d postgres </dev/null 2>&1 | tail -2
"${COMPOSE[@]}" run --rm migrate </dev/null 2>&1 | tail -3

echo "== 6. web"
"${COMPOSE[@]}" up -d --no-deps --no-build --force-recreate web </dev/null 2>&1 | tail -2
STATE=starting
for _ in $(seq 1 45); do
  STATE=$(docker inspect -f '{{.State.Health.Status}}' "$WEB")
  [ "$STATE" = healthy ] && break
  sleep 4
done
echo "web: $STATE"
[ "$STATE" = healthy ] || {
  echo "STOP: web did not come up. Logs: docker logs $WEB. Roll back: git checkout $BEFORE, build, up."
  exit 1
}

echo "== 7. checks"
docker exec "$WEB" python -c "
import urllib.request as u, urllib.error as e
def status(path):
    try:
        return u.urlopen('http://127.0.0.1:8000' + path, timeout=5).status
    except e.HTTPError as error:
        return error.code
print('login page:', status('/login'), '(200 expected)')
print('API without a session:', status('/api/v1/profiles'), '(401 expected)')
" </dev/null

echo "== done"
