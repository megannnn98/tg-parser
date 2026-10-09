# The server image: the code and the built frontend are inside, nothing is
# mounted from the project. Build from the repository root:
#   docker compose -f docker-compose.prod.yml build
FROM node:22-slim AS frontend

WORKDIR /frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build


FROM python:3.12-slim

WORKDIR /app

# tgcrypto has no wheel for this Python and is compiled.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# The CPU build: the default one brings several gigabytes of CUDA libraries
# the server has no use for.
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY --from=frontend /frontend/dist frontend/dist

# Everything that must outlive the container is under /app/data (a volume):
# the Telegram session, the channel list and the model caches.
RUN useradd --create-home --uid 1000 app \
    && mkdir -p /app/data \
    && chown app:app /app/data
USER app

ENV DATA_DIR=/app/data \
    SESSION_DIR=/app/data/session \
    CHANNELS_PATH=/app/data/channels.json \
    HF_HOME=/app/data/hf-home \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/deploy/entrypoint.sh"]
CMD ["uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8000"]
