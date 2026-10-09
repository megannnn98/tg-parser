import os
import json
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

CHANNELS_PATH = Path(os.getenv("CHANNELS_PATH", "channels.json"))
CHANNELS = json.loads(CHANNELS_PATH.read_text())
# Model caches and logs; the comments themselves live in PostgreSQL (DATABASE_URL).
DATA_DIR = os.getenv("DATA_DIR", "data")
LIMIT = int(os.getenv("LIMIT", 1000))
# Time zone of the activity charts: the hour and the day a comment was written in.
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Asia/Almaty")
# discover-channels stops resolving candidates once the list reaches this size.
DISCOVER_TARGET = int(os.getenv("DISCOVER_TARGET", 200))
