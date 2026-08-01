import os
import json
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

CHANNELS_PATH = Path(os.getenv("CHANNELS_PATH", "channels.json"))
CHANNELS = json.loads(CHANNELS_PATH.read_text())
DATA_DIR = os.getenv("DATA_DIR", "data")
DB_PATH = os.getenv("DB_PATH", os.path.join(DATA_DIR, "app.db"))
# Unset: user-comments names the file after the user (<username>_<tg_id>.db).
# Set: that exact file is used instead.
USER_DB_PATH = os.getenv("USER_DB_PATH")
LIMIT = int(os.getenv("LIMIT", 1000))
# discover-channels stops resolving candidates once the list reaches this size.
DISCOVER_TARGET = int(os.getenv("DISCOVER_TARGET", 200))
