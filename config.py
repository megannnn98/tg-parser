import os
import json
import re
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

CHANNELS = json.loads(Path("channels.json").read_text())
DATA_DIR = os.getenv("DATA_DIR", "data")
DB_PATH = os.getenv("DB_PATH", os.path.join(DATA_DIR, "app.db"))
LIMIT = int(os.getenv("LIMIT", 1000))
