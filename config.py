import os
import json
import re
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

CHANNELS = json.loads(Path("channels.json").read_text())
DATA_DIR = Path("data")
MAIN_DB = DATA_DIR / "main.db"
LIMIT = int(os.getenv("LIMIT", 1000))
