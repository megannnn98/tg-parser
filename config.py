import os
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

CHANNELS = os.getenv("CHANNELS", "")
DATA_DIR = os.getenv("DATA_DIR", "data")
LIMIT = int(os.getenv("LIMIT", 1000))
