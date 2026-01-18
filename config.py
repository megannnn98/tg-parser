import os
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
LIMIT = int(os.getenv("LIMIT", 10))

CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")
EXPORT_ENCODING = "utf-8"
DB_PATH = os.getenv("DB_PATH" + CHANNEL_USERNAME + ".db")
