import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

# API
EXCHANGE_API_KEY = os.getenv("EXCHANGE_API_KEY")
EXCHANGE_API_URL = "https://api.apilayer.com/exchangerates_data/latest"

# URLs
BANKS_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_largest_banks"

# Timestamp
CURRENT_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")