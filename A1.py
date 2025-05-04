from dotenv import load_dotenv
import os

# Load API keys from .env
load_dotenv()
API_KEY = os.getenv("KUCOIN_API_KEY")
API_SECRET = os.getenv("KUCOIN_API_SECRET")
API_PASSPHRASE = os.getenv("KUCOIN_API_PASSPHRASE")
API_KEY_VERSION = os.getenv("KUCOIN_API_KEY_VERSION")
