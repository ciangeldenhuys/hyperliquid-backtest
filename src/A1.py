import os, time, math, uuid, logging, requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from kucoin.client import Client

# Load variables from .env file
load_dotenv()

# Access credentialshttps://kucoin.onelink.me/iqEP/xy0tdqd1
API_KEY = os.getenv("KUCOIN_API_KEY")
API_SECRET = os.getenv("KUCOIN_API_SECRET")
API_PASSPHRASE = os.getenv("KUCOIN_API_PASSPHRASE")
API_KEY_VERSION = os.getenv("KUCOIN_API_KEY_VERSION")

client = Client(API_KEY, API_SECRET, API_PASSPHRASE)

# #Get the top 50 market-cap coin symbols for kucoin, used coingecko api
# def get_top50_symbols_kucoin_verified(client):
#     url = ("https://api.coingecko.com/api/v3/coins/markets"
#            "?vs_currency=usd&order=market_cap_desc&per_page=50&page=1")
#     gecko_data = requests.get(url, timeout=10).json()
#     top_symbols = {coin['symbol'].upper() for coin in gecko_data}

#     # Get KuCoin trading pairs
#     kucoin_pairs = client.get_symbols()
#     usdc_pairs = []
#     for pair in kucoin_pairs:
#         if pair['quoteCurrency'] == 'USDC':
#             usdc_pairs.append(pair['symbol'])

#     valid_symbols = []  
#     for s in usdc_pairs:                # go through each symbol like "BTC-USDC"
#         base_symbol = s.split("-")[0]   # split it into ["BTC", "USDC"], take "BTC"
#         if base_symbol in top_symbols:  # check if that base coin is in the CoinGecko top list
#             valid_symbols.append(s)     # if yes, keep the full symbol (e.g., "BTC-USDC")

#     return valid_symbols


# def get_top50_symbols_coingecko():
#     url = ("https://api.coingecko.com/api/v3/coins/markets"
#            "?vs_currency=usd&order=market_cap_desc&per_page=50&page=1")
#     gecko_data = requests.get(url, timeout=10).json()
#     top_symbols = [coin['symbol'].upper() for coin in gecko_data]

#     return top_symbols

# def fetch_global_volume(symbol_id: str, n_minutes: int):

#     url = "https://api.coingecko.com/api/v3/coins/{}/market_chart".format(symbol_id)
#     params = {"vs_currency": "usd", "days": "1", "interval": "minute"}
#     data = requests.get(url, params=params, timeout=10).json()
#     volume_data = data["total_volumes"]

#     recent_volumes = [v[1] for v in volume_data[-n_minutes:]]

#     return pd.Series(recent_volumes)


# VOLUMEDATA = fetch_global_volume("BTC", 1440)
# print(VOLUMEDATA)


