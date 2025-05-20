from source import Source
import os
from datetime import datetime
from dotenv import load_dotenv
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils.constants import MAINNET_API_URL

load_dotenv()
ADDRESS = os.getenv('METAMASK_ADDRESS')

info = Info(MAINNET_API_URL)
exchange = Exchange(ADDRESS, MAINNET_API_URL)

class Hyperliquid(Source):
    def __init__(self, coin: str):
        self.coin = coin
        self._trade_handlers= []

    def time(self):
        return datetime.now()

    def add_trade_handler(self, handler):
        self._trade_handlers.append(handler)

    def _handle_trade(self, trades):
        for handler in self._trade_handlers:
            handler(trades)

    def stream_trades(self):
        info.subscribe({"type": "trades", "coin": self.coin}, self._handle_trade)
