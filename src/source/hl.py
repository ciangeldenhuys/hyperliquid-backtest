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
        self._trade_handlers = []

    def time(self):
        return datetime.now()

    def add_trade_handler(self, handler):
        self._trade_handlers.append(handler)

    def _handle_trade(self, trades):
        for handler in self._trade_handlers:
            handler(trades)

    def stream_trades(self):
        info.subscribe({'type': 'trades', 'coin': self.coin}, self._handle_trade)

    def create_buy_order(self, buy_size, allowed_slip):
        order_result = exchange.market_open(self.coin, True, buy_size, None, allowed_slip)
        if order_result['status'] == 'ok':
            for status in order_result['response']['data']['statuses']:
                try:
                    filled = status['filled']
                    print(f'Order #{filled['oid']} filled {filled['totalSz']} @{filled['avgPx']}')
                except KeyError:
                    print(f'Error: {status['error']}')

    def create_sell_order(self, sell_size, allowed_slip):
        order_result = exchange.market_close(self.coin, sell_size, None, allowed_slip)
        if order_result["status"] == "ok":
            for status in order_result["response"]["data"]["statuses"]:
                try:
                    filled = status["filled"]
                    print(f'Order #{filled["oid"]} filled {filled["totalSz"]} @{filled["avgPx"]}')
                except KeyError:
                    print(f'Error: {status["error"]}')

    def market_price(self):
        mids = info.all_mids()
        return float(mids.get(self.coin))
    

    def wallet_assets(self):
        user_state = info.user_state(ADDRESS)
        return user_state['assetPositions']