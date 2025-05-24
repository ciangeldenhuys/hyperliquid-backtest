from source import Source
import os
from dotenv import load_dotenv
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils.constants import MAINNET_API_URL
from datetime import datetime, timezone

load_dotenv()
ADDRESS = os.getenv('METAMASK_ADDRESS')

class Hyperliquid(Source):
    def __init__(self, coin: str):
        self._coin = coin
        self._trade_handlers = []
        self._info = Info(MAINNET_API_URL)
        self._exchange = Exchange(ADDRESS, MAINNET_API_URL)

    def time(self):
        return datetime.now(tz=timezone.utc)

    def add_trade_handler(self, handler):
        self._trade_handlers.append(handler)

    def _handle_trade(self, trades):
        for handler in self._trade_handlers:
            handler(trades)

    def stream_trades(self):
        self._streaming = True
        self._info.subscribe({'type': 'trades', 'coin': self._coin}, self._handle_trade)

    def create_buy_order(self, buy_size, allowed_slip):
        order_result = self._exchange.market_open(self._coin, True, buy_size, None, allowed_slip)
        if order_result['status'] == 'ok':
            for status in order_result['response']['data']['statuses']:
                try:
                    filled = status['filled']
                    print(f'Order #{filled['oid']} filled {filled['totalSz']} @{filled['avgPx']}')
                except KeyError:
                    print(f'Error: {status['error']}')

    def create_sell_order(self, sell_size, allowed_slip):
        order_result = self._exchange.market_close(self._coin, sell_size, None, allowed_slip)
        if order_result['status'] == 'ok':
            for status in order_result['response']['data']['statuses']:
                try:
                    filled = status['filled']
                    print(f'Order #{filled['oid']} filled {filled['totalSz']} @{filled['avgPx']}')
                except KeyError:
                    print(f'Error: {status['error']}')

    def market_price(self):
        mids = self._info.all_mids()
        return float(mids.get(self._coin))
    

    def _wallet_assets(self):
        user_state = self._info.user_state(ADDRESS)
        return user_state['assetPositions']

    def position_size(self):
        all_positions = self._wallet_assets()
        position_size = 0.0

        for p in all_positions:
            pos = p.get('position')
            if pos.get('coin') == self._coin:
                position_size += float(pos.get('szi'))

        return position_size

    def withdrawable(self):
        user_data = self._info.user_state(ADDRESS)
        withdrawable = user_data['response']['data'].get('withdrawable')
        return float(withdrawable)