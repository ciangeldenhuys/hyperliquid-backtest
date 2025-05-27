from source import Source
from database import DatabaseSync as db
from database import CONNECTION_STR
import statistics
import psycopg
from psycopg.connection import Connection
from datetime import datetime

READ_SIZE = 100000

class Backtest(Source):
    def __init__(self, coin: str, start: datetime, end: datetime, withdrawable: float):
        self.coin = coin
        self.end = end
        self._time = start
        self._last_id = -1

        self._market_price = None
        self._last_sell = None
        self._last_buy = None

        self._trade_handlers = []
        self._wallet = {
            'assetPositions': [
                {
                    'position': {
                        'coin': self.coin,
                        'szi': 0
                    }
                }
            ],
            'withdrawable': withdrawable
        }

    def stream_trades(self):
        with psycopg.Connection.connect(CONNECTION_STR) as conn:
            while rows := self._get_rows(conn):
                for row in rows:
                    price, quantity, side, trade_time, trade_id = row
                    self._time = trade_time
                    self._last_id = trade_id

                    if side:
                        self._last_buy = price
                    else:
                        self._last_sell = price

                    if self._last_buy and self._last_sell:
                        self._market_price = statistics.mean([self._last_buy, self._last_sell])

                    trades = {
                        'data': [
                            {
                                'time': trade_time.timestamp() * 1000,
                                'px': price,
                                'side': 'B' if side else 'A',
                                'sz': quantity
                            }
                        ]
                    }

                    for handler in self._trade_handlers:
                        handler(trades)
    
    def last_sell_price(self):
        return self._last_sell
    
    def last_buy_price(self):
        return self._last_buy

    def _get_rows(self, conn: Connection) -> list[tuple[float, float, bool, datetime]]:
        with conn.cursor() as cur:
            coin_id = db.get_coin_id(self.coin)
            cur.execute("""
                SELECT price, quantity, side, trade_time, trade_id
                FROM trades
                WHERE coin_id = %s
                AND (trade_time, trade_id) > (%s, %s)
                AND trade_time < %s
                AND trade_type = 'spot'
                ORDER BY trade_time, trade_id
                LIMIT %s;
                """, (coin_id, self._time, self._last_id, self.end, READ_SIZE)
            )
            return cur.fetchall()
            
    def add_trade_handler(self, handler):
        self._trade_handlers.append(handler)

    def time(self):
        return self._time

    def market_price(self):
        return self._market_price
    
    def create_buy_order(self, buy_size, allowed_slip):
        self._wallet['assetPositions'][0]['position']['szi'] += buy_size
        self._wallet['withdrawable'] -= buy_size * float(self._last_buy)
    
    def create_sell_order(self, sell_size, allowed_slip):
        self._wallet['assetPositions'][0]['position']['szi'] = max(0, self._wallet['assetPositions'][0]['position']['szi'] - sell_size)
        self._wallet['withdrawable'] += sell_size * float(self._last_sell)
    
    def position_size(self):
        all_positions = self._wallet['assetPositions']
        position_size = 0.0

        for p in all_positions:
            pos = p.get('position')
            if pos.get('coin') == self.coin:
                position_size += float(pos.get('szi'))

        return position_size
    
    def withdrawable(self):
        return self._wallet['withdrawable']
    
    def current_total_usd(self):
        return self._wallet['withdrawable'] + float(self.position_size()) * float(self.last_sell_price())