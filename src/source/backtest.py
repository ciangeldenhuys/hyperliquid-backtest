from .source import Source
import psycopg
import datetime
import statistics
import asyncio
from database import database_sync as db
from database import CONNECTION_STR

READ_SIZE = 100000

asyncio.set_event_loop_policy(
    asyncio.WindowsSelectorEventLoopPolicy()
)

class Backtest(Source):
    def __init__(self, coin: str, start: datetime, end: datetime, withdrawable: float):
        self.coin = coin
        self.start = start
        self.end = end
        self._time = None
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

    async def _stream_trades(self):
        step = 0
        while rows := await self._get_rows(step):
            step += 1
            for row in rows:
                price, quantity, side, trade_time = row

                self._time = trade_time

                if side:
                    self._last_buy = price
                else:
                    self._last_sell = price

                if self._last_buy and self._last_sell:
                    self._market_price = statistics.mean([self._last_buy, self._last_sell])

                trades = {
                    'data': [
                        {
                            'px': price,
                            'side': 'B' if side else 'A',
                            'sz': quantity
                        }
                    ]
                }

                for handler in self._trade_handlers:
                    handler(trades)

    async def _get_rows(self, step):
        async with await psycopg.AsyncConnection.connect(CONNECTION_STR) as conn:
            async with conn.cursor() as cur:
                coin_id = db.DatabaseSync.get_coin_id(self.coin)
                await cur.execute("""
                    SELECT price, quantity, side, trade_time
                    FROM trades
                    WHERE coin_id = %s
                    AND trade_time < %s
                    ORDER BY trade_time
                    OFFSET %s ROWS
                    FETCH NEXT %s ROWS ONLY;
                    """, (coin_id, self.end, READ_SIZE * step, READ_SIZE)
                )
                return await cur.fetchall()

    def stream_trades(self):
        asyncio.create_task(self._stream_trades())

    def add_trade_handler(self, handler):
        self._trade_handlers.append(handler)

    def time(self):
        return self._time

    def market_price(self):
        return self._market_price
    
    def create_buy_order(self, buy_size, allowed_slip):
        self._wallet['assetPositions'][0]['position']['szi'] += buy_size
    
    def create_sell_order(self, sell_size, allowed_slip):
        self._wallet['assetPositions'][0]['position']['szi'] = max(0, self._wallet['assetPositions'][0]['position']['szi'] - sell_size)
    
    def wallet_assets(self):
        return self._wallet
    

