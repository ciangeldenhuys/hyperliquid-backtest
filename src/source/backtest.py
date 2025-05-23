from source import Source
import psycopg
from datetime import datetime
import statistics
import asyncio
from database import DatabaseSync as db
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
        self._streaming = False
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

    @property
    def streaming(self):
        return self._streaming

    def stream_trades(self):
        self._streaming = True
        asyncio.create_task(self._stream_trades())

    async def _stream_trades(self):
        step = 0
        while rows := await self._get_rows(step):
            step += 1
            for row in rows:
                print(row)
                price, quantity, side, trade_time = row

                if self._time.minute != trade_time.minute:
                    print(self._time)

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
                            'time': trade_time.timestamp() * 1000,
                            'px': price,
                            'side': 'B' if side else 'A',
                            'sz': quantity
                        }
                    ]
                }

                for handler in self._trade_handlers:
                    handler(trades)
        print('left')

    async def _get_rows(self, step) -> list[tuple[float, float, bool, datetime]]:
        async with await psycopg.AsyncConnection.connect(CONNECTION_STR) as conn:
            print('got conn')
            async with conn.cursor() as cur:
                coin_id = db.get_coin_id(self.coin)
                print(coin_id, self.end, READ_SIZE * step, READ_SIZE)
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

                print(await cur.fetchone())

                return await cur.fetchall()
            
    def add_trade_handler(self, handler):
        self._trade_handlers.append(handler)

    def time(self):
        return self._time

    def market_price(self):
        return self._market_price
    
    def create_buy_order(self, buy_size, allowed_slip):
        self._wallet['assetPositions'][0]['position']['szi'] += buy_size
        self._wallet['withdrawable'] -= buy_size * self._last_buy
    
    def create_sell_order(self, sell_size, allowed_slip):
        self._wallet['assetPositions'][0]['position']['szi'] = max(0, self._wallet['assetPositions'][0]['position']['szi'] - sell_size)
        self._wallet['withdrawable'] += sell_size * self._last_sell
    
    def wallet_assets(self):
        return self._wallet
    
    def get_position_size(self):
        all_positions = self.wallet_assets()
        position_size = 0.0

        for p in all_positions:
            pos = p.get("position")
            if pos.get("coin") == self.coin:
                position_size += float(pos.get("szi"))

        return position_size
    
    def get_wallet_balance(self):
        return self._wallet['withdrawable']
    