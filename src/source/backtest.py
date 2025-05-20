from .source import Source
import asyncio
import datetime
from database import database_sync as db
from database import CONNECTION_STR

READ_SIZE = 100000

class Backtest(Source):
    def __init__(self, coin: str, start: datetime, end: datetime):
        self.coin = coin
        self.start = start
        self.end = end
        self._time = None

    def stream_trades(self):
        pass

    def add_trade_handler(self, handler):
        pass

class BacktestDataStream:
    def __init__(self):
        self.queue = asyncio.Queue()

    async def __aiter__(self):
        while True:
            data = await self.queue.get()
            yield data