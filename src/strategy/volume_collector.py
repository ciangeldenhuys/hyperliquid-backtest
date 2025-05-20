import source
from collections import deque
import asyncio
from datetime import datetime, timezone

class VolumeCollector:
    def __init__(self, source: source.Source):
        self.buy_volume_buffer = deque(maxlen=2880)
        self.sell_volume_buffer = deque(maxlen=2880)
        self.buy_usd = 0.0
        self.sell_usd = 0.0
        self.last_clear = -1

        self.source = source
        source.stream_trades()
        self.source.add_trade_handler(self.trade_handler)

    def trade_handler(self, trades):
        for slot in trades["data"]:
            if abs(int(datetime.now(tz = timezone.utc).timestamp() * 1000 - slot["time"])) <= 10000:
                price = float(slot["px"])
                size  = float(slot["sz"])
                usd = price * size
        
                if slot["side"] == "A":
                    self.sell_usd += usd
                if slot["side"] == "B":
                    self.buy_usd += usd

    async def flush(self):        
        while True:
            now = self.source.time().second
            if (now == 30 or now == 0) and now != self.last_clear:
                self.buy_volume_buffer.append(self.buy_usd)
                self.sell_volume_buffer.append(self.sell_usd)
                print("Time:", datetime.now())
                print("Buffer size:", len(self.buy_volume_buffer))
                print("Buy buffer:", list(self.buy_volume_buffer))
                print("Sell buffer:", list(self.sell_volume_buffer))
                self.buy_usd = 0.0
                self.sell_usd = 0.0
                self.last_clear = now
                
            await asyncio.sleep(0.1)
