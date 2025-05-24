import source
import asyncio
from collections import deque
from statistics import mean, stdev

THRESHOLD = 2.5
MIN_POINTS = 0
POLL_INTERVAL = 0.2
Z_SCORE_MAX = 20
USD_NOTIONAL = 1000


class VolumeCollectorExecutor:
    def __init__(self, source: source.Source):
        self.buy_volume_buffer = deque(maxlen=2880)
        self.sell_volume_buffer = deque(maxlen=2880)
        self.buy_usd = 0.0
        self.sell_usd = 0.0
        self.last_clear = -1
        self.source = source
        self.source.add_trade_handler(self.trade_handler)
        self.tradetime_marker = None

        self.zb = 0
        self.zs = 0


    def trade_handler(self, trades):
        for slot in trades["data"]:
            if abs(int(self.source.time().timestamp() * 1000 - slot["time"])) <= 10000:
                trade_time = self.source.time().timestamp()
                price = float(slot["px"])
                size  = float(slot["sz"])
                usd = price * size

                if self.tradetime_marker is None:
                    self.tradetime_marker = trade_time 
                
                if trade_time >= self.tradetime_marker + 30:
                    self.flush(trade_time)

                if slot["side"] == "A":
                    self.sell_usd += usd
                if slot["side"] == "B":
                    self.buy_usd += usd

    def flush(self, trade_time):

        buy_buf = self.buy_volume_buffer
        sell_buf = self.sell_volume_buffer

        if len(buy_buf) >= 2 and len(sell_buf) >= 1:
            
            latest_buy = buy_buf[-1]
            latest_sell = sell_buf[-1]
            last_buy = buy_buf[-2]

            self.zb = self._z_score(list(buy_buf))
            self.zs = self._z_score(list(sell_buf))
            if self.zb >= THRESHOLD:
                if latest_buy >= latest_sell:
                    if latest_buy >= last_buy:
                        print(f"BUY-VOLUME, MOMENTUM SIGNAL | z = {self.zb}")
                        self.execute_buy_momentum()
                    else:
                        print(f"BUY-VOLUME FALLING AFTER MOMENTUM SIGNAL | z = {self.zb}")
                        self.execute_buy_falling()
                else:
                    print(f"SELL DOMINANT DESPITE BUY SIGNAL | z = {self.zb}")
                    self.execute_sell_dominant()

        self.buy_volume_buffer.append(self.buy_usd)
        self.sell_volume_buffer.append(self.sell_usd)
        print("Flushed at: ", trade_time)
        print("Buy buffer:", list(self.buy_volume_buffer))
        print("Sell buffer:", list(self.sell_volume_buffer))
        self.buy_usd = 0.0
        self.sell_usd = 0.0
        self.tradetime_marker = trade_time


    def _z_score(self, series):
        if len(series) < 2:
            return 0.0
        mu = mean(series[:-1])
        sig = stdev(series[:-1])
        return (series[-1] - mu) / sig if sig else 0.0

    def execute_buy_momentum(self):
        print("Executing BUY MOMENTUM trade...")

        market_price = self.source.market_price()

        buy_size = (USD_NOTIONAL * min(self.zb / Z_SCORE_MAX, 1)) / market_price

        self.source.create_buy_order(0.01, buy_size)

    def execute_buy_falling(self):
        print("Executing BUY FALLING trade...")

        buy_buf = self.buy_volume_buffer
        latest_buy = buy_buf[-1]
        last_buy = buy_buf[-2]
        ratio = latest_buy / last_buy
        if ratio <= 0.5 :
            ratio = 1
        
        sell_size = self.source.get_position_size() * (ratio)

        self.source.create_sell_order(0.01, sell_size)

    def execute_sell_dominant(self):
        print("Executing SELL DOMINANT trade...")

        sell_buf = self.sell_volume_buffer
        buy_buf = self.buy_volume_buffer
        latest_buy = buy_buf[-1]
        latest_sell = sell_buf[-1]
        ratio = latest_buy/latest_sell
        if ratio <= 0.5 :
            ratio = 1
        
        sell_size = self.source.get_position_size() * (ratio)

        self.source.create_sell_order(0.01, sell_size)