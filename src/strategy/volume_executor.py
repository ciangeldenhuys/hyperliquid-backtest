import source
from .volume_collector import VolumeCollector
from statistics import mean, stdev
import asyncio
import yaml
from datetime import datetime

# with open('config\\volume.yaml', 'r') as f:
#     config = yaml.safe_load(f)

# THRESHOLD = config['threshold']
# MIN_POINTS = config['min_points']
# POLL_INTERVAL = config['poll_interval']
# Z_SCORE_MAX = config['z_score_max']
# USD_NOTIONAL = config['usd_notional']

THRESHOLD = 2.5
MIN_POINTS = 0
POLL_INTERVAL = 0.2
Z_SCORE_MAX = 20
USD_NOTIONAL = 1000

class VolumeExecutor:
    def __init__(self, source: source.Source):
        self.zb = 0
        self.zs = 0
        self.source = source
        self.collector = VolumeCollector(source)

    async def start(self):
        try:
            self.source.stream_trades()
            await asyncio.gather(self.monitor(), self.collector.flush())
        except Exception as e:
            print(e)
            tasks = asyncio.all_tasks()
            for task in tasks:
                if task is not asyncio.current_task():
                    task.cancel()

    def _z_score(self, series):
        if len(series) < 2:
            return 0.0
        mu = mean(series[:-1])
        sig = stdev(series[:-1])
        return (series[-1] - mu) / sig if sig else 0.0
    
    async def monitor(self):
        print('monitoring')
        while True:
            print('mon')
            await asyncio.sleep(POLL_INTERVAL)
            print('going')
            
            buy_buf = self.collector.buy_volume_buffer
            sell_buf = self.collector.sell_volume_buffer

            if len(buy_buf) < 2 or len(sell_buf) < 1:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            # Check for None values
            if buy_buf[-1] is None or buy_buf[-2] is None or sell_buf[-1] is None:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            latest_buy = buy_buf[-1]
            latest_sell = sell_buf[-1]
            last_buy = buy_buf[-2]

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
                    
    def execute_buy_momentum(self):
        print("Executing BUY MOMENTUM trade...")

        market_price = self.source.market_price()

        buy_size = (USD_NOTIONAL * min(self.zb / Z_SCORE_MAX, 1)) / market_price

        self.source.create_buy_order(0.01, buy_size)

    def execute_buy_falling(self):
        print("Executing BUY FALLING trade...")

        buy_buf = self.collector.buy_volume_buffer
        latest_buy = buy_buf[-1]
        last_buy = buy_buf[-2]
        ratio = latest_buy / last_buy
        if ratio <= 0.5 :
            ratio = 1
        
        sell_size = self.source.get_position_size * (ratio)

        self.source.create_sell_order(0.01, sell_size)

    def execute_sell_dominant(self):
        print("Executing SELL DOMINANT trade...")

        sell_buf = self.collector.sell_volume_buffer
        buy_buf = self.collector.buy_volume_buffer
        latest_buy = buy_buf[-1]
        latest_sell = sell_buf[-1]
        ratio = latest_buy/latest_sell
        if ratio <= 0.5 :
            ratio = 1
        
        sell_size = self.source.get_position_size * (ratio)

        self.source.create_sell_order(0.01, sell_size)

