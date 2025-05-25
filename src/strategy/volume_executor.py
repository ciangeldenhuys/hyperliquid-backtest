import source
from collections import deque
from statistics import mean, stdev
import os
import yaml

with open(os.path.join(os.path.dirname(__file__), 'config', 'volume.yaml'), 'r') as f:
    config = yaml.safe_load(f)

MIN_POINTS = config['MIN_POINTS']
THRESHOLD = config['THRESHOLD']
Z_SCORE_MAX = config['Z_SCORE_MAX']
USD_NOTIONAL = config['USD_NOTIONAL']

class VolumeExecutor:
    def __init__(self, source: source.Source):
        self._buy_volume_buffer = deque(maxlen=2880)
        self._sell_volume_buffer = deque(maxlen=2880)
        self._buy_usd = 0.0
        self._sell_usd = 0.0
        self._source = source
        self._source.add_trade_handler(self._trade_handler)
        self._tradetime_marker = None

        self.flush_count = 0

        self.zb = 0
        self.zs = 0

        self._source.stream_trades()

    def _trade_handler(self, trades):
        for slot in trades['data']:
            if abs(int(self._source.time().timestamp() * 1000 - slot['time'])) <= 10000:
                trade_time = self._source.time().timestamp()
                price = float(slot['px'])
                size  = float(slot['sz'])
                usd = price * size

                if self._tradetime_marker is None:
                    self._tradetime_marker = trade_time 
                
                if trade_time >= self._tradetime_marker + 30:
                    self._flush(trade_time)

                if slot['side'] == 'A':
                    self._sell_usd += usd
                if slot['side'] == 'B':
                    self._buy_usd += usd

    def _flush(self, trade_time):
        buy_buf = self._buy_volume_buffer
        sell_buf = self._sell_volume_buffer

        if len(buy_buf) >= 2 and len(sell_buf) >= 1:
            
            latest_buy = buy_buf[-1]
            latest_sell = sell_buf[-1]
            last_buy = buy_buf[-2]

            self.zb = self._z_score(list(buy_buf))
            self.zs = self._z_score(list(sell_buf))
            if self.zb >= THRESHOLD:
                if latest_buy >= latest_sell:
                    if latest_buy >= last_buy:
                        print(f'BUY-VOLUME, MOMENTUM SIGNAL | z = {self.zb}')
                        print('Balance: ', self._source.withdrawable())

                        self._execute_buy_momentum()
                    else:
                        print(f'BUY-VOLUME FALLING AFTER MOMENTUM SIGNAL | z = {self.zb}')
                        print('Balance: ', self._source.withdrawable())

                        self._execute_buy_falling()
                else:
                    print(f'SELL DOMINANT DESPITE BUY SIGNAL | z = {self.zb}')
                    print('Balance: ', self._source.withdrawable())

                    self._execute_sell_dominant()

        self._buy_volume_buffer.append(self._buy_usd)
        self._sell_volume_buffer.append(self._sell_usd)
        #print('Flushed at: ', trade_time)
        #print('Flush Count: ', self.flush_count)
        #print('Buy buffer:', list(self._buy_volume_buffer)[-5:])
        #print('Sell buffer:', list(self._sell_volume_buffer)[-5:])
        #print('Balance: ', self._source.withdrawable())
        self.flush_count += 1
        self._buy_usd = 0.0
        self._sell_usd = 0.0
        self._tradetime_marker = trade_time


    def _z_score(self, series):
        if len(series) < 3:
            return 0.0
        mu = mean(series[:-1])
        sig = stdev(series[:-1])
        return (series[-1] - mu) / sig if sig else 0.0

    def _execute_buy_momentum(self):
        print('Executing BUY MOMENTUM trade...')

        market_price = float(self._source.market_price())

        buy_size = (USD_NOTIONAL * min(self.zb / Z_SCORE_MAX, 1)) / market_price
        print('buy size: ', buy_size)
        self._source.create_buy_order(buy_size)

    def _execute_buy_falling(self):
        print('Executing BUY FALLING trade...')

        buy_buf = self._buy_volume_buffer
        latest_buy = buy_buf[-1]
        last_buy = buy_buf[-2]
        ratio = latest_buy / last_buy
        if ratio <= 0.5 :
            ratio = 1
        
        sell_size = self._source.position_size() * (ratio)
        print('sell size: ', sell_size)

        self._source.create_sell_order(sell_size)

    def _execute_sell_dominant(self):
        print('Executing SELL DOMINANT trade...')

        sell_buf = self._sell_volume_buffer
        buy_buf = self._buy_volume_buffer
        latest_buy = buy_buf[-1]
        latest_sell = sell_buf[-1]
        ratio = latest_buy/latest_sell
        if ratio <= 0.5 :
            ratio = 1
        
        sell_size = self._source.position_size() * (ratio)
        print('sell size: ', sell_size)

        self._source.create_sell_order(sell_size)