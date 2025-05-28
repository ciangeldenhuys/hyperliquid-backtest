import source
from strategy.utils.deque_avg_var import DequeAvgVar
import strategy.config.volume_config as config
from collections import deque

class VolumeExecutor:
    def __init__(self, source: source.Source, usd_notional: float, graph: bool = True):
        short_buf = config.SHORT_BUF // config.FLUSH
        self._buy_short_buf = DequeAvgVar(maxlen=short_buf)
        self._sell_short_buf = DequeAvgVar(maxlen=short_buf)

        long_buf = config.LONG_BUF // config.FLUSH
        self._buy_long_buf = DequeAvgVar(maxlen=long_buf)
        self._sell_long_buf = DequeAvgVar(maxlen=long_buf)

        self._full_flag = False
        self._buy_usd = 0.0
        self._sell_usd = 0.0
        self._source = source
        self._source.add_trade_handler(self._trade_handler)
        self._tradetime_marker = None
        self._available = usd_notional
        self.usd_notional = usd_notional

        self._rsi_prices_list = deque(maxlen=1000)
        self._rsi = None

        self._zb = 0
        self._zs = 0
        self.count = 0.0  

        self.graph = graph
        if self.graph:
            self.graph_marker = None
            self._first_price = None
            self.balance_values = []
            self.price_values = []
            self.times = []

    def start(self):
        print('Streaming trades')
        self._source.stream_trades()

    def _trade_handler(self, trades):
        for slot in trades['data']:
            if abs(int(self._source.time().timestamp() * 1000 - slot['time'])) < 10000:
                trade_time = self._source.time().timestamp()
                price = float(slot['px'])
                size  = float(slot['sz'])
                usd = price * size

                mid_price = self._source.market_price()
                if mid_price:
                       self._rsi_prices_list.append(mid_price)
                       
                
                if self._tradetime_marker is None:
                    self._tradetime_marker = trade_time

                if trade_time > self._tradetime_marker + config.FLUSH:
                    self._tradetime_marker = trade_time
                    self._flush()

                if self.graph and self._full_flag:
                    self._update_graph()

                if slot['side'] == 'A':
                    self._sell_usd += usd

                if slot['side'] == 'B':
                    self._buy_usd += usd
            
            # self.count += 1
            # print(self.count)
    def _flush(self):
        self._append_all()

        if self._all_full():
            if not self._full_flag:
                print('Buffers full: starting trading')
                self._full_flag = True
            
            self._z_scores()
            self._rsi = self._calc_relative_strength_index()
            if self._zs > config.THRESHOLD_S:
                if self._sell_short_buf.average() > self._buy_short_buf.average(): # if the short-term sell volume average is higher than the short-term buy volume average, sell the whole position
                    if self._source.position_size() > 0:
                        if(self._rsi < 50):
                            print('Selling pressure: selling full position')
                            print(f'Balance: {self._source.current_total_usd()}')
                            self.sell_full_position()
            elif self._zb > config.THRESHOLD: # if there is a short-term buy volume spike, buy some
                if self._available > 0:
                    if(self._rsi > 50):
                        print('Buy volume spike: buying')
                        print('z-score: ', self._zb)
                        print(f'Balance: {self._source.current_total_usd()}')
                        self._partial_buy()
                
    def sell_full_position(self):
        market_sell_price = float(self._source.last_sell_price())
        sell_size = self._source.position_size()
        self._available = self._available + sell_size * market_sell_price

        print(f'Sell size: {sell_size}')
        self._source.create_sell_order(sell_size, 0.01)

    def _partial_buy(self):
        combined_z = max(self._zb, 0) # the z-score is a signed number, so if sell is below average and buy is above averge it will buy even more

        market_buy_price = float(self._source.last_buy_price())
        buy_size = (self._available * min(combined_z / config.Z_SCORE_MAX, 1)) / market_buy_price
        self._available = self._available - min(combined_z / config.Z_SCORE_MAX, 1) * self._available

        print(f'Buy size: {buy_size}')
        self._source.create_buy_order(buy_size, 0.01)

    def _z_scores(self):
        self._zb = (self._buy_short_buf.average() - self._buy_long_buf.average()) / self._buy_long_buf.variance() ** 0.5
        self._zs = (self._sell_short_buf.average() - self._sell_long_buf.average()) / self._sell_long_buf.variance() ** 0.5

    def _update_graph(self):
        if self.graph_marker is None:
            self.graph_marker = self._source.time()

        if self._first_price is None:
            self._first_price = self._source.market_price()

        if self._source.time().timestamp() > self.graph_marker.timestamp() + config.GRAPH_STEP:
            self.price_values.append((self._source.market_price() / self._first_price - 1) * 100)
            self.balance_values.append((self._source.current_total_usd() / self.usd_notional - 1) * 100)
            self.times.append(self._source.time())

    def _append_all(self):
        self._buy_short_buf.append(self._buy_usd)
        self._sell_short_buf.append(self._sell_usd)
        self._buy_long_buf.append(self._buy_usd)
        self._sell_long_buf.append(self._sell_usd)
        self._buy_usd = 0.0
        self._sell_usd = 0.0
        self.count += 1
        #print(self.count)

    def _calc_relative_strength_index(self):

        if len(self._rsi_prices_list) < 1000:
            return 0.0

        gains = []
        losses = []

        for i in range(1, len(self._rsi_prices_list)):
            delta = self._rsi_prices_list[i] - self._rsi_prices_list[i - 1]
            if delta > 0:
                gains.append(delta)
            else:
                losses.append(-delta)

        avg_gain = sum(gains) / 1000 if gains else 0
        avg_loss = sum(losses) / 1000 if losses else 0

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))


    def _all_full(self):
        return self._buy_long_buf.is_full() and self._sell_long_buf.is_full()
    