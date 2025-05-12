from statistics import mean, stdev
import asyncio
from volume_collecter import VolumeCollecter
from hyperliquid.exchange import Exchange, Info
from hyperliquid.utils import constants
from dotenv import load_dotenv
import os

load_dotenv()

class VolumeSignal:

    def __init__(self, symbol: str, threshold: float, min_points: int, poll_interval: float, usd_notional: float, Z_SCORE_MAX: int):
        """
        Initialize a VolumeSignal instance.
        """
        self.collector = VolumeCollecter(symbol)
        self.threshold = threshold
        self.min_points = min_points
        self.poll_int = poll_interval
        self.zb = 0.0
        self.zs = 0.0
        self.usd_notional = usd_notional
        self.Z_SCORE_MAX = Z_SCORE_MAX
        self.symbol = symbol

        self.private_key = os.getenv("PRIVATE_KEY")
        self.api_base_url = constants.MAINNET_API_URL

        self.exchange = Exchange("WALLET_ADRESS", self.api_base_url)
    

        self.actions = {
            "buy_momentum": self.execute_buy_momentum,
            "buy_falling": self.execute_buy_falling,
            "sell_dominant": self.execute_sell_dominant
        }

    def _z_score(self, series):
        """
        Returns the Z score of the latest data point in a series.
        """
        if len(series) < 2:
            return 0.0
        μ = mean(series[:-1])
        σ = stdev(series[:-1])
        return (series[-1] - μ) / σ if σ else 0.0

    async def monitor(self):
        """
        Coroutine — run inside an asyncio task.
        """
        print("VolumeSignal monitor started")
        while True:
            await asyncio.sleep(self.poll_int)

            buy_buf = self.collector.buy_volume_buffer
            sell_buf = self.collector.sell_volume_buffer

            if len(buy_buf) < self.min_points or len(sell_buf) < self.min_points:
                continue

            latest_buy = buy_buf[-1]
            latest_sell = sell_buf[-1]
            last_buy = buy_buf[-2]

            self.zb = self._z_score(list(buy_buf))
            self.zs = self._z_score(list(sell_buf))

            # Check conditions and execute actions
            if self.zb >= self.threshold:
                if latest_buy >= latest_sell:
                    if latest_buy >= last_buy:
                        print(f"BUY‑VOLUME, MOMENTUM SIGNAL | z = {self.zb}")
                        self.actions["buy_momentum"]()
                    else:
                        print(f"BUY-VOLUME FALLING AFTER MOMENTUM SIGNAL | z = {self.zb}")
                        self.actions["buy_falling"]()
                else:
                    print(f"SELL DOMINANT DESPITE BUY SIGNAL | z = {self.zb}")
                    self.actions["sell_dominant"]()

    def execute_buy_momentum(self):
        print("Executing BUY MOMENTUM trade...")

        buy_size = (self.usd_notional * (self.zb / self.Z_SCORE_MAX)) / self.collector.last_trade_price
        if buy_size * self.collector.last_trade_price > self.usd_notional:
            buy_size = self.usd_notional / self.collector.last_trade_price

        self.create_buy_order(True, 0.01, buy_size)

    def execute_buy_falling(self):
        print("Executing BUY FALLING trade...")

        sell_size = (2) #fill in logic idk

        self.create_sell_order(0.01, sell_size)

    def execute_sell_dominant(self):
        print("Executing SELL DOMINANT trade...")

        sell_size = (2) #fill in logic idk

        self.create_sell_order(0.01, sell_size)



    def create_buy_order(self, is_buy: bool, allowed_slip : float, buy_size : float):

        order_result = self.exchange.market_open(self.symbol, is_buy, buy_size, None, allowed_slip)
        if order_result["status"] == "ok":
            for status in order_result["response"]["data"]["statuses"]:
                try:
                    filled = status["filled"]
                    print(f'Order #{filled["oid"]} filled {filled["totalSz"]} @{filled["avgPx"]}')
                except KeyError:
                    print(f'Error: {status["error"]}')
   
    def create_sell_order(self, allowed_slip : float, sell_size : float):
        
        order_result = self.exchange.market_close(self.symbol, sell_size, None, allowed_slip)
        if order_result["status"] == "ok":
            for status in order_result["response"]["data"]["statuses"]:
                try:
                    filled = status["filled"]
                    print(f'Order #{filled["oid"]} filled {filled["totalSz"]} @{filled["avgPx"]}')
                except KeyError:
                    print(f'Error: {status["error"]}')

      


if __name__ == "__main__":
    symbol = "@151"
    vs = VolumeSignal(symbol=symbol, threshold=3.0, min_points=10, poll_interval=30)

    async def main():
        collector_task = asyncio.create_task(vs.collector.stream_HL_trades())
        signal_task = asyncio.create_task(vs.monitor())
        await asyncio.gather(collector_task, signal_task)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  stopped by user")
