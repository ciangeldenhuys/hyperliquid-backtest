from statistics import mean, stdev
import asyncio
from volume_collecter import VolumeCollecter


class VolumeSignal:

    def __init__(self, symbol: str, threshold: float, min_points: int, poll_interval: float):
        """
        Initialize a VolumeSignal instance.
        Parameters:
            symbol: string
                The cryptos string

            collector : VolumeCollecter  
                The live data stream instance providing buy/sell volume buffers.

            threshold : float  
                The z-score threshold at which a volume spike signal is triggered.

            min_points : int  
                The minimum number of data points required in the buffer before signals are evaluated.

            poll_interval : float  
                The number of seconds to wait between each signal check (polling frequency).
        """
        self.collector    = VolumeCollecter(symbol)
        self.threshold    = threshold
        self.min_points   = min_points
        self.poll_int     = poll_interval

    def _z_score(self, series):
        """
        Returns the Z score of the latest data point in a series.
        
        Parameters:
            series : list[float]
                Historical volume values. The last element is treated as the 'latest' value.
        """
        if len(series) < 2:
            return 0.0
        μ = mean(series[:-1])
        σ = stdev(series[:-1])
        return (series[-1] - μ) / σ if σ else 0.0

    async def monitor(self):
        """
        Coroutine — run inside an asyncio task.
        Prints BUY / SELL signals in real time.
        """
        print("VolumeSignal monitor started")
        while True:
            await asyncio.sleep(self.poll_int)

            buy_buf  = self.collector.buy_volume_buffer
            sell_buf = self.collector.sell_volume_buffer

            if len(buy_buf) < self.min_points or len(sell_buf) < self.min_points:
                continue

            latest_buy  = buy_buf[-1]
            latest_sell = sell_buf[-1]

            zb = self._z_score(list(buy_buf))
            zs = self._z_score(list(sell_buf))


            if zb >= self.threshold:
                print(f"🟢 BUY‑VOLUME SPIKE  | z = {zb:,.2f}  | last = {latest_buy:,.2f} USDC")
            if zs >= self.threshold:
                print(f"🔴 SELL‑VOLUME SPIKE | z = {zs:,.2f}  | last = {latest_sell:,.2f} USDC")


if __name__ == "__main__":
    symbol = "ETHUSDC"
    vs = VolumeSignal(symbol=symbol, threshold=3.0, min_points=10, poll_interval=30)

    async def main():
        collector_task = asyncio.create_task(vs.collector._stream_binance_trades())
        signal_task    = asyncio.create_task(vs.monitor())
        await asyncio.gather(collector_task, signal_task)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  stopped by user")