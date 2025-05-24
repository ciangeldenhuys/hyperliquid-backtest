import source
from strategy.volume_executor import VolumeExecutor
from datetime import datetime
import asyncio

asyncio.set_event_loop_policy(
    asyncio.WindowsSelectorEventLoopPolicy()
)

if __name__ == "__main__":
    asyncio.run(VolumeExecutor(source.Backtest('XRPUSDT', datetime(2025, 1, 1), datetime(2025, 2, 1), 10000)).start())
    