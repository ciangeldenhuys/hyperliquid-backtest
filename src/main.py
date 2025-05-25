import source
from strategy.volume_executor import VolumeExecutor
from datetime import datetime

if __name__ == '__main__':
    VolumeExecutor(source.Backtest('XRPUSDT', datetime(2025, 1, 1), datetime(2025, 2, 1), 1000))
    