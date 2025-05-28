import source
from strategy.volume_executor import VolumeExecutor
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

if __name__ == '__main__':
    ex = VolumeExecutor(source.Backtest('XRPUSDT', datetime(2025, 1, 1), datetime(2025, 2, 1), 1000), 1000)
    try:
        ex.start()
    finally:
        print('Selling remaining positions')
        ex.sell_full_position()
        if ex.graph:
            print('Plotting results')
            fig, ax = plt.subplots()
            ax.plot(ex.times, ex.price_values, label='Price')
            ax.plot(ex.times, ex.balance_values, label='Balance')

            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            ax.xaxis.set_major_locator(mdates.AutoDateLocator())

            plt.xlabel('Time')
            plt.ylabel('%')
            plt.legend(loc='upper left')
            plt.gcf().autofmt_xdate()
            plt.tight_layout()
            plt.show()