import pandas as pd
import os
from datetime import datetime, timedelta
import requests
import zipfile

class Correlation:
    @staticmethod
    def setup():
        """
        Sort all data for each coin into one .csv file per coin.
        """
        histdata = 'historical_data\\spot'
        outputdir = os.path.join(histdata, 'totals')
        columns = ['trade_id', 'trade_time', 'price', 'size', 'side']

        for coin in os.listdir(histdata):
            if coin != 'totals':
                coinpath = os.path.join(histdata, coin)
                outputpath = os.path.join(outputdir, f'{coin}-trades.csv')
                pd.DataFrame(columns=columns).to_csv(outputpath, mode='w', header=True, index=False)
                for filename in sorted(os.listdir(coinpath)):
                    filepath = os.path.join(coinpath, filename)
                    if filepath != outputpath:
                        next_day = pd.read_csv(filepath, skiprows=1, header=None)
                        next_day.columns = columns
                        next_day.to_csv(outputpath, mode='a', header=False, index=False)

    @staticmethod
    def compute(coin1, coin2, step, start=None, end=None):
        """
        Compute correlation between two coins.

        Parameters:
            coin1 (str): Name of first coin.

            coin2 (str): Name of second coin.

            step (int): Time step (ms) between datapoints in sum.

            start (int): Time (ms) to start correlation from.

            end (int): Time (ms) to end correlation at.

        Returns:
            correlation: The correlation between the two coins on the given interval.
        """
        datadir = 'historical_data\\spot\\totals'

        for filename in os.listdir(datadir):
            if filename.startswith(coin1):
                data1 = pd.read_csv(os.path.join(filename))
            if filename.startswith(coin2):
                data2 = pd.read_csv(os.path.join(filename))

        

    @staticmethod
    def download_kucoin_data(coin, start=datetime.now() - timedelta(days=1), end=datetime.now(), type=False):
        """
        Exports KuCoin historical data to .csv files in the historical_data folder.

        Parameters:
            coin (str): Name of coin.

            start (datetime): Date to start pulling data from (inclusive).

            end (datetime): Date to stop pulling data from (exclusive).

            type (bool): False is spot, True is futures.
        """
        if type:
            coin = coin + 'M'
        outputdir = f'historical_data\\{'futures' if type else 'spot'}\\{coin}'
        os.makedirs(outputdir, exist_ok=True)
        current = start
        while (current < end):
            url = f'https://historical-data.kucoin.com/data/{'futures' if type else 'spot'}/daily/trades/{coin}/{coin}-trades-{current:%Y-%m-%d}.zip'
            zippath = os.path.join(outputdir, f'{coin}-trades-{current:%Y-%m-%d}.zip')

            response = requests.get(url)
            with open(zippath, 'wb') as f:
                f.write(response.content)

            with zipfile.ZipFile(zippath, 'r') as z:
                z.extractall(outputdir)

            os.remove(zippath)

            current += timedelta(days=1)


# Test download/unzip
# if __name__ == '__main__':
#     Correlation.download_kucoin_data('ETHUSDC', start=datetime.now() - timedelta(days=1), type=True)
#     Correlation.setup()