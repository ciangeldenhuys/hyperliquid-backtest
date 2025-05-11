import datetime
from datetime import timedelta
import requests
import os
from dotenv import load_dotenv
import zipfile
import hashlib
import pandas as pd
import psycopg2
from psycopg2.extensions import cursor
from psycopg2.extras import execute_values


class CSVDownload:
    @staticmethod
    def download_binance_data(coin_pair: str, start=datetime.now() - timedelta(days=1), end=datetime.now(), type=False):
        """
        Exports Binance daily data to .csv files in the historical_data folder.

        Parameters:
            coin_pair (str): Name of coin_pair.

            start (datetime): Date to start pulling data from (inclusive).

            end (datetime): Date to stop pulling data from (exclusive).

            type (bool): False is spot, True is futures.
        """
        if type:
            coin_pair = coin_pair + 'M'
        outputdir = f'historical_data\\{'futures' if type else 'spot'}\\{coin_pair}'
        os.makedirs(outputdir, exist_ok=True)
        current = start
        while (current < end):
            url = f'https://data.binance.vision/data/{'futures' if type else 'spot'}/daily/trades/{coin_pair}/{coin_pair}-trades-{current:%Y-%m-%d}.zip'
            zippath = os.path.join(outputdir, f'{coin_pair}-trades-{current:%Y-%m-%d}.zip')

            response = requests.get(url)
            with open(zippath, 'wb') as f:
                f.write(response.content)

            response = requests.get(url + '.CHECKSUM')
            with open(zippath + '.CHECKSUM', 'wb') as cs:
                cs.write(response.content)

            if not CSVDownload._verify_checksum(zippath):
                print(f'Skipped {current:%Y-%m-%d} due to corrupted zip file.')
                continue
            else:
                with zipfile.ZipFile(zippath, 'r') as z:
                    z.extractall(outputdir)

            os.remove(zippath)
            os.remove(zippath + '.CHECKSUM')

            current += timedelta(days=1)

    @staticmethod
    def gather_totals():
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
    def _verify_checksum(zippath: str):
        with open(zippath + '.CHECKSUM', 'r') as cs:
            expected = cs.read().strip().split()[0]

        sha256 = hashlib.sha256()
        with open(zippath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        actual = sha256.hexdigest()

        return actual == expected

    @staticmethod
    def sync_db():
        """
        Syncs all data files currently in the historical_data directory to the database.
        Deletes all .csv files after excecution.
        """
        load_dotenv()
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        cur = conn.cursor()

        histdata = 'historical_data\\spot'
        for coin_pair in os.listdir(histdata):
            if coin_pair != 'totals':
                coinpath = os.path.join(histdata, coin_pair)
                for filename in os.listdir(coinpath):
                    filepath = os.path.join(coinpath, filename)
                    CSVDownload.sync_csv_to_db(coin_pair, 'spot', filepath, cur)
                    conn.commit()
                    os.remove(filepath)

        cur.close()
        conn.close()

    @staticmethod
    def sync_csv_to_db(coin_pair: str, type: str, filepath: str, cur: cursor):
        coin_id = CSVDownload._get_coin_id(coin_pair)

        df = pd.read_csv(filepath, header=None)
        df.columns = ['trade_id', 'price', 'quantity', 'quoteqty', 'timestamp', 'is_buyer_maker', 'best_match']
        df['trade_time'] = pd.to_datetime(df['timestamp'], unit='us')
        df['side'] = ~df['is_buyer_maker']

        rows = df[['trade_id', 'trade_time', 'price', 'quantity', 'side', 'best_match']].values.tolist()
        rows = [(r[0], coin_id, r[1], r[2], r[3], r[4], r[5], type) for r in rows]

        insert_query = """
        INSERT INTO trades (
            trade_id, coin_id, trade_time, price, quantity, side, best_match, trade_type
        ) VALUES %s
        ON CONFLICT (trade_id) DO NOTHING;
        """

        execute_values(cur, insert_query, rows)