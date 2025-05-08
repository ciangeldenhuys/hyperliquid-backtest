import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
import zipfile
import hashlib
import psycopg2
from psycopg2.extensions import cursor
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import pandas as pd
import io
from typing import Optional, List, Tuple
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

class DatabaseSync:
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

            if not DatabaseSync._verify_checksum(zippath):
                print(f'Skipped {current:%Y-%m-%d} due to corrupted zip file.')
                continue
            else:
                with zipfile.ZipFile(zippath, 'r') as z:
                    z.extractall(outputdir)

            os.remove(zippath)
            os.remove(zippath + '.CHECKSUM')

            current += timedelta(days=1)

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
                    DatabaseSync.sync_csv_to_db(coin_pair, 'spot', filepath, cur)
                    conn.commit()
                    os.remove(filepath)

        cur.close()
        conn.close()

    @staticmethod
    def sync_csv_to_db(coin_pair: str, type: str, filepath: str, cur: cursor):
        coin_id = DatabaseSync._get_coin_id(coin_pair)

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

    @staticmethod
    def _get_coin_id(coin_pair: str) -> int:
        load_dotenv()
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        cur = conn.cursor()

        cur.execute('SELECT coin_id FROM coin_pair WHERE symbol = %s;', (coin_pair,))
        result = cur.fetchone()
        if result:
            ret = result[0]
        else:
            cur.execute('INSERT INTO coin_pair(symbol) VALUES (%s) RETURNING coin_id', (coin_pair,))
            ret = cur.fetchone()[0]

        conn.commit()
        cur.close()
        conn.close()

        return ret
    
    @staticmethod
    def download_binance_to_db(coin_pairs: Optional[List[Tuple[str, str]]]=None, start=datetime.now() - timedelta(days=1), end=datetime.now()):
        """
        Does not support options or futures at this point.
        """
        if coin_pairs is None:
            coin_pairs = DatabaseSync.get_existing_pairings()
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = end.replace(hour=0, minute=0, second=0, microsecond=0)
        for pairing in coin_pairs:
            coin_pair = pairing[0]
            type = pairing[1]

            curr = start

            if curr.day != 1:
                stop = (start + relativedelta(months=1)).replace(day=1)
                while curr < stop and curr < end:
                    url = f'https://data.binance.vision/data/{type}/daily/trades/{coin_pair}/{coin_pair}-trades-{curr:%Y-%m-%d}.zip'
                    DatabaseSync.url_to_db(url)
                    curr += timedelta(days=1)

            stop = end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            while curr < stop:
                url = f'https://data.binance.vision/data/{type}/monthly/trades/{coin_pair}/{coin_pair}-trades-{curr:%Y-%m}.zip'
                DatabaseSync.url_to_db(url)
                curr += relativedelta(months=1)

            while curr < end:
                url = f'https://data.binance.vision/data/{type}/daily/trades/{coin_pair}/{coin_pair}-trades-{curr:%Y-%m-%d}.zip'
                DatabaseSync.url_to_db(url)
                curr += timedelta(days=1)

    @staticmethod
    def url_to_db(url: str):
        load_dotenv()

        coin_pair = url.split('/')[-1].split('-')[0]
        trade_type = url.split('/')[4]
        date_str = '-'.join(url.split('/')[-1].split('-')[2:])[:-4]
        coin_id = DatabaseSync._get_coin_id(coin_pair)

        # Settings
        chunk_size = 5000
        buffer_limit = 50000  # Adjusted to 20,000 to increase task frequency
        max_workers = 16  # Increased to 16 to better utilize CPU

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            downloaded_data = io.BytesIO()

            # Download with progress bar
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=f'Downloading ({coin_pair}, {trade_type}) on {date_str}') as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    downloaded_data.write(chunk)
                    pbar.update(len(chunk))

            downloaded_data.seek(0)

            # Extract and process the CSV
            with zipfile.ZipFile(downloaded_data) as z:
                file_name = z.namelist()[0]

                with z.open(file_name) as csv_file:
                    chunk_iterator = pd.read_csv(csv_file, header=None, chunksize=chunk_size)
                    buffer = []

                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        with tqdm(total=None, desc=f'Inserting ({coin_pair}, {trade_type}) on {date_str}', unit=' chunks') as pbar:
                            for chunk_df in chunk_iterator:
                                chunk_df.columns = ['trade_id', 'price', 'quantity', 'quoteqty', 'timestamp', 'is_buyer_maker', 'best_match']
                                chunk_df['trade_time'] = pd.to_datetime(chunk_df['timestamp'], unit='us')
                                chunk_df['side'] = ~chunk_df['is_buyer_maker']

                                rows = chunk_df[['trade_id', 'trade_time', 'price', 'quantity', 'side', 'best_match']].values.tolist()
                                rows = [(r[0], coin_id, r[1], r[2], r[3], r[4], r[5], trade_type) for r in rows]

                                buffer.extend(rows)

                                # Process buffer when the limit is reached
                                if len(buffer) >= buffer_limit:
                                    # Submit each buffer as a separate task
                                    executor.submit(DatabaseSync.bulk_insert, buffer)
                                    buffer = []  # Reset buffer

                                pbar.update(1)

                            # Process any remaining buffer
                            if buffer:
                                executor.submit(DatabaseSync.bulk_insert, buffer)

            print(f'Successfully committed ({coin_pair}, {trade_type}) on {date_str}.\n')

        except Exception as e:
            print(f'Error processing {url}: {e}\n')

    @staticmethod
    def bulk_insert(rows):
        """
        Each bulk_insert call opens its own database connection.
        """
        load_dotenv()
        try:
            conn = psycopg2.connect(
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT')
            )
            with conn.cursor() as cur:
                with io.StringIO() as buffer:
                    for row in rows:
                        buffer.write(','.join(map(str, row)) + '\n')
                    buffer.seek(0)
                    cur.copy_expert("""
                        COPY trades (trade_id, coin_id, trade_time, price, quantity, side, best_match, trade_type)
                        FROM STDIN WITH CSV;
                    """, buffer)

            conn.commit()

        except Exception as e:
            print(f"Error in bulk_insert: {e}")

        finally:
            if 'conn' in locals():
                conn.close()


    @staticmethod
    def get_existing_pairings() -> list[tuple[str, str]]:
        load_dotenv()
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        cur = conn.cursor()

        query = """
            SELECT DISTINCT cp.symbol, t.trade_type
            FROM coin_pair cp
            JOIN trades t ON cp.coin_id = t.coin_id;
        """
        cur.execute(query)
        pairings = cur.fetchall()

        cur.close()
        conn.close

        return pairings

if __name__ == '__main__':
    download_list = [
        ('XRPUSDT', 'spot'),
        ('BNBUSDT', 'spot'),
        ('TRXUSDT', 'spot'),
        ('ADAUSDT', 'spot'),
        ('SUIUSDT', 'spot'),
    ]
    DatabaseSync.download_binance_to_db(download_list, datetime(2025, 2, 1), datetime.now())