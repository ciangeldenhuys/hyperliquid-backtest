import os
import io
import zipfile
from psycopg import AsyncConnection, Connection, connect
from psycopg_pool import AsyncConnectionPool
import pandas as pd
from typing import List, Optional, Tuple
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import asyncio
import aiohttp
from dotenv import load_dotenv

load_dotenv()
CONNECTION_STR = f"""
    dbname={os.getenv('DB_NAME')}
    user={os.getenv('DB_USER')}
    password={os.getenv('DB_PASSWORD')}
    host={os.getenv('DB_HOST')}
    port={os.getenv('DB_PORT')}
"""
asyncio.set_event_loop_policy(
    asyncio.WindowsSelectorEventLoopPolicy()
)

class DatabaseSync:
    """
    Sync data from Binance to the Postgres database. Provide DB credentials in .env file.
    Does not currently support data sync for futures or options.
    """

    # --- Async Connection Pool --- #

    pool = AsyncConnectionPool(CONNECTION_STR, open=False)

    # --- Database AsyncConnection Utils --- #

    @staticmethod
    async def _get_async_connection() -> AsyncConnection:
        """
        Gets an async connection from the async connection pool.

        Returns:
            conn (AsyncConnection): An asynchronous connection from the pool.
        """
        return await DatabaseSync.pool.getconn()

    @staticmethod
    async def _release_async_connection(conn: AsyncConnection):
        """
        Returns the given async connection to the pool.

        Parameters:
            conn (AsyncConnection): Connection to return to the pool.
        """
        await DatabaseSync.pool.putconn(conn)

    # --- Database Sync Utils --- #

    @staticmethod
    def _get_connection() -> Connection:
        """
        Gets a regular connection to the database, not from a pool.

        Returns:
            conn (Connection): A synchronous connection to the database.
        """
        return connect(CONNECTION_STR)
    
    @staticmethod
    def _release_connection(conn: Connection):
        """
        Closes the given connection.

        Parameters:
            conn (Connection): Database connection to close.
        """
        conn.close()

    # --- Existing Coins Utils --- #

    @staticmethod
    def get_coin_id(coin_pair: str) -> int:
        """        
        Gets the coin_id for the given coin pairing from the database.

        Inserts the coin pairing if it is not already in the database and creates a coin_id.

        Parameters:
            coin_pair (str): Coin pair to return the ID for.

        Returns:
            coin_id (int): ID of the given coin in the database.
        """
        conn = DatabaseSync._get_connection()
        with conn.cursor() as cur:
            cur.execute('SELECT coin_id FROM coin_pair WHERE symbol = %s;', (coin_pair,))
            result = cur.fetchone()
            if result:
                ret = result[0]
            else:
                cur.execute('INSERT INTO coin_pair(symbol) VALUES (%s) RETURNING coin_id', (coin_pair,))
                ret = cur.fetchone()[0]
        conn.commit()
        DatabaseSync._release_connection(conn)
        return ret
    
    @staticmethod
    def _get_existing_pairings() -> list[tuple[str, str]]:
        """
        Gets all the existing (coin_pair, trade_type) tuples in the database.

        Returns:
            existing_pairings (list[tuple[str, str]]): A list of (coin_pair, trade_type) tuples already in the database.
        """
        conn = DatabaseSync._get_connection()
        with conn.cursor() as cur:
            query = """
                SELECT DISTINCT cp.symbol, t.trade_type
                FROM coin_pair cp
                JOIN trades t ON cp.coin_id = t.coin_id;
            """
            cur.execute(query)
            pairings = cur.fetchall()
        conn.commit()
        DatabaseSync._release_connection(conn)
        return pairings
    
    # --- Mass Insert Utils --- #

    @staticmethod
    def _drop_index():
        """
        Removes the index from the database for quicker inserts.
        """
        conn = DatabaseSync._get_connection()
        cur = conn.cursor()
        try:
            cur.execute('DROP INDEX IF EXISTS trades_index;')
            conn.commit()
        except Exception as e:
            print(f'Error dropping index: {e}')
        finally:
            cur.close()
            DatabaseSync._release_connection(conn)

    @staticmethod
    def _recreate_index():
        """
        Reinstates the index after inserting data.
        """
        conn = DatabaseSync._get_connection()
        cur = conn.cursor()
        try:
            cur.execute('CREATE INDEX trades_index ON trades (coin_id, trade_type, trade_time);')
            conn.commit()
        except Exception as e:
            print(f'Error recreating index: {e}')
        finally:
            cur.close()
            DatabaseSync._release_connection(conn)

    @staticmethod
    def _drop_fk():
        """
        Drops foreign key constraint for faster insert.
        """
        conn = DatabaseSync._get_connection()
        cur = conn.cursor()
        try:
            cur.execute('ALTER TABLE trades DROP CONSTRAINT coin_fk;')
            conn.commit()
        except Exception as e:
            print(f'Error dropping foreign key: {e}')
        finally:
            cur.close()
            DatabaseSync._release_connection(conn)

    @staticmethod
    def _recreate_fk():
        """
        Reinstates foreign key constraint after large insert.
        """
        conn = DatabaseSync._get_connection()
        cur = conn.cursor()
        try:
            cur.execute('ALTER TABLE trades ADD CONSTRAINT coin_fk FOREIGN KEY coin_id REFERENCES coin_pair(coin_id);')
            conn.commit()
            print('Foreign key recreated.')
        except Exception as e:
            print(f'Error recreating foreign key: {e}')
        finally:
            cur.close()
            DatabaseSync._release_connection(conn)
    
    # --- Database Insert Implementation --- #

    @staticmethod
    async def _bulk_insert(rows: list[tuple[int, int, datetime, float, float, bool, bool, str]]):
        """
        Inserts all the provided rows into the database.

        This method uses COPY, so does not check for primary key constraints. Must ensure no duplicate inserts before use.

        Parameters:
            rows (list[tuple[int, int, datetime, float, float, bool, bool, str]]): List of rows to insert to the trades table.
        """
        try:
            conn = await DatabaseSync._get_async_connection()
            async with conn.cursor() as cur:
                copy_query = """
                    COPY trades (trade_id, coin_id, trade_time, price, quantity, side, best_match, trade_type)
                    FROM STDIN WITH CSV;
                """
                async with cur.copy(copy_query) as copy:
                    with io.StringIO() as buffer:
                        for row in rows:
                            buffer.write(','.join(map(str, row)) + '\n')
                        buffer.seek(0)
                        while data := buffer.read(8192):
                            await copy.write(data)
            await conn.commit()
        except Exception as e:
            print(f"Error in bulk_insert: {e}")
        finally:
            await DatabaseSync._release_async_connection(conn)

    @staticmethod
    async def _download_and_process(url: str):
        """
        Download data from the given url and copy it to the database concurrently in chunks.

        Parameters:
            url (str): The url to download a zipfile containing trade data from.
        """
        trade_type = url.split('/')[4]
        coin_pair = url.split('/')[-1].split('-')[0]
        date_str = '-'.join(url.split('/')[-1].split('-')[2:])[:-4]
        coin_id = DatabaseSync.get_coin_id(coin_pair)
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    print(f"Failed to download {url}")
                    return

                downloaded_data = io.BytesIO(await response.read())
                downloaded_data.seek(0)

                with zipfile.ZipFile(downloaded_data) as z:
                    file_name = z.namelist()[0]

                    with z.open(file_name) as csv_file:
                        chunk_iterator = pd.read_csv(csv_file, header=None, chunksize=50000)
                        tasks = []
                        count = 0
                        for chunk_df in chunk_iterator:
                            chunk_df.columns = ['trade_id', 'price', 'quantity', 'quoteqty', 'timestamp', 'is_buyer_maker', 'best_match']
                            chunk_df['trade_time'] = pd.to_datetime(chunk_df['timestamp'], unit='us')
                            chunk_df['side'] = ~chunk_df['is_buyer_maker']

                            rows = chunk_df[['trade_id', 'trade_time', 'price', 'quantity', 'side', 'best_match']].values.tolist()
                            rows = [(r[0], coin_id, r[1], r[2], r[3], r[4], r[5], trade_type) for r in rows]

                            tasks.append(asyncio.create_task(DatabaseSync._bulk_insert(rows)))
                            count += 1

                            if count == 16:
                                await asyncio.gather(*tasks)
                                print(f'16 chunks of {coin_pair} inserted on {date_str}.')
                                count = 0
                                tasks = []

                        await asyncio.gather(*tasks)
                        print(f'Finished inserting {coin_pair} on {date_str}.')
    @staticmethod
    async def _download_binance_to_db(coin_pairs: list[tuple[str, str]], start: datetime, end: datetime):
        """
        Loops all dates between start (inc.) and end (excl.) and downloads the data to the database.

        Parameters:
            coin_pairs (list[tuple[str, str]]): List of (coin_pair, trade_type) tuples to download.

            start (datetime): Date (inc.) to start download from.

            end (datetime): Date (excl.) to end download from.
        """
        for pairing in coin_pairs:
            coin_pair, trade_type = pairing
            curr = start

            if curr.day != 1:
                stop = (start + relativedelta(months=1)).replace(day=1)
                while curr < stop and curr < end:
                    url = f'https://data.binance.vision/data/{trade_type}/daily/trades/{coin_pair}/{coin_pair}-trades-{curr:%Y-%m-%d}.zip'
                    await DatabaseSync._download_and_process(url)
                    curr += timedelta(days=1)

            stop = end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            while curr < stop:
                url = f'https://data.binance.vision/data/{trade_type}/monthly/trades/{coin_pair}/{coin_pair}-trades-{curr:%Y-%m}.zip'
                await DatabaseSync._download_and_process(url)
                curr += relativedelta(months=1)

            while curr < end:
                url = f'https://data.binance.vision/data/{trade_type}/daily/trades/{coin_pair}/{coin_pair}-trades-{curr:%Y-%m-%d}.zip'
                await DatabaseSync._download_and_process(url)
                curr += timedelta(days=1)

    @staticmethod
    async def _async_calls(coin_pairs: list[tuple[str, str]], start: datetime, end: datetime, drop_index: bool):
        """
        Makes all async function calls sequentially.
        
        Preps database for download before downloading anything by removing index/FK if specified.

        FK dropping not currently implemented.

        Parameters:
            coin_pairs (list[tuple[str, str]]): List of (coin_pair, trade_type) tuples to download.

            start (datetime): Date (inc.) to start download from.

            end (datetime): Date (excl.) to end download from.

            drop_index (bool): Whether index should be dropped before download.
        """
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = end.replace(hour=0, minute=0, second=0, microsecond=0)
        if coin_pairs == None:
            coin_pairs = DatabaseSync._get_existing_pairings()
        if drop_index:
            DatabaseSync._drop_index()
            print('Removed index')

        await DatabaseSync.pool.open()
        await DatabaseSync._download_binance_to_db(coin_pairs, start, end)
        await DatabaseSync.pool.close()

        print('Successfully inserted all data.')

        if drop_index:
            DatabaseSync._recreate_index()
            print('Recreated index.')

    @staticmethod
    def start_binance_download(
        coin_pairs: Optional[List[Tuple[str, str]]] = None,
        start: datetime = datetime.now() - timedelta(days=1),
        end: datetime = datetime.now(),
        drop_index: bool = False        
    ):
        """
        Starts concurrent download of Binance data to Postgres DB.

        Parameters:
            coin_pairs (list[tuple[str, str]]): List of (coin_pair, trade_type) tuples to download. Defaults to the list of all existing tuples in the database.

            start (datetime): Date (inc.) to start download from. Defaults to yesterday.

            end (datetime): Date (excl.) to end download from. Defaults to today.

            drop_index (bool): Whether index should be dropped before download. Defaults to not dropping index.
        """
        asyncio.run(DatabaseSync._async_calls(coin_pairs, start, end, drop_index))

if __name__ == '__main__':
    download_list = [
        ('XRPUSDT', 'spot'),
        ('BNBUSDT', 'spot'),
        ('TRXUSDT', 'spot'),
        ('ADAUSDT', 'spot'),
        ('SUIUSDT', 'spot')
    ]
    DatabaseSync.start_binance_download(download_list, datetime(2025, 1, 1), drop_index=True)