import os
import io
import zipfile
from psycopg import AsyncConnection, Connection, connect
from psycopg_pool import AsyncConnectionPool
import pandas as pd
from typing import List, Optional, Tuple, Coroutine
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import asyncio
import aiohttp
from dotenv import load_dotenv
from tqdm.asyncio import tqdm as tqdm_async

class DatabaseSync:

    # --- Database AsyncConnection Utils --- #

    load_dotenv()
    connection_str = f"""
        dbname={os.getenv('DB_NAME')}
        user={os.getenv('DB_USER')}
        password={os.getenv('DB_PASSWORD')}
        host={os.getenv('DB_HOST')}
        port={os.getenv('DB_PORT')}
    """
    pool = AsyncConnectionPool(connection_str, open=False)
    
    asyncio.set_event_loop_policy(
        asyncio.WindowsSelectorEventLoopPolicy()
    )

    @staticmethod
    async def get_async_connection() -> Coroutine[None, None, AsyncConnection]:
        return await DatabaseSync.pool.getconn()

    @staticmethod
    async def release_async_connection(conn: AsyncConnection):
        await DatabaseSync.pool.putconn(conn)

    # --- Database Sync Utils --- #

    @staticmethod
    def get_connection() -> Connection:
        return connect(DatabaseSync.connection_str)
    
    @staticmethod
    def release_connection(conn: Connection):
        conn.close()

    # --- Existing Coins Utils --- #

    @staticmethod
    def _get_coin_id(coin_pair: str) -> int:
        conn = DatabaseSync.get_connection()
        with conn.cursor() as cur:
            cur.execute('SELECT coin_id FROM coin_pair WHERE symbol = %s;', (coin_pair,))
            result = cur.fetchone()
            if result:
                ret = result[0]
            else:
                cur.execute('INSERT INTO coin_pair(symbol) VALUES (%s) RETURNING coin_id', (coin_pair,))
                ret = cur.fetchone()[0]
        conn.commit()
        DatabaseSync.release_connection(conn)
        return ret
    
    @staticmethod
    def _get_existing_pairings() -> list[tuple[str, str]]:
        conn = DatabaseSync.get_connection()
        with conn.cursor() as cur:
            query = """
                SELECT DISTINCT cp.symbol, t.trade_type
                FROM coin_pair cp
                JOIN trades t ON cp.coin_id = t.coin_id;
            """
            cur.execute(query)
            pairings = cur.fetchall()
        conn.commit()
        DatabaseSync.release_connection(conn)
        return pairings
    
    # --- Mass Insert Utils --- #

    @staticmethod
    def _drop_index():
        conn = DatabaseSync.get_connection()
        cur = conn.cursor()
        try:
            cur.execute('DROP INDEX IF EXISTS trades_index;')
            conn.commit()
        except Exception as e:
            print(f'Error dropping index: {e}')
        finally:
            cur.close()
            DatabaseSync.release_connection(conn)

    @staticmethod
    def _recreate_index():
        conn = DatabaseSync.get_connection()
        cur = conn.cursor()
        try:
            cur.execute('CREATE INDEX trades_index ON trades (coin_id, trade_time);')
            conn.commit()
        except Exception as e:
            print(f'Error recreating index: {e}')
        finally:
            cur.close()
            DatabaseSync.release_connection(conn)

    @staticmethod
    def _drop_fk():
        conn = DatabaseSync.get_connection()
        cur = conn.cursor()
        try:
            cur.execute('ALTER TABLE trades DROP CONSTRAINT coin_fk;')
            conn.commit()
        except Exception as e:
            print(f'Error dropping foreign key: {e}')
        finally:
            cur.close()
            DatabaseSync.release_connection(conn)

    @staticmethod
    def _recreate_fk():
        conn = DatabaseSync.get_connection()
        cur = conn.cursor()
        try:
            cur.execute('ALTER TABLE trades ADD CONSTRAINT coin_fk FOREIGN KEY coin_id REFERENCES coin_pair(coin_id);')
            conn.commit()
            print('Foreign key recreated.')
        except Exception as e:
            print(f'Error recreating foreign key: {e}')
        finally:
            cur.close()
            DatabaseSync.release_connection(conn)

    @staticmethod
    async def bulk_insert(rows):
        try:
            conn = await DatabaseSync.get_async_connection()
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
            print(f"final threads: {DatabaseSync.thread_count}, final copied: {DatabaseSync.copy_count}")
        finally:
            await DatabaseSync.release_async_connection(conn)

    @staticmethod
    async def download_and_process(url: str):
        trade_type = url.split('/')[4]
        coin_pair = url.split('/')[-1].split('-')[0]
        coin_id = DatabaseSync._get_coin_id(coin_pair)
        
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

                            tasks.append(asyncio.create_task(DatabaseSync.bulk_insert(rows)))
                            count += 1

                            if count == 32:
                                await asyncio.gather(*tasks)
                                count = 0
                                tasks = []

                        await asyncio.gather(*tasks)

    @staticmethod
    async def download_binance_to_db(coin_pairs, start, end):
        for pairing in coin_pairs:
            coin_pair, trade_type = pairing
            curr = start

            if curr.day != 1:
                stop = (start + relativedelta(months=1)).replace(day=1)
                while curr < stop and curr < end:
                    url = f'https://data.binance.vision/data/{trade_type}/daily/trades/{coin_pair}/{coin_pair}-trades-{curr:%Y-%m-%d}.zip'
                    await DatabaseSync.download_and_process(url)
                    curr += timedelta(days=1)

            stop = end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            while curr < stop:
                url = f'https://data.binance.vision/data/{trade_type}/monthly/trades/{coin_pair}/{coin_pair}-trades-{curr:%Y-%m}.zip'
                await DatabaseSync.download_and_process(url)
                curr += relativedelta(months=1)

            while curr < end:
                url = f'https://data.binance.vision/data/{trade_type}/daily/trades/{coin_pair}/{coin_pair}-trades-{curr:%Y-%m-%d}.zip'
                await DatabaseSync.download_and_process(url)
                curr += timedelta(days=1)

    @staticmethod
    async def async_calls(coin_pairs: list[tuple[str, str]], start: datetime, end: datetime, drop_index: bool):
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = end.replace(hour=0, minute=0, second=0, microsecond=0)
        if coin_pairs == None:
            coin_pairs = DatabaseSync._get_existing_pairings()
        if drop_index:
            DatabaseSync._drop_index()

        await DatabaseSync.pool.open()
        await DatabaseSync.download_binance_to_db(coin_pairs, start, end)
        await DatabaseSync.pool.close()

        print('finished')

        if drop_index:
            DatabaseSync._recreate_index()

    @staticmethod
    def start_binance_download(
        coin_pairs: Optional[List[Tuple[str, str]]] = None,
        start: datetime = datetime.now() - timedelta(days=1),
        end: datetime = datetime.now(),
        drop_index: bool = False        
    ):
        asyncio.run(DatabaseSync.async_calls(coin_pairs, start, end, drop_index))

if __name__ == '__main__':
    download_list = [
        ('XRPUSDT', 'spot'),
        ('BNBUSDT', 'spot'),
        ('TRXUSDT', 'spot'),
        ('ADAUSDT', 'spot'),
        ('SUIUSDT', 'spot')
    ]
    DatabaseSync.start_binance_download(download_list)