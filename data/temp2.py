@staticmethod
    def process_pair(pairing: Tuple[str, str], start: datetime, end: datetime):
        coin_pair, type = pairing

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
    def download_binance_to_db(coin_pairs: Optional[List[Tuple[str, str]]] = None, start: datetime = datetime.now() - timedelta(days=1), end: datetime = datetime.now()):
        """
        Does not support options or futures at this point.
        """
        if coin_pairs is None:
            coin_pairs = DatabaseSync.get_existing_pairings()

        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = end.replace(hour=0, minute=0, second=0, microsecond=0)

        max_workers = 4

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(DatabaseSync.process_pair, pairing, start, end) for pairing in coin_pairs]
            for future in futures:
                future.result()  # Ensures that all tasks complete

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