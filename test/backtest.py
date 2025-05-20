from datetime import datetime, timedelta
import psycopg
import asyncio
from old.backtest_datastream import BacktestDataStream
import os
from dotenv import load_dotenv
import json

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

READ_SIZE = 100000

class Backtest:
    def __init__(self, start: datetime, end: datetime, coin: str):
        self.start = start
        self.end = end
        self.stream = BacktestDataStream()
        self.coin = coin

    async def run(self):
        step = 0
        while rows := await self.get_rows(step):
            step += 1
            for row in rows:
                if row[2]:
                    await self.stream.queue.put(Backtest.tuple_to_json(brow=row))
                else:
                    await self.stream.queue.put(Backtest.tuple_to_json(srow=row))

    async def get_rows(self, step):
        async with await psycopg.AsyncConnection.connect(CONNECTION_STR) as conn:
            async with conn.cursor() as cur:
                coin_id = Backtest.get_coin_id(self.coin)
                await cur.execute(
                    'SELECT price, quantity, side FROM trades WHERE coin_id = %s AND trade_time < %s ORDER BY trade_time OFFSET %s ROWS FETCH NEXT %s ROWS ONLY;',
                    (coin_id, self.end, READ_SIZE * step, READ_SIZE)
                )
                return await cur.fetchall()

    # async def save_trades(self):
    #     async with await psycopg.AsyncConnection.connect(CONNECTION_STR) as conn:
    #         async with conn.cursor() as cur:
    #             time = self.time
    #             coin_id = Backtest.get_coin_id(self.coin)
    #             await cur.execute(
    #                 'SELECT price, quantity, side FROM trades WHERE trade_time BETWEEN %s AND %s AND coin_id = %s',
    #                 (time, time + timedelta(seconds=1), coin_id)
    #             )
    #             rows = await cur.fetchall()

    #             buy = [0, 0, 'B']
    #             sell = [0, 0, 'A']

    #             for r in rows:
    #                 price, quantity, side = r
    #                 if side:
    #                     buy[0] += price * quantity
    #                     buy[1] += quantity
    #                 else:
    #                     sell[0] += price * quantity
    #                     sell[1] += quantity

    #             buy[0] = (buy[0] / buy[1]) if buy[1] else None
    #             sell[0] = (sell[0] / sell[1]) if sell[1] else None

    #             buy = tuple(buy) if buy[1] else None
    #             sell = tuple(sell) if sell[1] else None

    #             await self.stream.queue.put(Backtest.tuple_to_json(sell, buy))


    @staticmethod
    def tuple_to_json(srow=None, brow=None):
        data = []
        if brow is not None:
            data.append({
                'side': brow[2],
                'px': str(brow[0]),
                'sz': str(brow[1])
            })

        if srow is not None:
            data.append({
                'side': srow[2],
                'px': str(srow[0]),
                'sz': str(srow[1])
            })

        dict_data = {
            'channel': 'trades',
            'data': data
        }

        return json.dumps(dict_data)

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
        with psycopg.connect(CONNECTION_STR) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT coin_id FROM coin_pair WHERE symbol = %s;', (coin_pair,))
                result = cur.fetchone()
                if result:
                    ret = result[0]
                else:
                    cur.execute('INSERT INTO coin_pair(symbol) VALUES (%s) RETURNING coin_id', (coin_pair,))
                    ret = cur.fetchone()[0]
            conn.commit()
        
        return ret