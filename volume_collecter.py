import os
from dotenv import load_dotenv
from kucoin.client import Client
from datetime import datetime
import asyncio
import json
import websockets
import csv
from zoneinfo import ZoneInfo  # requires Python 3.9+
from collections import deque
from collections import deque


# Load variables from .env file
load_dotenv()

# Access credentialshttps://kucoin.onelink.me/iqEP/xy0tdqd1
API_KEY = os.getenv("KUCOIN_API_KEY")
API_SECRET = os.getenv("KUCOIN_API_SECRET")
API_PASSPHRASE = os.getenv("KUCOIN_API_PASSPHRASE")
API_KEY_VERSION = os.getenv("KUCOIN_API_KEY_VERSION")

client = Client(API_KEY, API_SECRET, API_PASSPHRASE)

class VolumeCollecter:

    def __init__(self, symbol: str):
        self.symbol = symbol.lower()
        self.buy_volume_buffer = deque(maxlen=2880)
        self.sell_volume_buffer = deque(maxlen=2880)

    
    async def _stream_binance_trades(self):#, outfile: str):
        """
        Stream <symbol>@trade from Binance, aggregate per‑minute BUY / SELL USD
        and append to `outfile` continuously.
        Use Ctrl‑C to stop.
        """

        wss    = f"wss://stream.binance.com:9443/ws/{self.symbol}@trade"

        current_bucket = None
        buy_usd = sell_usd = 0.0

        def flush():
            nonlocal current_bucket, buy_usd, sell_usd
            if current_bucket is None:
                return
            row = [current_bucket.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"),
                   round(buy_usd, 8), round(sell_usd, 8)]
            
            print("Buffer size:", len(self.buy_volume_buffer))
            print("Buy buffer:", list(self.buy_volume_buffer)[-5:])  # last 5 entries
            print("Sell buffer:", list(self.sell_volume_buffer)[-5:])  # last 5 entries
            # append to CSV
            # write_header = not os.path.exists(outfile)
            # with open(outfile, "a", newline="") as f:
            #     w = csv.writer(f)
            #     if write_header:
            #         w.writerow(["time", "buy_usd", "sell_usd"])
            #     w.writerow(row)

            # Update in-memory buffers
            self.buy_volume_buffer.append(row[1])
            self.sell_volume_buffer.append(row[2])

            # Reset counters
            buy_usd = sell_usd = 0.0
            current_bucket = None

        async with websockets.connect(wss, ping_interval=20, ping_timeout=10) as ws:
            print("connected to", wss)
            async for m in ws:
                msg = json.loads(m)
                price = float(msg["p"])
                size  = float(msg["q"])
                usd   = price * size
                t = datetime.fromtimestamp(msg["T"]/1000, tz=ZoneInfo("America/Los_Angeles"))
                bucket = t.replace(second=(0 if t.second < 30 else 30), microsecond=0)


                if current_bucket is None:
                    current_bucket = bucket
                elif bucket > current_bucket: 
                    flush()
                    current_bucket = bucket

                # side: msg["m"] True taker SELL, False taker BUY (I think)
                if msg["m"]:
                    sell_usd += usd
                else:
                    buy_usd  += usd
    
    
    def binance_live(self):#, outfile: str)
        """
        Blocking call: runs the asyncio event loop, prints and appends
        one CSV row per closed minute bucket.
        """
        # print("Saving to:", os.path.abspath(outfile))
        async def runner():                       # nested so we can flush
            try:
                await self._stream_binance_trades()
            finally:
                # make sure the last minute is flushed on Ctrl‑C
                pass                               # flush happens inside _stream

        try:
            asyncio.run(runner())
        except KeyboardInterrupt:
            print("\n stopped by user")


if __name__ == '__main__':
    symbol1 = "ETHUSDC"

    print(f"    Starting live Binance stream for {symbol1} …")
    print("    Console     : prints one line every 30 seconds\n")

    try:
        vc = VolumeCollecter(symbol1)
        vc.binance_live()
    except Exception as e:
        print("❌ stream terminated:", e)


