from datetime import datetime
import asyncio
import json
import websockets
from zoneinfo import ZoneInfo
from collections import deque


class VolumeCollecter:

    def __init__(self, coin: str):
        self.coin = coin
        self.buy_volume_buffer = deque(maxlen=2880)
        self.sell_volume_buffer = deque(maxlen=2880)
        self.buy_usd = 0.0
        self.sell_usd = 0.0
        self.last_clear = -1

    
    async def stream_HL_trades(self):

        HL_WSS = "wss://api.hyperliquid.xyz/ws" 

        async with websockets.connect(HL_WSS, ping_interval=1, ping_timeout=10) as ws:
            print("connected to", HL_WSS)

            # Send subscription message
            subscription_message = {
                "method": "subscribe",
                "subscription": {
                    "type": "trades",
                    "coin": self.coin
                }
            }
            await ws.send(json.dumps(subscription_message))
            print(f"Subscribed to trades for index: {self.coin}")

            async for m in ws:
                msg = json.loads(m)
        
                if msg.get("channel") == "subscriptionResponse":
                    print("Subscription confirmed:", msg)
                    continue
                    
                if msg.get("channel") == "trades":
                    for slot in msg["data"]:
                

                        price = float(slot["px"])
                        size  = float(slot["sz"])
                        usd   = price * size


                        if slot["side"] == "A":
                            self.sell_usd += usd
                        if slot["side"] == "B":
                            self.buy_usd += usd

    async def flush(self):
        tz = ZoneInfo("America/Los_Angeles")
        
        while True:
            current_seconds = datetime.now(tz).second
            if (current_seconds == 30 or current_seconds == 0) and current_seconds != self.last_clear:
                self.buy_volume_buffer.append(self.buy_usd)
                self.sell_volume_buffer.append(self.sell_usd)
                print("Time:", datetime.now(tz))
                print("Buffer size:", len(self.buy_volume_buffer))
                print("Buy buffer:", [f'{x:.2f}' for x in self.buy_volume_buffer])
                print("Sell buffer:", [f'{x:.2f}' for x in self.sell_volume_buffer])
                self.buy_usd = 0.0
                self.sell_usd = 0.0
                self.last_clear = current_seconds
                
            await asyncio.sleep(0.1)
    
    async def run(self):
        await asyncio.gather(self.stream_HL_trades(), self.flush())
    
    
if __name__ == "__main__":
    vc = VolumeCollecter("@151")       # ETH/USDC spot asset
    asyncio.run(vc.run())


