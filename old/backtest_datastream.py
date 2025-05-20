import asyncio

class BacktestDataStream:
    def __init__(self):
        self.queue = asyncio.Queue()

    async def __aiter__(self):
        while True:
            data = await self.queue.get()
            yield data