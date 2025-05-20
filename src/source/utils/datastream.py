import asyncio

class DataStream:
    def __init__(self):
        self.queue = asyncio.Queue()

    def __aiter__(self):
        return self
    
    async def __anext__(self):
        return await self.queue.get()