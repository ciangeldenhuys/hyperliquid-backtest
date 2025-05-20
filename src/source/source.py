from abc import ABC, abstractmethod
from datetime import datetime

class Source(ABC):
    
    @abstractmethod
    def time(self) -> datetime:
        pass

    @abstractmethod
    async def stream_trades(self):
        pass
    
    @abstractmethod
    def add_trade_handler(self, handler):
        pass
