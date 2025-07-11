from abc import ABC, abstractmethod
from datetime import datetime

class Source(ABC):

    @abstractmethod
    def time(self) -> datetime:
        pass

    @abstractmethod
    def stream_trades(self):
        pass
    
    @abstractmethod
    def add_trade_handler(self, handler):
        pass

    @abstractmethod
    def create_buy_order(self, buy_size, allowed_slip):
        pass

    @abstractmethod
    def create_sell_order(self, sell_size, allowed_slip):
        pass

    @abstractmethod
    def market_price(self):
        pass

    @abstractmethod
    def position_size(self):
        pass

    @abstractmethod
    def withdrawable(self):
        pass
    
    @abstractmethod
    def last_sell_price(self):
        pass

    @abstractmethod
    def last_buy_price(self):
        pass   

    @abstractmethod
    def current_total_usd(self):
        pass