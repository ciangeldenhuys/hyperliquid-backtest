from collections import deque

class DequeAvgVar:
    def __init__(self, maxlen):
        self._deque = deque(maxlen=maxlen)
        self._sum = 0
        self._sum_sq = 0

    def append(self, val):
        if len(self._deque) == self._deque.maxlen:
            removed = self._deque[0]
            self._sum -= removed
            self._sum_sq -= removed ** 2
        self._deque.append(val)
        self._sum += val
        self._sum_sq += val ** 2

    def average(self):
        return 0 if not self._deque else self._sum / len(self._deque)
    
    def variance(self):
        n = len(self._deque)
        if n == 0:
            return 0
        mean = self._sum / n
        return (self._sum_sq / n) - (mean ** 2)

    def __getitem__(self, index):
        return self._deque[index]

    def __len__(self):
        return len(self._deque)
    
    def is_full(self):
        return len(self._deque) == self._deque.maxlen