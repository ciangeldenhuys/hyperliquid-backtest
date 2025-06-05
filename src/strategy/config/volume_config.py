__all__ = ['THRESHOLD', 'Z_SCORE_MAX', 'FLUSH', 'SHORT_BUF', 'LONG_BUF']

THRESHOLD = 1.75
THRESHOLD_S = 1.75
Z_SCORE_MAX = 6

FLUSH = 5 * (60) # seconds
SHORT_BUF = 1 * (60 * 60) # seconds
LONG_BUF = 5 * (24 * 60 * 60) # seconds

GRAPH_STEP = 6 * (60 * 60) # seconds

THRESHOLD_RSI_B = 70
THRESHOLD_RSI_S = 40