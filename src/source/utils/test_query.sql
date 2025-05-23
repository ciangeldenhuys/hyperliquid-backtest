SELECT price, quantity, side, trade_time
FROM trades
WHERE coin_id = 1
AND trade_time < '2025-02-01 00:00:00'
ORDER BY trade_time
OFFSET 0 ROWS
FETCH NEXT 100000 ROWS ONLY;