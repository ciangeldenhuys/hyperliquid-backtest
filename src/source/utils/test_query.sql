SELECT price, quantity, side, trade_time, trade_id
FROM trades
WHERE coin_id = 1
AND (trade_time, trade_id) > ('2025-01-01 00:00:00', -1)
AND trade_time < '2025-02-01 00:00:00'
ORDER BY trade_time, trade_id
LIMIT 1;