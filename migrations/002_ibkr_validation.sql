SELECT symbol, conid, timeframe, COUNT(*), MIN(timestamp), MAX(timestamp)
FROM ibkr_candles
GROUP BY symbol, conid, timeframe
ORDER BY symbol;
