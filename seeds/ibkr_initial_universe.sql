-- Initial unresolved IBKR test universe.
-- Resolve with: IBKR_ENABLED=true python -m signalmaker.jobs.ibkr_resolve_contracts --symbols AAPL,MSFT,SPY,QQQ,AIR,TTE,MC,BNP
-- European/Euronext symbols are intentionally inserted unresolved; validate ambiguous=true rows manually after IBKR resolution.

INSERT INTO ibkr_contracts (symbol, sec_type, exchange, currency, resolved, ambiguous, active)
VALUES
    ('AAPL', 'STK', 'SMART', 'USD', false, false, true),
    ('MSFT', 'STK', 'SMART', 'USD', false, false, true),
    ('SPY', 'STK', 'SMART', 'USD', false, false, true),
    ('QQQ', 'STK', 'SMART', 'USD', false, false, true),
    ('AIR', 'STK', 'SMART', 'EUR', false, false, true),
    ('TTE', 'STK', 'SMART', 'EUR', false, false, true),
    ('MC', 'STK', 'SMART', 'EUR', false, false, true),
    ('BNP', 'STK', 'SMART', 'EUR', false, false, true)
ON CONFLICT (symbol, sec_type, exchange, currency, COALESCE(primary_exchange, ''))
DO NOTHING;
