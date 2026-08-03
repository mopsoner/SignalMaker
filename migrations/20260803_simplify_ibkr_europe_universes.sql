-- Idempotent PostgreSQL upgrade for the simplified IBKR Europe universe model.
ALTER TABLE market_assets ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 'IBKR';
ALTER TABLE market_assets ADD COLUMN IF NOT EXISTS pea_eligible BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE market_assets ADD COLUMN IF NOT EXISTS ucits BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE market_assets ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::jsonb;

INSERT INTO market_universes (name, region, asset_type, provider, enabled)
VALUES ('Europe Stocks', 'EU', 'STOCK', 'IBKR', TRUE),
       ('Europe ETF', 'EU', 'ETF', 'IBKR', TRUE)
ON CONFLICT (name) DO UPDATE SET region=EXCLUDED.region, asset_type=EXCLUDED.asset_type,
  provider=EXCLUDED.provider, updated_at=CURRENT_TIMESTAMP;

UPDATE market_assets a SET universe_id=n.id, region='EU', country='FR', exchange_code='PA',
  asset_type='STOCK', pea_eligible=TRUE, provider='IBKR', updated_at=CURRENT_TIMESTAMP
FROM market_universes o, market_universes n
WHERE a.universe_id=o.id AND o.name='Stocks Euronext Paris' AND n.name='Europe Stocks'
  AND (a.provider='IBKR' OR a.provider IS NULL);
UPDATE market_assets a SET universe_id=n.id, region='EU', asset_type='STOCK', provider='IBKR', updated_at=CURRENT_TIMESTAMP
FROM market_universes o, market_universes n
WHERE a.universe_id=o.id AND o.name='Stocks Europe' AND n.name='Europe Stocks'
  AND (a.provider='IBKR' OR a.provider IS NULL);
UPDATE market_assets a SET universe_id=n.id, region='EU', asset_type='ETF', pea_eligible=TRUE,
  ucits=TRUE, provider='IBKR', updated_at=CURRENT_TIMESTAMP
FROM market_universes o, market_universes n
WHERE a.universe_id=o.id AND o.name='ETF PEA' AND n.name='Europe ETF'
  AND (a.provider='IBKR' OR a.provider IS NULL);
UPDATE market_assets a SET universe_id=n.id, region='EU', asset_type='ETF', ucits=TRUE,
  provider='IBKR', updated_at=CURRENT_TIMESTAMP
FROM market_universes o, market_universes n
WHERE a.universe_id=o.id AND o.name='ETF Europe UCITS' AND n.name='Europe ETF'
  AND (a.provider='IBKR' OR a.provider IS NULL);
