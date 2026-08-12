-- Remove the obsolete auto-seeded strategy fingerprint.  Requiring the whole
-- fingerprint keeps any partial or visibly edited administrator configuration.
DELETE FROM app_settings
WHERE category = 'strategy'
  AND key IN (
    'signal_entry_rsi_min',
    'signal_entry_rsi_max',
    'planner_min_score',
    'planner_min_rr',
    'signal_session_confirm_filter_enabled'
  )
  AND EXISTS (SELECT 1 FROM app_settings WHERE category = 'strategy' AND key = 'signal_entry_rsi_min' AND CAST(value AS TEXT) IN ('45', '45.0'))
  AND EXISTS (SELECT 1 FROM app_settings WHERE category = 'strategy' AND key = 'signal_entry_rsi_max' AND CAST(value AS TEXT) IN ('55', '55.0'))
  AND EXISTS (SELECT 1 FROM app_settings WHERE category = 'strategy' AND key = 'planner_min_score' AND CAST(value AS TEXT) IN ('4', '4.0'))
  AND EXISTS (SELECT 1 FROM app_settings WHERE category = 'strategy' AND key = 'planner_min_rr' AND CAST(value AS TEXT) = '0.8')
  AND EXISTS (SELECT 1 FROM app_settings WHERE category = 'strategy' AND key = 'signal_session_confirm_filter_enabled' AND CAST(value AS TEXT) IN ('true', '1'));
