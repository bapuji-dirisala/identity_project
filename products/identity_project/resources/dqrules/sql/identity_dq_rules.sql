-- Spark Expectations DQ rules evaluated inside the streaming notebooks.
-- Each row is an assertion; actions on failure: ignore / drop_row / fail_batch.

-- Primary key not-null
INSERT INTO non_published_domain.identity_foundation_secure.dq_rules VALUES
  ('identity_core', 'upm_id_not_null',  'row_dq', 'upm_id IS NOT NULL',                      'drop_row',   true),
  ('identity_core', 'email_format',     'row_dq', 'email_address_txt RLIKE "^.+@.+\\..+$"',   'drop_row',   false),
  ('identity_core', 'country_iso2_len', 'row_dq', 'country_iso2_cd IS NULL OR LENGTH(country_iso2_cd)=2',
                                                                                               'drop_row',   false);

-- Aggregated / completeness checks
INSERT INTO non_published_domain.identity_foundation_secure.dq_rules VALUES
  ('identity_hub_dimension', 'row_count_min',  'agg_dq', 'count(1) > 1000',    'fail_batch', true),
  ('identity_hub_dimension', 'bot_ind_null_rate', 'agg_dq', 'sum(CASE WHEN bot_ind IS NULL THEN 1 ELSE 0 END) * 1.0 / count(1) < 0.05',
                                                                                              'fail_batch', false);
