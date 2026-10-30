-- Prod-only strict rules (promoted to fail_batch severity).
INSERT INTO non_published_domain.identity_foundation_secure.dq_rules VALUES
  ('identity_hub_dimension', 'duplicate_upm_id', 'agg_dq',
   'count(1) = count(DISTINCT upm_id)', 'fail_batch', true);
