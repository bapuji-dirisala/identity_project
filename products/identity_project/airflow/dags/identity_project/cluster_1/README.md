# `airflow/dags/identity_project/cluster_1/` — scaffold stub

**Purpose in the real repository:** DAGs that get deployed to the Airflow 'cluster_1' MAP cluster. One DAG per Databricks job trigger.

> This folder is intentionally present but mostly empty. The 2 end-to-end
> examples live elsewhere; this directory exists so the repo's shape
> matches the production monorepo and a future reader (or AI agent) can
> immediately locate where a given concern would live.

### Example files you'd find here in prod

- `identity_profile_deltas_dbx.py` — **implemented** example (batch delta trigger)
- `identity_rotate_sole_token.py`   — **implemented** example (Cerberus rotation)
- In the real repo there are ~25 more DAGs here, one per domain
  (ltv, churn, bots, timezone, kop_deletes, scd, partner_consumption, etc.)

