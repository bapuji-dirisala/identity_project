# `catalog/snowflake/` — scaffold stub

**Purpose in the real repository:** DDL for the Snowflake export tier. Gold Databricks tables are periodically exported into Snowflake for BI / downstream consumers; the CREATE TABLE statements for those targets live here.

> This folder is intentionally present but mostly empty. The 2 end-to-end
> examples live elsewhere; this directory exists so the repo's shape
> matches the production monorepo and a future reader (or AI agent) can
> immediately locate where a given concern would live.

### Example files you'd find here in prod

`identity_hub_sf.sql`, `identity_users_sf.sql`, etc.

