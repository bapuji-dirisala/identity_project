# Skeleton notes

This repo is a **replica scaffold**. Only two end-to-end flows are
fully implemented:

1. **Streaming medallion** —
   `spark/python/src/identity_raw.py` → `identity_core.py` → `identity_hub_realtime.py`
   deployed via `workflows/dbx/identity_project_pipelines/wf_identity_raw.py`
   and `wf_identity_hub_load.py`.
2. **Airflow-triggered batch** —
   `airflow/dags/identity_project/cluster_1/identity_profile_deltas_dbx.py`
   (plus the Cerberus-rotation DAG `identity_rotate_sole_token.py`).

Every other folder you see is a **scaffold stub** — present so the shape of
the repo matches the real production monorepo. Each stub folder contains:

- an `__init__.py` so Python treats it as a package,
- a `README.md` explaining what the real module does,
- optionally a `_stub.py` file with illustrative function signatures.

### Where to look for each concern

| Concern                               | Folder                                                         |
| ------------------------------------- | -------------------------------------------------------------- |
| GDPR / PII cleanup                    | `spark/python/src/goodbyepii/`                                 |
| DQ helpers                            | `spark/python/src/transforms/common/spark_expectations_helper.py` |
| DQ rule seeds                         | `resources/dqrules/sql/*.sql`                                  |
| DDL for bronze/silver/gold            | `catalog/dbx/*.sql`                                            |
| DDL for Snowflake exports             | `catalog/snowflake/`                                           |
| Avro/StructType schemas (bronze/silver) | `spark/python/src/resources/identity_*_schema.py`            |
| Shared Airflow helpers (envs/clusters/secrets) | `airflow/dags/identity_project_config.py`             |
| Brickflow entrypoint for `brickflow deploy` | `workflows/entrypoint.py`                                 |
| Per-pipeline Databricks Job defs      | `workflows/dbx/identity_project_pipelines/wf_*.py`             |
| Per-pipeline job cluster props        | `workflows/dbx/resources/prop/*_prop.yaml`                     |
| Brickflow shared helpers              | `workflows/dbx/resources/common/common_utils.py`               |
| Jenkins declarative config            | `products/identity_project/AutoJenkinsfile`                    |
| SonarQube / Scan@Source config        | `sonar-project.properties`, `scanAtSourceRules.json`           |
| Quality-gate thresholds (QMA)         | `quality-config.yaml`                                          |

### Why the stubs matter

The point of the stubs is **recall**: six months from now you (or an AI
agent) can open any folder and immediately know *what used to live there*
in the real project, even though the production implementations aren't
reproduced here.
