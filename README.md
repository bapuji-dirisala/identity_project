# identity_project

> **Replica skeleton** of a production enterprise data product mono-repo.
> Mirrors the layout, deployment pipeline, and architectural patterns of a
> real Databricks + Airflow + Delta Lake project at a Fortune-50 scale.
>
> Two end-to-end flows are fully implemented so you can trace *exactly*
> what happens from `git push` all the way to a row landing in the gold
> dimension table in production.
>
> All references to the original company have been generalized:
> `<COMPANY>`, `<CLOUD_ACCOUNT_ID>`, `<CERBERUS_URL>`, `<DBX_WORKSPACE>` etc.

---

## 1. What this project does

A **member identity data product**. It ingests raw identity events from a
Kafka topic, cleanses them in a silver layer, and merges them into a
business-facing gold dimension table (`identity_hub_dimension`) that powers
downstream analytics (Snowflake exports, RDS, marketing, personalization).

Two reference pipelines ship end-to-end:

| # | Pipeline | Entry point | Pattern |
|---|---|---|---|
| 1 | **Streaming medallion** (Bronze -> Silver -> Gold) | `products/identity_project/spark/python/src/identity_raw.py` | Databricks Structured Streaming, foreachBatch MERGE, idempotent on `upm_id` |
| 2 | **Airflow-orchestrated batch delta** | `products/identity_project/airflow/dags/identity_project/cluster_1/identity_profile_deltas_dbx.py` | DAG with `DatabricksSubmitRunOperator`, time-sensor + task-dependency sensor, Spark job computes deltas vs the hub table |

Everything else is a stub with inline comments showing where real
implementations go.

---

## 2. Architecture at a glance

```
                   +----------------------+
                   |     Developer        |
                   |  git push origin ... |
                   +----------+-----------+
                              v
          +-------------------+----------------------+
          |             GitHub Enterprise            |
          |   nike-style monorepo /identity_project  |
          +------+------------------+----------------+
                 |                  |
                 v                  v
         +-------+------+   +-------+---------+
         |  Jenkins CI  |   | GitHub Actions  |  (lint + pytest on PR)
         |  pipeline    |   +-----------------+
         +---+----------+
             |
   +---------+---------+---------------------+-------------------------+
   | Scan@Source       | Pytest + Coverage  | S3 Deploy (Spark code)  |
   | (SCA/secret/IaC)  | (QMA / SonarQube)  | s3://<BUCKET>/<ENV>/... |
   +---+---------------+--------------------+------------+------------+
       |                                                 |
       v                                                 v
   +---+---------------+                     +-----------+------------+
   | Brickflow deploy  |                     |  Airflow (MWAA / MAP) |
   | Databricks Jobs   |                     |  deploys DAGs from     |
   | via DAB bundles   |                     |  airflow/dags/         |
   +---+---------------+                     +-----------+------------+
       |                                                 |
       v                                                 v
   +---+-------------------------------------------------+------------+
   |              Databricks workspace (classic compute)             |
   |   Streaming jobs, foreachBatch MERGE into Delta on S3           |
   |                                                                  |
   |   Bronze (member_profile_raw)  <-- Kafka (Schema Registry Avro) |
   |      v                                                           |
   |   Silver (identity_core)       <-- Delta CDC, cleansing          |
   |      v                                                           |
   |   Gold  (identity_hub_dim)     <-- foreachBatch MERGE +          |
   |                                    10 batch enrichment deltas    |
   +------+----------------------------------+------------------------+
          |                                   |
          v                                   v
   +------+--------+                  +-------+--------+
   | Snowflake     |                  | RDS exports via|
   | marts         |                  | NGAP S3 drops  |
   +---------------+                  +----------------+
```

**Control plane** = Databricks workspace (jobs, notebooks, UC metastore, secrets).
**Compute plane** = EC2 clusters inside your own AWS account, launched via
cluster policy. Data (Delta files on S3, Kafka messages) **never** leaves
your AWS.

---

## 3. Repository layout

```
identity_project/
|-- README.md                           <- this file (end-to-end tour)
|-- Jenkinsfile                         <- top-level Jenkins pipeline (library-driven)
|-- brickflow-multi-project.yml         <- Brickflow multi-project registry
|-- .gitignore
|-- .github/
|   |-- CODEOWNERS
|   |-- pull_request_template.md
|   `-- workflows/ci.yml                <- lint + pytest on PR (GH Actions)
`-- products/
    `-- identity_project/
        |-- AutoJenkinsfile             <- declarative pipeline config (envs, S3, Brickflow, tags)
        |-- Dockerfile                  <- build image (python 3.11 + JDK 11 + poetry + node)
        |-- pyproject.toml              <- poetry deps, build-system
        |-- product-info.toml           <- product metadata (owner, tags)
        |-- quality-config.yaml         <- QMA quality gates (pytest, coverage, Scan@Source)
        |-- sonar-project.properties    <- SonarQube config
        |-- scanAtSourceRules.json      <- security scan rules
        |-- .brickflow-project-root.yml <- Brickflow project marker (auto-generated)
        |-- README.md                   <- product-specific README
        |
        |-- airflow/
        |   |-- dags/
        |   |   `-- identity_project/
        |   |       `-- cluster_1/
        |   |           |-- identity_project_config.py       <- shared helpers (connections, cluster, alerting)
        |   |           |-- identity_profile_deltas_dbx.py   <- EXAMPLE DAG 1 (batch Spark on Databricks)
        |   |           `-- identity_rotate_sole_token.py    <- Cerberus-driven token rotation
        |   `-- resources/
        |       `-- requirements.txt    <- Airflow DAG-level pip deps
        |
        |-- spark/
        |   `-- python/
        |       |-- src/
        |       |   |-- identity_raw.py          <- EXAMPLE JOB (bronze streaming notebook)
        |       |   |-- identity_core.py         <- EXAMPLE JOB (silver streaming notebook)
        |       |   |-- identity_hub_realtime.py <- EXAMPLE JOB (gold streaming + foreachBatch MERGE)
        |       |   |-- ddl/
        |       |   |   |-- common_ddl_utils.py
        |       |   |   |-- identity_raw_ddl.py
        |       |   |   |-- identity_core_ddl.py
        |       |   |   `-- identity_hub_ddl.py
        |       |   |-- transforms/
        |       |   |   |-- common/
        |       |   |   |   `-- identity_project.py
        |       |   |   |-- cleansed/
        |       |   |   |   `-- identity_cleansed.py
        |       |   |   `-- hub/
        |       |   |       `-- identity_hub.py  <- merge_results_* functions (heart of gold layer)
        |       |   |-- resources/
        |       |   |   `-- config/
        |       |   |       `-- identity_hub.yaml <- enrichment-source cadence config
        |       |   `-- se/
        |       |       `-- se_common.py          <- Spark Expectations DQ helper
        |       `-- tests/
        |           |-- conftest.py
        |           `-- transforms/
        |               `-- hub/
        |                   `-- test_identity_hub.py
        |
        |-- workflows/
        |   `-- dbx/                     <- Brickflow (Databricks Asset Bundle equivalent)
        |       |-- identity_project_pipelines/
        |       |   |-- __init__.py
        |       |   |-- wf_identity_raw.py       <- Brickflow workflow for bronze
        |       |   `-- wf_identity_hub_load.py  <- Brickflow workflow for gold streaming
        |       `-- resources/
        |           |-- common/
        |           |   `-- common_utils.py       <- Brickflow helpers (cluster, config loader)
        |           `-- prop/
        |               |-- common_prop.yaml      <- shared config (catalogs, email, access)
        |               |-- identity_hub_prop.yaml
        |               `-- identity_raw_prop.yaml
        |
        |-- catalog/
        |   `-- dbx/
        |       |-- identity_raw_ddl.sql
        |       `-- identity_core_ddl.sql
        |
        |-- resources/
        |   `-- dqrules/
        |       `-- sql/
        |           |-- identity_dq_rules.sql       <- Spark Expectations rules (DQ)
        |           `-- identity_dq_rules_prod.sql
        |
        `-- utils/
            `-- schema/
                `-- schema_registry.py              <- Avro / Schema Registry client wrapper
```

---

## 4. Deployment pipeline (from `git push` to prod)

1. **Developer pushes** a feature branch to GitHub Enterprise.
2. **PR opened** -> GitHub Actions runs `./github/workflows/ci.yml` (lint, pytest).
3. **PR reviewed + merged** into the branch mapped to the target env in
   `AutoJenkinsfile.buildFlow`:
   - `Development` -> `dev`
   - `qa` -> `qa`
   - `uat` -> `uat`
   - `main` -> `prod`
4. **Jenkins pipeline** (`Jenkinsfile`) fires. It uses shared libraries:
   - `cop-pipeline-step` (common build primitives)
   - `cicd-pipeline`     (QMA quality gates)
   - `dataeng-jsl`       (data-eng-specific stages)
   - `ae-build-pipeline` (analytics engineering build conventions)
   - `common-utils-cicd` (shared utils)
5. **Stages** executed by Jenkins (from the shared pipeline + `AutoJenkinsfile`):
   1. **Scan@Source** - SCA, secret, static, and IaC scans. Fail-closed.
   2. **Tag Validation** - every cloud resource must have required tags
      (`application`, `owner`, `distributionlist`, `businessappid`, `tagguid`).
   3. **Pyspark unittest** - builds the image
      `<ARTIFACTORY>/jsl/unittest:0.0.3`, mounts the workspace, runs
      `make ci-cov` which runs pytest and emits `coverage.xml`.
   4. **QMA quality gates** - pytest results + coverage + Scan@Source results
      checked against `quality-config.yaml` thresholds.
   5. **SonarQube** - adds badges, decorates PR with code-quality findings.
   6. **CodeDeploy**:
      - `s3Deploy` block pushes `spark/python/src/transforms/` to
        `s3://<SPARK_CODE_BUCKET>/<ENV>/identity-project/scripts/spark/transforms/`
        using an AWS IAM role assumed via Jenkins.
      - **Brickflow deploy** - packages `products/identity_project/workflows/`
        into a Databricks Asset Bundle and deploys to the target workspace
        (`<DBX_WORKSPACE>`). The service principal auth comes from a secret
        in Cerberus (`<CERBERUS_URL>/app/<COMPANY>-data/`).
      - **Airflow deploy** - uploads `airflow/dags/` to the target
        Airflow (MWAA/MAP) cluster via Okta-authenticated API.
   7. **Launch Awareness** - posts to Slack channel so other teams know
      a deploy just happened.
6. **In Databricks**: the Brickflow bundle registers/updates one Job per
   `wf_*.py` file under `workflows/dbx/`. Each Job has its own cluster
   policy, tags, and ACL (`CAN_MANAGE` to the owner group).
7. **In Airflow**: the DAGs appear in the UI and run on their schedule.
   DAGs trigger Databricks jobs via `DatabricksSubmitRunOperator` using the
   `databricks_<prefix>_<workspace>` Airflow connection (token refreshed
   weekly by a dedicated DAG - see `identity_rotate_sole_token.py`).

---

## 5. Medallion architecture (what's built)

| Layer | Table | Load | Trigger |
|---|---|---|---|
| **Bronze** | `identity_raw` | Kafka Avro streaming | continuous |
| **Silver** | `identity_core` | Delta CDC streaming | 120 s |
| **Gold** | `identity_hub_dimension` | foreachBatch MERGE on `upm_id`, enriched by 10 batch deltas via a version-stats table | 60 s |
| Export | Snowflake / RDS via NGAP S3 | Daily batch (Airflow) | daily |

Delta properties standardized across all tables (see
`spark/python/src/ddl/common_ddl_utils.py`):

- `delta.enableChangeDataFeed = true`
- `delta.enableDeletionVectors = true`
- `delta.autoOptimize.autoCompact = true`
- `delta.autoOptimize.optimizeWrite = true`
- `delta.deletedFileRetentionDuration = interval 7 days`
- `targetFileSize = 256mb`

Gold is **liquid-clustered** on `(upm_id, process_date)` for cheap pruning
on high-cardinality merge keys.

---

## 6. Example 1 - Streaming medallion walkthrough

**Trigger**: a producer publishes a profile-update event to Kafka topic
`identity-events`.

1. **Bronze** - `spark/python/src/identity_raw.py` runs continuously on a
   Databricks cluster (`m7gd.2xlarge` x 1-5). It reads Kafka via
   `pycaasio.sources.kafka.KafkaSourceGenerator`, deserializes Avro using
   `utils/schema/schema_registry.py` (versions `v1` and `v2` unioned),
   writes to `<CATALOG>.<SCHEMA>.identity_raw` with `mergeSchema=true`.
2. **Silver** - `spark/python/src/identity_core.py` runs continuously (120 s
   trigger). It reads `identity_raw` as a Delta CDC stream
   (`skipChangeCommits=true`), cleans + casts, and writes
   `<CATALOG>.<SCHEMA>.identity_core`.
3. **Gold** - `spark/python/src/identity_hub_realtime.py` runs continuously
   (60 s trigger, `m7gd.4xlarge` x 1-7). The main stream is from
   `identity_core`; on **each micro-batch** (`foreachBatch`) it also checks
   10 batch enrichment delta tables (`identity_hub.yaml`) via a *version-stats
   table* (`identity_hub_realtime_sources_stats`), and only re-merges
   sources whose Delta version advanced since last run. Every MERGE is
   idempotent on `upm_id` so re-runs are safe.

Related code:
- Merge functions: `transforms/hub/identity_hub.py` (`merge_results_*`)
- DDL: `ddl/identity_hub_ddl.py`
- Brickflow deploy: `workflows/dbx/identity_project_pipelines/wf_identity_hub_load.py`

---

## 7. Example 2 - Airflow batch delta DAG walkthrough

**Trigger**: Airflow scheduler fires `identity_profile_deltas` daily.

1. `BatchStartOperator` registers the batch in the enterprise lineage system.
2. `TimeDeltaSensor` waits 11 h 55 m after midnight UTC (gives upstream
   enough time to publish yesterday's partitions).
3. `TaskDependencySensor` blocks until an upstream DAG
   (`dpa_user_activity_v2`) has succeeded on today's partition.
4. `DatabricksSubmitRunOperator` submits a Spark Python job to Databricks:
   - Spins a job cluster (`r7gd.4xlarge` x 10) with the shared policy.
   - Downloads the Spark code from
     `s3://<SPARK_CODE_BUCKET>/<ENV>/identity-project/scripts/spark/transforms/identity_profile/identity_profile_deltas.py`.
   - Reads `identity_core` + `identity_hub_dimension`, computes rows that
     changed, writes deltas to `identity_hub_deltas` (used by the realtime
     gold job as an enrichment source).
5. `BatchEndOperator` closes the batch, triggers downstream notifications.

Alerts: `on_failure_callback = failure_email_callback` sends a Databricks-UI-
style Slack message + email to the owner group on *final* retry failure
(see `identity_project_config.py`).

---

## 8. Secrets, auth, networking

- **Cerberus** (`<CERBERUS_URL>`) stores all service-principal credentials.
  Keys are versioned per environment (`.../Developer`, `.../DataAdmin`).
  A weekly rotation DAG (`identity_rotate_sole_token.py`) pulls the latest
  SP token and upserts the Airflow Databricks connection.
- **Kafka** credentials come from Databricks Secret Scopes
  (`memberdata-scope.kafka.memberdata.client_secret`).
- **AWS IAM** - clusters assume an instance profile
  (`arn:aws:iam::<CLOUD_ACCOUNT_ID>:instance-profile/sole/group/memberdata`)
  granting S3 read/write on the project buckets.
- **Databricks workspace** - two workspaces used for workload isolation:
  one for continuous streaming + batch (`<DBX_WORKSPACE>`), one for
  Sole/NGAP loads (`<DBX_SOLE_WORKSPACE>`).

---

## 9. Data quality

- **Spark Expectations** rules live in
  `resources/dqrules/sql/identity_dq_rules.sql` and are evaluated inside
  the silver + gold notebooks when `spark_expectations_enabled=true`.
- **DQ completeness Brickflow** job (`wf_identity_dq_completeness`) runs
  after each load, writes stats to
  `<CATALOG>.<SCHEMA>.identity_sole_validations_stats`, fires
  `completeness_failure_callback` on breach.
- **Scan@Source** (SCA, secret, static, IaC) gates every build in Jenkins.
- **SonarQube** + QMA quality config enforce coverage > threshold and
  decorate PRs.

---

## 10. Interview-relevant takeaways

- **Classic compute vs serverless** - this repo is classic: explicit
  `node_type_id` + `policy_id` in every cluster definition. The data lives
  in your AWS, metadata in Databricks' control plane.
- **Idempotency recipe** - natural keys + MERGE + version-stats table +
  checkpoint on UC Volume + snapshot fallback when CDF is unavailable.
- **Cost control** - cluster tags for FinOps, autoscale min=1 max=N,
  pool-less job clusters, Photon disabled on UDF-heavy streams.
- **Schema evolution** - `mergeSchema=true` + Schema Registry versions
  unioned + stable projection views (`identity_hub_v2_v`) for downstream
  contracts.
- **DAB / Brickflow** - the workflow definitions in `workflows/dbx/` get
  bundled and deployed as Databricks Asset Bundles. One `wf_*.py` file =
  one Databricks Job.

See the separate `MemberHub_Interview_Prep.docx` (if you have it) for 12
deep-dive Q&As grounded in the same patterns used here.
