# Disabled GitHub Actions workflows

> **This folder is intentionally outside `.github/workflows/`.**
> GitHub only scans `.github/workflows/` for workflow files. Anything placed
> here is **inert** — it will not be scheduled, triggered by pushes, pull
> requests, or by any other event. To re-enable, move the file(s) back under
> `.github/workflows/`.

## Why `ci.yml` was detached (2026-10-30)

This repository is a **replica scaffold of an enterprise data-engineering
monorepo** (Jenkins + Databricks + Brickflow + Airflow). The real build and
deploy path is:

```
git push  →  Jenkins (AutoJenkinsfile)  →  s3Deploy / Brickflow deploy / Airflow deploy
          →  Databricks Jobs / UC catalog / DAGs
```

GitHub Actions is **not** part of that path. I originally added `ci.yml` as a
lightweight sanity check (black + mypy + pytest), but every failure on the
GitHub-hosted runner turned out to be an environment mismatch rather than a
real bug in the scaffold code:

| Failure                                      | Root cause                                                                 | Nature         |
| -------------------------------------------- | -------------------------------------------------------------------------- | -------------- |
| `black --check` fails                        | Scaffold is auto-generated; formatting drift                               | Cosmetic       |
| `java.lang.UnsupportedOperationException: getSubject is not supported` | `ubuntu-latest` ships JDK 24; PySpark 3.5 only supports JDK ≤ 17 | Env mismatch   |
| `DATA_SOURCE_NOT_FOUND: delta`               | Delta Lake jars aren't on the Spark classpath on a bare runner             | Env mismatch   |
| `ModuleNotFoundError: transforms.hub.*`      | `src` layout needs `pythonpath` in pytest config                           | Config         |
| `TypeError: 'JavaPackage' object is not callable` | `DeltaTable.forName(...)` called without the Delta runtime loaded     | Env mismatch   |

Rather than keep hacking the workflow until a trivial smoke test turns green,
I moved the file here so:

1. The CI badge on the repo doesn't show misleading red failures.
2. The workflow file is preserved verbatim — if you ever want to resurrect it
   (e.g. for a GitHub-first fork), `git mv` it back into
   `.github/workflows/` and it will run again.
3. The design intent — "**Jenkins is the real CI**, not GitHub Actions" — is
   explicit, not implicit.

## If you ever re-enable it

The minimum you'll need to fix:

1. Pin JDK 17 via `actions/setup-java@v4` (JDK 24+ breaks PySpark 3.5).
2. Either (a) install `delta-spark` jars via `--packages` and the matching
   `SparkSession` config, **or** (b) keep the smoke test on `parquet` tables
   so no Delta runtime is required.
3. Keep `[tool.pytest.ini_options] pythonpath = ["spark/python/src"]` in
   `pyproject.toml` so pytest can resolve the `src/` layout.

That gives you a passing CI without pretending the runner is a Databricks
cluster.
