# `.github/workflows/` — intentionally empty

The real CI/CD for this repo is **Jenkins** (see top-level `Jenkinsfile` and
`products/identity_project/AutoJenkinsfile`), not GitHub Actions.

The original `ci.yml` was moved to `.github/workflows-disabled/ci.yml` on
2026-10-30. See [`../workflows-disabled/README.md`](../workflows-disabled/README.md)
for the full rationale (TL;DR: environment mismatch between a bare
`ubuntu-latest` runner and a real Spark/Delta/Databricks environment).

Add workflow files here only if you deliberately want GitHub Actions to run
them.
