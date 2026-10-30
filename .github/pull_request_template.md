## Summary

<!-- One-paragraph description of what this PR changes and why -->

## Related tickets

- JIRA-XXXX

## Checklist

- [ ] Tests added / updated and passing locally (`poetry run pytest`)
- [ ] Schema changes are additive-only (no column drops / type changes)
- [ ] Secrets not committed; references go through Cerberus / Databricks Secrets
- [ ] DQ rules updated under `resources/dqrules/sql/` if semantics changed
- [ ] Scan@Source clean (no new high-severity findings)
- [ ] Owners / tags on any new cloud resources
- [ ] Downstream consumer impact considered (views, Snowflake, RDS)

## Deployment plan

<!-- dev -> qa -> uat -> prod. Note any manual steps. -->

## Rollback plan

<!-- How to revert if something goes wrong in prod -->
