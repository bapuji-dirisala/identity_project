# identity_project (data product)

Member identity data product. Owned by `<YOUR_TEAM>`.

- Streaming: Kafka -> Delta Bronze -> Silver -> Gold (every 60 s).
- Batch: Daily Airflow DAGs for delta computations + Snowflake / RDS exports.
- Deploy: Jenkins -> S3 (spark code) + Brickflow (Databricks jobs) + Airflow (DAGs).

See the top-level `README.md` for the full end-to-end tour.
