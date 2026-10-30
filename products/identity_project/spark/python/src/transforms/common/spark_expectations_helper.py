"""
Thin wrapper around the open-source `spark-expectations` library. Every
silver/gold writer calls `apply_dq(...)` which:

  1. Loads expectation rows from the DQ control table (see
     `resources/dqrules/sql/identity_dq_rules*.sql`).
  2. Attaches them to the input DataFrame.
  3. Routes failed rows to the error table and good rows to the target.
  4. Emits DQ metrics to the stats table consumed by observability DAGs.
"""


def apply_dq(df, product_id: str, table_name: str, env: str = "prod"):
    """SCAFFOLD STUB: real impl constructs a SparkExpectations instance.

    Example (illustrative only):

        from spark_expectations.core.expectations import SparkExpectations
        se = SparkExpectations(product_id=product_id)
        se.with_expectations(
            target_table=table_name,
            write_to_table=True,
            row_dq=True,
        )
        return se.get_rules_from_df(df)
    """
    return df
