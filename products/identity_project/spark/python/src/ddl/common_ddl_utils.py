'''Shared Delta table builder with standard enterprise properties.'''

from delta.tables import DeltaTable


def configure_delta_table_builder(spark, table_name: str, schema: list, table_comment: str):
    '''
    Builds a DeltaTable with the schema + standard properties applied.

    schema: list of (col_name, col_type) or (col_name, col_type, col_comment) tuples.
    '''
    builder = DeltaTable.createIfNotExists(spark).tableName(table_name)
    for col in schema:
        if len(col) == 3:
            name, dtype, comment = col
            builder = builder.addColumn(name, dataType=dtype, nullable=True, comment=comment)
        else:
            name, dtype = col
            builder = builder.addColumn(name, dataType=dtype, nullable=True)
    builder = builder.comment(table_comment)
    return _apply_common_properties(builder)


def _apply_common_properties(builder):
    return (builder
        .property("delta.enableChangeDataFeed",              "true")
        .property("delta.enableDeletionVectors",             "true")
        .property("delta.autoOptimize.autoCompact",          "true")
        .property("delta.autoOptimize.optimizeWrite",        "true")
        .property("delta.deletedFileRetentionDuration",      "interval 7 days")
        .property("nike.table.sla.tracker.enabled",          "true")
        .property("nike.table.sla.check_frequency",          "interval 1 hour")
        .property("nike.table.sla.expected_refresh_frequency","interval 4 hours")
        .property("nike.eng.organization",                   "edai")
        .property("targetFileSize",                          "256mb")
        .property("nike.sole.ops.runbook_location",
                  "https://github.com/<YOUR_GITHUB_ORG>/identity_project/blob/main/README.md"))
