'''Common Spark helpers used across identity transforms.'''

from pyspark.sql.functions import from_avro


def with_avro(df, value_col: str, schema: str):
    '''Deserialize an Avro-encoded binary column into a struct.'''
    return df.withColumn("decoded", from_avro(df[value_col], schema))
