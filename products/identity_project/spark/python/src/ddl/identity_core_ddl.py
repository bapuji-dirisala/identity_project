'''DDL for silver layer: cleansed identity profile.'''
from pyspark.sql import SparkSession
from ddl.common_ddl_utils import configure_delta_table_builder


def create_table(table_name: str, spark: SparkSession):
    schema = [
        ("upm_id",                 "STRING",    "Unique profile member id"),
        ("email_address_txt",      "STRING",    "Primary email"),
        ("first_nm",               "STRING",    "First name"),
        ("last_nm",                "STRING",    "Last name"),
        ("dob_dt",                 "DATE",      "Date of birth"),
        ("country_iso2_cd",        "STRING",    "ISO2 country code"),
        ("registration_tmst_utc",  "TIMESTAMP", "Registration timestamp UTC"),
        ("profile_update_tmst_utc","TIMESTAMP", "Last profile update UTC"),
        ("record_load_tmst_utc",   "TIMESTAMP", "Row load timestamp"),
    ]
    builder = configure_delta_table_builder(spark, table_name, schema,
        "Silver cleansed identity profile (CDC-driven, 120s streaming merge)")
    builder.execute()
