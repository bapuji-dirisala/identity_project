"""
goodbyepii.goodbye_pii — SCAFFOLD STUB.

Purpose in the real repository
------------------------------
GDPR / privacy enforcement: scans silver + gold tables for PII that should have been removed (name, email, address, phone, DOB) and redacts or deletes offending rows. Runs on a scheduled Databricks job.

This file is a placeholder so the repo's directory shape matches the
production layout. Only 2 end-to-end examples
(`identity_raw` → `identity_core` → `identity_hub_realtime` for streaming,
and `identity_profile_deltas_dbx` for the Airflow-triggered batch) are
fully implemented. Everything else is skeleton to show *where* things live.
"""


def scan_table_for_pii(*args, **kwargs):
    """TODO (skeleton). See module docstring."""
    raise NotImplementedError("goodbyepii.goodbye_pii.scan_table_for_pii is a scaffold stub.")

def redact_rows(*args, **kwargs):
    """TODO (skeleton). See module docstring."""
    raise NotImplementedError("goodbyepii.goodbye_pii.redact_rows is a scaffold stub.")

def delete_rows(*args, **kwargs):
    """TODO (skeleton). See module docstring."""
    raise NotImplementedError("goodbyepii.goodbye_pii.delete_rows is a scaffold stub.")
