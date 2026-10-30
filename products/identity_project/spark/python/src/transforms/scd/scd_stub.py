"""
transforms.scd — SCAFFOLD STUB.

Purpose in the real repository
------------------------------
Slow-changing-dimension export jobs (hub → S3 → Snowflake).

This file is a placeholder so the repo's directory shape matches the
production layout. Only 2 end-to-end examples
(`identity_raw` → `identity_core` → `identity_hub_realtime` for streaming,
and `identity_profile_deltas_dbx` for the Airflow-triggered batch) are
fully implemented. Everything else is skeleton to show *where* things live.
"""


def build(*args, **kwargs):
    """TODO (skeleton). See module docstring."""
    raise NotImplementedError("transforms.scd.build is a scaffold stub.")

def run(*args, **kwargs):
    """TODO (skeleton). See module docstring."""
    raise NotImplementedError("transforms.scd.run is a scaffold stub.")
