"""
Brickflow multi-project top-level entrypoint.

`brickflow deploy` discovers all `wf_*.py` files under
`workflows/dbx/identity_project_pipelines/` by importing this entrypoint;
each discovered module exports a `Workflow` object which becomes one
Databricks Job.
"""

# Import every pipeline module here so Brickflow's workflow registry sees them.
# In the real repo this file imports ~25 wf_* modules. For the scaffold we
# import only the two fully-implemented examples.
from workflows.dbx.identity_project_pipelines import wf_identity_raw  # noqa: F401
from workflows.dbx.identity_project_pipelines import wf_identity_hub_load  # noqa: F401
