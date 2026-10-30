"""
Brickflow workflow definition for the BRONZE stream (identity_raw).

What `brickflow deploy` turns this into:
    - a Databricks Job named `<env>_wf_identity_raw`
    - a single notebook task running `spark/python/src/identity_raw.py`
    - a job cluster per the identity_raw cluster config (m7gd.2xlarge x 1-5)
    - ACLs granted to the Developer / DataAdmin groups
    - failure emails routed to pager duty / email list from common_prop

Every `wf_*.py` under this folder becomes one Databricks Job on deploy.
"""

from brickflow import (
    ctx,
    TaskSettings,
    Workflow,
    WorkflowPermissions,
    Group,
    EmailNotifications,
)
from workflows.dbx.resources.common.common_utils import CommonUtils

utils = CommonUtils()
env = "dev" if ctx.env == "local" else ctx.env
conf = utils.get_config(
    cmn_prop_file_path="common_prop", job_prop_file_path="identity_raw_prop"
)

cluster_cfg = conf["identity_raw"]
task_name = "identity_raw"
task_settings = TaskSettings(max_retries=cluster_cfg.get("max_retries", 0))

if env == "prod":
    environment = "prod"
    product_id = conf["product_id"]
    tech_sol_id = conf["nike-techsolution"]
    source_catalog = dest_catalog = conf["catalog_config"]["prod"]["catalog"]
    target_schema = conf["catalog_config"]["prod"]["schema"]
    client_secret_scope = conf["catalog_config"]["prod"]["client_secret_scope"]
    client_secret_key = conf["catalog_config"]["prod"]["client_secret_key"]
    access_control = conf["access_control"]["prod"]["group"]
    email_rcpts = conf["email_recipient"]["eng-team"]
    on_failure_alert = conf["catalog_config"]["prod"]["pager_duty_var"]
    available_now = "false"
    checkpoint_version = "0.0.1"
    maxFilesPerTrigger = "5"
else:
    environment = "development"
    product_id = conf["product_id"]
    tech_sol_id = conf["nike-techsolution"]
    source_catalog = dest_catalog = conf["catalog_config"]["non_prod"]["catalog"]
    target_schema = conf["catalog_config"]["non_prod"]["schema"]
    client_secret_scope = conf["catalog_config"]["non_prod"]["client_secret_scope"]
    client_secret_key = conf["catalog_config"]["non_prod"]["client_secret_key"]
    access_control = conf["access_control"]["non_prod"]["group"]
    email_rcpts = conf["email_recipient"]["eng-team"]
    on_failure_alert = conf["catalog_config"]["non_prod"]["pager_duty_var"]
    available_now = "false"
    checkpoint_version = "0.0.1"
    maxFilesPerTrigger = "5"

wf = Workflow(
    utils.workflow_name(conf, "wf_identity_raw"),
    default_cluster=utils.create_job_cluster(cluster_cfg),
    schedule_pause_status="UNPAUSED" if ctx.env == "local" else "PAUSED",
    schedule_quartz_expression=None,  # streaming job - no schedule
    tags={"product_id": product_id, "tech_solution_id": tech_sol_id},
    permissions=WorkflowPermissions(can_manage=[Group(g) for g in access_control]),
    default_task_settings=TaskSettings(
        email_notifications=EmailNotifications(
            on_start=[], on_failure=on_failure_alert
        ),
    ),
)

base_parameters = (
    "{{"
    "'environment':'" + environment + "',"
    "'destination_catalog':'" + dest_catalog + "',"
    "'target_schema':'" + target_schema + "',"
    "'target_table_name':'identity_raw',"
    "'checkpoint_version':'" + checkpoint_version + "',"
    "'client_secret_scope':'" + client_secret_scope + "',"
    "'client_secret_key':'" + client_secret_key + "',"
    "'available_now':'" + available_now + "',"
    "'maxFilesPerTrigger':'" + maxFilesPerTrigger + "'"
    "}}"
)


@wf.task
def start():
    return "Workflow start task started"


@wf.notebook_task(name=task_name, task_settings=task_settings, depends_on=start)
def spark_notebook_task(
    *, notebook_path=cluster_cfg["notebook_path"], base_parameters=base_parameters
):
    return utils.create_notebook_task(notebook_path, base_parameters)


@wf.task(depends_on=[task_name])
def end():
    return "Workflow completed"
