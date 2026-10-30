"""
Brickflow workflow definition for the GOLD stream (identity_hub_realtime).
Trigger: continuous (60s processing time). Cluster: m7gd.4xlarge x 1-7.
Mirrors wf_identity_raw but points at identity_hub_realtime notebook.
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
    cmn_prop_file_path="common_prop", job_prop_file_path="identity_hub_prop"
)

cluster_cfg = conf["identity_hub"]
task_name = "identity_hub_load_realtime"
task_settings = TaskSettings(max_retries=cluster_cfg.get("max_retries", 0))

if env == "prod":
    env_name = "prod"
    source_catalog = dest_catalog = conf["catalog_config"]["prod"]["catalog"]
    target_schema = conf["catalog_config"]["prod"]["schema"]
    access_control = conf["access_control"]["prod"]["group"]
    on_failure_alert = conf["catalog_config"]["prod"]["pager_duty_var"]
    source_table = "identity_core_stg"
else:
    env_name = "development"
    source_catalog = dest_catalog = conf["catalog_config"]["non_prod"]["catalog"]
    target_schema = conf["catalog_config"]["non_prod"]["schema"]
    access_control = conf["access_control"]["non_prod"]["group"]
    on_failure_alert = conf["catalog_config"]["non_prod"]["pager_duty_var"]
    source_table = "identity_core"

wf = Workflow(
    utils.workflow_name(conf, "wf_identity_hub_load_realtime"),
    default_cluster=utils.create_job_cluster(cluster_cfg),
    schedule_pause_status="UNPAUSED" if ctx.env == "local" else "PAUSED",
    schedule_quartz_expression=None,
    tags={
        "product_id": conf["product_id"],
        "tech_solution_id": conf["nike-techsolution"],
    },
    permissions=WorkflowPermissions(can_manage=[Group(g) for g in access_control]),
    default_task_settings=TaskSettings(
        email_notifications=EmailNotifications(
            on_start=[], on_failure=on_failure_alert
        ),
    ),
)

base_parameters = (
    "{{"
    "'environment':'" + env_name + "',"
    "'source_catalog':'" + source_catalog + "',"
    "'destination_catalog':'" + dest_catalog + "',"
    "'source_schema':'" + target_schema + "',"
    "'target_schema':'" + target_schema + "',"
    "'source_table_name':'" + source_table + "',"
    "'target_table_name':'identity_hub_dimension',"
    "'checkpoint_version':'0.0.2',"
    "'maxFilesPerTrigger':'50'"
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
