'''
Shared Airflow helpers for the identity_project DAGs.

Mirrors the pattern from the production monorepo:
    - env-aware catalog + schema resolution,
    - Databricks + Sole workspace connection ids,
    - cluster config builder with autoscale,
    - failure callback with Slack + email alerting,
    - time-sensor helpers to wait N minutes from first poke.
'''

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote

from airflow.configuration import conf
from airflow.models import Variable

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_QUEUE = "airflow"

DATABRICKS_POLICY_ID       = "<DBX_CLUSTER_POLICY_ID>"
DATABRICKS_CONNECTION_ID   = "databricks_identity_default"       # main Databricks workspace
DATABRICKS_SOLE_CONNECTION_ID = "databricks_identity_sole_blazer" # dedicated Sole workspace

ACCESS_CONTROL_LIST = [
    {"group_name": "<OWNER_GROUP>", "permission_level": "CAN_MANAGE"}
]

CERBERUS_JOB_LIBRARIES = [
    {"pypi": {"package": "cerberus-python-client"}}
]

# Databricks runtime + node types
DBR_16_4            = "16.4.x-scala2.13"
NODE_R7GD_4XLARGE   = "r7gd.4xlarge"
NODE_R5D_12XLARGE   = "r5d.12xlarge"
NODE_M7GD_4XLARGE   = "m7gd.4xlarge"

# Required tech-solution tag enforced by cloud governance
TECH_SOLUTION_IDENTITY_EMR = "<TECH_SOLUTION_ID>"

DEFAULT_AWS_ATTRIBUTES = {"first_on_demand": 1, "availability": "ON_DEMAND"}
DEFAULT_SPARK_CONF     = {"spark.databricks.libraries.ignoreSSL": "true"}
MIN_AUTOSCALE_WORKERS  = 2

# Alerting
SLACK_CONN_ID        = "slack_webhook_api"
SLACK_CHANNEL_PROD   = "#identity-alerts"
SLACK_CHANNEL_DEV    = "#identity-dev-support"
DEFAULT_OWNER        = "identity"
DEFAULT_EMAILS       = ["data-engineering@example.com"]
EMAIL_ALERT_PROD_ONLY = True


# ---------------------------------------------------------------------------
# Profile / env helpers
# ---------------------------------------------------------------------------
def get_profile() -> Dict[str, Any]:
    # In production this comes from the enterprise `BICommon` helper that
    # reads the MAP profile (env name, email list, etc.).
    # Here we mock the shape so the DAG scaffolding runs locally.
    return {"ENV": Variable.get("env", default_var="dev"),
            "EMAIL_LIST": DEFAULT_EMAILS}


def get_env(profile: Optional[Dict[str, Any]] = None) -> str:
    profile = profile or get_profile()
    return profile["ENV"].lower()


def get_catchup(profile: Optional[Dict[str, Any]] = None) -> bool:
    return get_env(profile) == "prod"


# ---------------------------------------------------------------------------
# Databricks cluster builder
# ---------------------------------------------------------------------------
def build_cluster_config(spark_version, node_type_id, num_workers,
                         policy_id=DATABRICKS_POLICY_ID,
                         tech_solution_tag=TECH_SOLUTION_IDENTITY_EMR,
                         driver_node_type_id=None,
                         min_workers=MIN_AUTOSCALE_WORKERS,
                         **overrides):
    max_workers = num_workers
    autoscale_min = min(min_workers, max_workers)
    config = {
        "spark_version": spark_version,
        "node_type_id": node_type_id,
        "policy_id": policy_id,
        "autoscale": {"min_workers": autoscale_min, "max_workers": max_workers},
        "custom_tags": {"nike-techsolution": tech_solution_tag},
        "spark_conf": DEFAULT_SPARK_CONF.copy(),
        "aws_attributes": DEFAULT_AWS_ATTRIBUTES.copy(),
    }
    if driver_node_type_id:
        config["driver_node_type_id"] = driver_node_type_id
    config.update(overrides)
    return config


# ---------------------------------------------------------------------------
# S3 path helpers
# ---------------------------------------------------------------------------
SPARK_CODE_S3_BUCKET = "<SPARK_CODE_BUCKET>"
NGAP_S3_BUCKET       = "<NGAP_BUCKET>--prod--us-east-1"


def get_spark_code_s3_base(profile=None) -> str:
    env = "prod" if get_env(profile) == "prod" else "dev"
    return f"s3://{SPARK_CODE_S3_BUCKET}/<COMPANY>/{env}/identity-project/scripts/spark/transforms"


def spark_python_file(relative_path, profile=None) -> str:
    return f"{get_spark_code_s3_base(profile)}/{relative_path.lstrip('/')}"


def get_identity_deltas_path(profile=None) -> str:
    env = "prod" if get_env(profile) == "prod" else "dev"
    return f"s3://{NGAP_S3_BUCKET}/identity/{env}/deltas/"


# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------
def get_default_emails(profile=None) -> List[str]:
    return list(DEFAULT_EMAILS)


def get_slack_channel(profile=None) -> str:
    return SLACK_CHANNEL_PROD if get_env(profile) == "prod" else SLACK_CHANNEL_DEV


def _is_final_task_failure(ti) -> bool:
    return ti.try_number > ti.task.retries


def _send_slack_alert(message, channel=None):
    try:
        from airflow.providers.slack.hooks.slack import SlackHook
        SlackHook(slack_conn_id=SLACK_CONN_ID).call(
            "chat.postMessage",
            json={"channel": channel or get_slack_channel(), "text": message},
        )
    except Exception:
        logging.exception("Failed to send Slack alert")


def failure_email_callback(context):
    '''Send Slack + email on final task failure (not on intermediate retries).'''
    ti = context["task_instance"]
    if not _is_final_task_failure(ti):
        return
    from airflow.utils.email import send_email
    env     = get_env()
    dag_id  = context["dag"].dag_id
    task_id = ti.task_id
    exception = str(context.get("exception", ""))[:2000]
    msg = f":red_circle: *Task Failed* [{env}]\n*DAG:* {dag_id}\n*Task:* {task_id}\n*Error:* {exception}"
    _send_slack_alert(msg)
    if not EMAIL_ALERT_PROD_ONLY or env == "prod":
        try:
            send_email(to=get_default_emails(),
                       subject=f"[{env}] Task failed: {dag_id}.{task_id}",
                       html_content=f"<pre>{exception}</pre>")
        except Exception:
            logging.exception("Failed to send failure alert email")


def get_base_default_args(owner=DEFAULT_OWNER) -> Dict[str, Any]:
    return {
        "owner": owner,
        "email": DEFAULT_EMAILS,
        "email_on_failure": False,
        "email_on_retry": False,
        "on_failure_callback": failure_email_callback,
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    }
