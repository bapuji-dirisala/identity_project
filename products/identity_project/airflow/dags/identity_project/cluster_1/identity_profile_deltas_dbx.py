"""
EXAMPLE DAG 1 - batch delta computation triggered daily.

Flow:
    BatchStart
        -> TimeDeltaSensor (wait 11h55m after midnight UTC so upstream has published)
        -> TaskDependencySensor (wait for upstream DAG success)
        -> DatabricksSubmitRunOperator (runs Spark job on an ephemeral cluster)
        -> BatchEnd

The Spark Python file it submits lives in the Spark code S3 bucket, deployed
by the Jenkins s3Deploy stage from `spark/python/src/transforms/identity_profile/...`.

This mirrors the pattern of member_hub_load_dpa_undeliverable_to_rds_dbx.py
in the production monorepo.
"""

# Deploy-sync comment: the Airflow plugin picks up changes based on file mtime.

import airflow
from airflow import DAG
from airflow.utils import dates
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)
from datetime import timedelta

# BatchStart/End + TaskDependencySensor come from the enterprise MAP plugin;
# stubbed as DummyOperator-equivalents here so this file is self-contained.
try:
    from map.operators.batch_ops_plugin import BatchStartOperator, BatchEndOperator
    from map.operators.taskdependencysensor_plugin import TaskDependencySensor
except Exception:
    from airflow.operators.dummy import DummyOperator as BatchStartOperator  # type: ignore
    from airflow.operators.dummy import DummyOperator as BatchEndOperator  # type: ignore
    from airflow.sensors.external_task_sensor import ExternalTaskSensor as TaskDependencySensor  # type: ignore

from identity_project_config import (
    get_profile,
    get_env,
    get_catchup,
    get_base_default_args,
    build_cluster_config,
    spark_python_file,
    get_identity_deltas_path,
    DEFAULT_QUEUE,
    DATABRICKS_CONNECTION_ID,
    ACCESS_CONTROL_LIST,
    DBR_16_4,
    NODE_R7GD_4XLARGE,
)

default_args = get_base_default_args()
profile = get_profile()
env = get_env(profile)
dag_name = "identity_profile_deltas"
default_queue = DEFAULT_QUEUE

default_args.update(
    {
        "start_date": dates.days_ago(4),
        "owner": "identity",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    }
)

commonClusterConfig = build_cluster_config(DBR_16_4, NODE_R7GD_4XLARGE, 10)
identity_deltas_path = get_identity_deltas_path(profile)


with DAG(
    dag_id=dag_name,
    default_args=default_args,
    catchup=get_catchup(profile),
    schedule_interval="0 0 * * *",
) as dag:

    t_batch_start = BatchStartOperator(
        queue=default_queue,
        dag=dag,
        retries=5,
        retry_delay=timedelta(minutes=5),
    )
    t_batch_end = BatchEndOperator(queue=default_queue, dag=dag)

    # Give upstream DAGs time to publish today's partition before we start.
    task_start_time = TimeDeltaSensor(
        task_id="time_sensor",
        delta=timedelta(hours=11, minutes=55),
        queue=default_queue,
        dag=dag,
    )

    # Wait for an upstream DAG success (identity_user_activity).
    t_dependency_user_activity = TaskDependencySensor(
        task_id="dependency_user_activity",
        external_dag_id="identity_user_activity_v2",
        external_task_id="success",
        allowed_states=["success"],
        cluster_id="<UPSTREAM_CLUSTER_ID>",
        queue=default_queue,
        dag=dag,
    )

    # Submit the Spark job to Databricks. The job spins up a new cluster,
    # pulls `identity_profile/identity_profile_deltas.py` from S3, runs it,
    # terminates.
    t_spark_identity_deltas = DatabricksSubmitRunOperator(
        task_id="spark_identity_profile_deltas",
        databricks_conn_id=DATABRICKS_CONNECTION_ID,
        spark_python_task={
            "python_file": spark_python_file(
                "identity_profile/identity_profile_deltas.py"
            ),
            "parameters": [
                "--source_db",
                "<CATALOG>.<SCHEMA>",
                "--identity_deltas_path",
                identity_deltas_path,
            ],
        },
        new_cluster={
            **commonClusterConfig,
            "name": f"{dag_name}_cluster",
        },
        access_control_list=ACCESS_CONTROL_LIST,
        dag=dag,
    )

    (
        t_batch_start
        >> task_start_time
        >> t_dependency_user_activity
        >> t_spark_identity_deltas
        >> t_batch_end
    )
