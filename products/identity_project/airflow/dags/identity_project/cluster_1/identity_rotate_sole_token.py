'''
Weekly token-rotation DAG.

Pattern: production monorepos store Databricks SP tokens in Cerberus (or any
enterprise secret vault). A dedicated DAG pulls the latest token every week
and upserts the Airflow Databricks `Connection` row so other DAGs continue
to authenticate without manual intervention.

Schedule: every Tuesday at 03:00 UTC.
'''
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.settings import Session

try:
    from map.operators.batch_ops_plugin import BatchStartOperator, BatchEndOperator
except Exception:
    from airflow.operators.dummy import DummyOperator as BatchStartOperator  # type: ignore
    from airflow.operators.dummy import DummyOperator as BatchEndOperator    # type: ignore

from identity_project_config import (
    get_profile, get_env, DEFAULT_QUEUE, get_base_default_args,
)

log = logging.getLogger(__name__)

profile = get_profile()
env     = get_env(profile)
dag_name       = "identity_rotate_sole_token"
default_queue  = DEFAULT_QUEUE
default_args   = get_base_default_args()

# Secret vault path + service-principal key
cerberus_path  = "app/<COMPANY>-data/data"
sp_role        = "DataAdmin" if env == "prod" else "Developer"
cerberus_sp_key = f"ServicePrincipal_<DBX_WORKSPACE>_App.Data.{sp_role}"

conn_id   = "databricks_identity_sole_blazer"
conn_host = "https://<DBX_WORKSPACE>"


def get_cerberus_token(path: str, sp_key: str) -> str:
    from cerberus.client import CerberusClient
    client = CerberusClient("https://<CERBERUS_URL>")
    secrets = client.get_secrets_data(path)
    if sp_key not in secrets:
        raise ValueError(f"Key '{sp_key}' not found in SDB '{path}'")
    return secrets[sp_key]


def rotate_connection(**_):
    token = get_cerberus_token(cerberus_path, cerberus_sp_key)
    extra = json.dumps({"service_principal_sdb_key": cerberus_sp_key})
    session = Session()
    try:
        existing = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if existing:
            existing.conn_type = "databricks"
            existing.host      = conn_host
            existing.login     = "token"
            existing.password  = token
            existing.extra     = extra
            session.commit()
            log.info("Updated '%s' with the latest token.", conn_id)
        else:
            session.add(Connection(
                conn_id=conn_id, conn_type="databricks", host=conn_host,
                login="token", password=token, extra=extra,
                description="Databricks SP connection, rotated weekly",
            ))
            session.commit()
            log.info("Created '%s' with the latest token.", conn_id)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


with DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval="0 3 * * 2",     # Tuesdays 03:00 UTC
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
) as dag:

    t_start = BatchStartOperator(queue=default_queue, dag=dag,
                                 retries=5, retry_delay=timedelta(minutes=5))
    t_end   = BatchEndOperator(queue=default_queue, dag=dag)

    rotate = PythonOperator(task_id="rotate_sole_token", python_callable=rotate_connection, dag=dag)

    t_start >> rotate >> t_end
