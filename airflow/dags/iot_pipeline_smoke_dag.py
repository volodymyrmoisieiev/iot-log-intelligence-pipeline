from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="iot_pipeline_smoke_dag",
    description="Smoke test DAG for the local Airflow foundation.",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["stage-7a", "smoke-test"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_airflow_environment = BashOperator(
        task_id="check_airflow_environment",
        bash_command=(
            "echo 'Airflow DAG smoke test running'; "
            "python --version; "
            "airflow version"
        ),
    )

    finish = EmptyOperator(task_id="finish")

    start >> check_airflow_environment >> finish
