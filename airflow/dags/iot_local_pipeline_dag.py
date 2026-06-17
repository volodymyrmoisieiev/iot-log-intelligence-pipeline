from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT_DIR = "/opt/project"
COMPOSE_FILE = f"{PROJECT_DIR}/docker-compose.yml"
COMPOSE_PROJECT_NAME = "iot_log_intelligence_pipeline"


def compose_command(command: str) -> str:
    return (
        "set -euo pipefail; "
        f"cd {PROJECT_DIR}; "
        f"docker compose -p {COMPOSE_PROJECT_NAME} -f {COMPOSE_FILE} {command}"
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="iot_local_pipeline_dag",
    description="Manual local Airflow orchestration for the IoT pipeline services.",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
    tags=["stage-7b", "local-orchestration"],
) as dag:
    start = EmptyOperator(task_id="start")

    start_infrastructure = BashOperator(
        task_id="start_infrastructure",
        bash_command=compose_command("up -d kafka kafka-ui kafka-init postgres"),
    )

    run_go_producer = BashOperator(
        task_id="run_go_producer",
        bash_command=compose_command(
            "run --rm -e PRODUCER_SEND_DELAY_MS=0 go-producer"
        ),
        retries=0,
    )

    run_python_consumer = BashOperator(
        task_id="run_python_consumer",
        bash_command=compose_command(
            "run --rm "
            "-e CONSUMER_GROUP_ID=airflow-pipeline-run "
            "-e CONSUMER_MAX_MESSAGES=72 "
            "python-consumer"
        ),
    )

    run_warehouse_loader = BashOperator(
        task_id="run_warehouse_loader",
        bash_command=compose_command(
            "run --rm "
            "-e WAREHOUSE_LOADER_GROUP_ID=airflow-loader-run "
            "-e WAREHOUSE_LOADER_MAX_MESSAGES=72 "
            "warehouse-loader"
        ),
    )

    run_dbt_run = BashOperator(
        task_id="run_dbt_run",
        bash_command=compose_command("run --rm dbt dbt run"),
    )

    run_dbt_test = BashOperator(
        task_id="run_dbt_test",
        bash_command=compose_command("run --rm dbt dbt test"),
    )

    finish = EmptyOperator(task_id="finish")

    (
        start
        >> start_infrastructure
        >> run_go_producer
        >> run_python_consumer
        >> run_warehouse_loader
        >> run_dbt_run
        >> run_dbt_test
        >> finish
    )
