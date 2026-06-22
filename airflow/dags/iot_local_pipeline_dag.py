from __future__ import annotations

import os
import shlex
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT_DIR = "/opt/project"
COMPOSE_FILE = f"{PROJECT_DIR}/docker-compose.yml"
COMPOSE_PROJECT_NAME = "iot_log_intelligence_pipeline"
RUN_ID_SAFE = (
    "{{ run_id | replace(':', '_') | replace('+', '_') | replace('.', '_') }}"
)
OBSERVABILITY_RUN_ID = f"airflow-observability-{RUN_ID_SAFE}"

DEFAULT_DATASET_PROFILE = "sample"
DEFAULT_PRODUCER_MAX_ROWS = "0"
DEFAULT_CONSUMER_MAX_MESSAGES = "72"
DEFAULT_WAREHOUSE_LOADER_MAX_MESSAGES = "72"
DEFAULT_CONSUMER_PROGRESS_INTERVAL = "1000"
DEFAULT_WAREHOUSE_LOADER_PROGRESS_INTERVAL = "1000"
DEFAULT_PRODUCER_SEND_DELAY_MS = "0"


def get_env_value(key: str, default: str) -> str:
    value = os.getenv(key, "").strip()
    return value or default


def format_compose_env(env_vars: dict[str, str]) -> str:
    return " ".join(
        f"-e {key}={shlex.quote(value)}" for key, value in env_vars.items()
    )


def compose_command(command: str) -> str:
    return dedent(
        f"""
        set -euo pipefail
        cd {PROJECT_DIR}
        docker compose -p {COMPOSE_PROJECT_NAME} -f {COMPOSE_FILE} {command}
        """
    ).strip()


def project_command(command: str) -> str:
    return dedent(
        f"""
        set -euo pipefail
        cd {PROJECT_DIR}
        {command}
        """
    ).strip()


DATASET_PROFILE = get_env_value("DATASET_PROFILE", DEFAULT_DATASET_PROFILE)
PRODUCER_MAX_ROWS = get_env_value("PRODUCER_MAX_ROWS", DEFAULT_PRODUCER_MAX_ROWS)
CONSUMER_MAX_MESSAGES = get_env_value(
    "CONSUMER_MAX_MESSAGES",
    DEFAULT_CONSUMER_MAX_MESSAGES,
)
WAREHOUSE_LOADER_MAX_MESSAGES = get_env_value(
    "WAREHOUSE_LOADER_MAX_MESSAGES",
    DEFAULT_WAREHOUSE_LOADER_MAX_MESSAGES,
)
CONSUMER_PROGRESS_INTERVAL = get_env_value(
    "CONSUMER_PROGRESS_INTERVAL",
    DEFAULT_CONSUMER_PROGRESS_INTERVAL,
)
WAREHOUSE_LOADER_PROGRESS_INTERVAL = get_env_value(
    "WAREHOUSE_LOADER_PROGRESS_INTERVAL",
    DEFAULT_WAREHOUSE_LOADER_PROGRESS_INTERVAL,
)
PRODUCER_SEND_DELAY_MS = get_env_value(
    "PRODUCER_SEND_DELAY_MS",
    DEFAULT_PRODUCER_SEND_DELAY_MS,
)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


dag_doc_md = """
# IoT Local Pipeline DAG

This DAG orchestrates the existing local IoT Log Intelligence Pipeline for demo runs.

It is designed for manual local execution only:

- no schedule
- no catchup
- safe reset of Kafka runtime state between runs
- warehouse-table truncation without touching Airflow metadata
- unique Kafka consumer group ids for each Airflow `run_id`
- default sample-safe dataset mode settings with optional medium-profile overrides through environment variables

The DAG does **not** start the Streamlit dashboard and does **not** add any cloud or production orchestration.
It uploads Spark device features only to local MinIO and does **not** use production AWS S3.
"""


with DAG(
    dag_id="iot_local_pipeline_dag",
    description="Manual local Airflow orchestration for repeatable IoT pipeline demo runs.",
    doc_md=dag_doc_md,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
    tags=["iot", "local", "airflow", "orchestration"],
) as dag:
    start = EmptyOperator(task_id="start")

    reset_local_pipeline_state = BashOperator(
        task_id="reset_local_pipeline_state",
        bash_command=compose_command(
            dedent(
                """
                stop kafka kafka-init kafka-ui || true
                rm -f kafka kafka-init kafka-ui || true
                """
            ).strip()
        ),
        execution_timeout=timedelta(minutes=5),
    )

    start_infrastructure = BashOperator(
        task_id="start_infrastructure",
        bash_command=compose_command("up -d kafka kafka-ui kafka-init postgres"),
        execution_timeout=timedelta(minutes=10),
    )

    truncate_warehouse_tables = BashOperator(
        task_id="truncate_warehouse_tables",
        bash_command=compose_command(
            dedent(
                """
                exec -T postgres bash -lc '
                PGPASSWORD="$POSTGRES_PASSWORD" \
                psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
                -c "TRUNCATE TABLE processed_iot_logs, invalid_iot_logs RESTART IDENTITY;"
                '
                """
            ).strip()
        ),
        execution_timeout=timedelta(minutes=5),
    )

    run_go_producer = BashOperator(
        task_id="run_go_producer",
        bash_command=compose_command(
            "run --rm "
            f"{format_compose_env({
                'DATASET_PROFILE': DATASET_PROFILE,
                'PRODUCER_MAX_ROWS': PRODUCER_MAX_ROWS,
                'PRODUCER_SEND_DELAY_MS': PRODUCER_SEND_DELAY_MS,
            })} "
            "go-producer"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    run_python_consumer = BashOperator(
        task_id="run_python_consumer",
        bash_command=compose_command(
            "run --rm "
            f"-e CONSUMER_GROUP_ID=airflow-consumer-{RUN_ID_SAFE} "
            f"{format_compose_env({
                'CONSUMER_MAX_MESSAGES': CONSUMER_MAX_MESSAGES,
                'CONSUMER_PROGRESS_INTERVAL': CONSUMER_PROGRESS_INTERVAL,
            })} "
            "python-consumer"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    run_warehouse_loader = BashOperator(
        task_id="run_warehouse_loader",
        bash_command=compose_command(
            "run --rm "
            f"-e WAREHOUSE_LOADER_GROUP_ID=airflow-loader-{RUN_ID_SAFE} "
            f"{format_compose_env({
                'WAREHOUSE_LOADER_MAX_MESSAGES': WAREHOUSE_LOADER_MAX_MESSAGES,
                'WAREHOUSE_LOADER_PROGRESS_INTERVAL': WAREHOUSE_LOADER_PROGRESS_INTERVAL,
            })} "
            "warehouse-loader"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    run_dbt_run = BashOperator(
        task_id="run_dbt_run",
        bash_command=compose_command("run --rm dbt dbt run"),
        execution_timeout=timedelta(minutes=10),
    )

    run_dbt_test = BashOperator(
        task_id="run_dbt_test",
        bash_command=compose_command("run --rm dbt dbt test"),
        execution_timeout=timedelta(minutes=10),
    )

    run_spark_device_features = BashOperator(
        task_id="run_spark_device_features",
        bash_command=compose_command(
            "run --rm spark-batch python /app/jobs/device_features_job.py"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    validate_spark_device_features_output = BashOperator(
        task_id="validate_spark_device_features_output",
        bash_command=project_command(
            dedent(
                """
                output_dir="data/processed/spark/device_features"

                if [ ! -d "$output_dir" ]; then
                    echo "Missing Spark output directory: $output_dir" >&2
                    exit 1
                fi

                if ! find "$output_dir" -maxdepth 1 -type f \\( -name 'part-*.parquet' -o -name '*.parquet' \\) | grep -q .; then
                    echo "No Parquet output files found in: $output_dir" >&2
                    exit 1
                fi

                echo "Validated Spark device features output in $output_dir"
                """
            ).strip()
        ),
        execution_timeout=timedelta(minutes=5),
    )

    start_object_storage = BashOperator(
        task_id="start_object_storage",
        bash_command=compose_command(
            "--profile object-storage up -d minio minio-init"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    upload_spark_features_to_minio = BashOperator(
        task_id="upload_spark_features_to_minio",
        bash_command=compose_command(
            "--profile object-storage run --build --rm object-storage-uploader"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    validate_minio_spark_features_upload = BashOperator(
        task_id="validate_minio_spark_features_upload",
        bash_command=compose_command(
            dedent(
                """
                --profile object-storage run --rm --entrypoint /bin/sh minio-init -ec '
                mc alias set local "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
                object_path="local/$MINIO_BUCKET/spark/device_features/latest/"
                objects="$(mc ls --recursive "$object_path" || true)"

                case "$objects" in
                    *".parquet"*)
                        echo "Validated uploaded Parquet objects under s3://$MINIO_BUCKET/spark/device_features/latest/"
                        ;;
                    *)
                        echo "No uploaded Parquet objects found under s3://$MINIO_BUCKET/spark/device_features/latest/" >&2
                        exit 1
                        ;;
                esac
                '
                """
            ).strip()
        ),
        execution_timeout=timedelta(minutes=5),
    )

    run_observability_writer = BashOperator(
        task_id="run_observability_writer",
        bash_command=compose_command(
            "run --build --rm observability-writer "
            f"--run-id {OBSERVABILITY_RUN_ID} "
            "--publish-alerts"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    validate_observability_output = BashOperator(
        task_id="validate_observability_output",
        bash_command=compose_command(
            dedent(
                f"""
                exec -T postgres bash -lc '
                observability_run_id="{OBSERVABILITY_RUN_ID}"
                audit_count=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A -v ON_ERROR_STOP=1 -c "SELECT COUNT(*) FROM pipeline_run_audit WHERE run_id = \$run_id\$$observability_run_id\$run_id\$;")
                quality_count=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A -v ON_ERROR_STOP=1 -c "SELECT COUNT(*) FROM pipeline_quality_checks WHERE run_id = \$run_id\$$observability_run_id\$run_id\$;")

                if [ "$audit_count" -ne 1 ]; then
                    echo "Expected exactly one pipeline_run_audit row for run_id=$observability_run_id, got $audit_count" >&2
                    exit 1
                fi

                if [ "$quality_count" -lt 2 ]; then
                    echo "Expected at least two pipeline_quality_checks rows for run_id=$observability_run_id, got $quality_count" >&2
                    exit 1
                fi

                echo "Validated observability output for run_id=$observability_run_id (audit_count=$audit_count, quality_count=$quality_count)"
                '
                """
            ).strip()
        ),
        execution_timeout=timedelta(minutes=5),
    )

    finish = EmptyOperator(task_id="finish")

    (
        start
        >> reset_local_pipeline_state
        >> start_infrastructure
        >> truncate_warehouse_tables
        >> run_go_producer
        >> run_python_consumer
        >> run_warehouse_loader
        >> run_dbt_run
        >> run_dbt_test
        >> run_spark_device_features
        >> validate_spark_device_features_output
        >> start_object_storage
        >> upload_spark_features_to_minio
        >> validate_minio_spark_features_upload
        >> run_observability_writer
        >> validate_observability_output
        >> finish
    )
