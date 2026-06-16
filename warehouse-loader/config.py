import os
from dataclasses import dataclass


DEFAULT_BOOTSTRAP_SERVERS = "kafka:9092"
DEFAULT_PROCESSED_TOPIC = "iot_processed_logs"
DEFAULT_INVALID_TOPIC = "iot_invalid_logs"
DEFAULT_POSTGRES_DB = "iot_logs"
DEFAULT_POSTGRES_USER = "iot_user"
DEFAULT_POSTGRES_PASSWORD = "iot_password"
DEFAULT_POSTGRES_HOST = "postgres"
DEFAULT_POSTGRES_PORT = 5432
DEFAULT_WAREHOUSE_LOADER_GROUP_ID = "iot-warehouse-loader"
DEFAULT_WAREHOUSE_LOADER_MAX_MESSAGES = 0
DEFAULT_WAREHOUSE_LOADER_IDLE_TIMEOUT_SECONDS = 10


@dataclass(frozen=True)
class Config:
    kafka_bootstrap_servers: str
    processed_topic: str
    invalid_topic: str
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: int
    warehouse_loader_group_id: str
    warehouse_loader_max_messages: int
    warehouse_loader_idle_timeout_seconds: int


def load_config() -> Config:
    return Config(
        kafka_bootstrap_servers=_get_env("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
        processed_topic=_get_env("KAFKA_PROCESSED_TOPIC", DEFAULT_PROCESSED_TOPIC),
        invalid_topic=_get_env("KAFKA_INVALID_TOPIC", DEFAULT_INVALID_TOPIC),
        postgres_db=_get_env("POSTGRES_DB", DEFAULT_POSTGRES_DB),
        postgres_user=_get_env("POSTGRES_USER", DEFAULT_POSTGRES_USER),
        postgres_password=_get_env("POSTGRES_PASSWORD", DEFAULT_POSTGRES_PASSWORD),
        postgres_host=_get_env("POSTGRES_HOST", DEFAULT_POSTGRES_HOST),
        postgres_port=_get_env_int("POSTGRES_PORT", DEFAULT_POSTGRES_PORT),
        warehouse_loader_group_id=_get_env(
            "WAREHOUSE_LOADER_GROUP_ID",
            DEFAULT_WAREHOUSE_LOADER_GROUP_ID,
        ),
        warehouse_loader_max_messages=_get_env_int(
            "WAREHOUSE_LOADER_MAX_MESSAGES",
            DEFAULT_WAREHOUSE_LOADER_MAX_MESSAGES,
        ),
        warehouse_loader_idle_timeout_seconds=_get_env_int(
            "WAREHOUSE_LOADER_IDLE_TIMEOUT_SECONDS",
            DEFAULT_WAREHOUSE_LOADER_IDLE_TIMEOUT_SECONDS,
        ),
    )


def _get_env(key: str, default: str) -> str:
    value = os.getenv(key, "").strip()
    return value or default


def _get_env_int(key: str, default: int) -> int:
    raw_value = os.getenv(key, "").strip()
    if not raw_value:
        return default

    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer") from exc

    if value < 0:
        raise ValueError(f"{key} must be zero or greater")

    return value
