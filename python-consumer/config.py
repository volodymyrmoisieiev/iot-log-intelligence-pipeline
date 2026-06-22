import os
from dataclasses import dataclass


DEFAULT_BOOTSTRAP_SERVERS = "kafka:9092"
DEFAULT_RAW_TOPIC = "iot_raw_logs"
DEFAULT_PROCESSED_TOPIC = "iot_processed_logs"
DEFAULT_INVALID_TOPIC = "iot_invalid_logs"
DEFAULT_CONSUMER_GROUP_ID = "iot-python-consumer"
DEFAULT_CONSUMER_MAX_MESSAGES = 0
DEFAULT_CONSUMER_IDLE_TIMEOUT_SECONDS = 10
DEFAULT_CONSUMER_PROGRESS_INTERVAL = 1000


@dataclass(frozen=True)
class Config:
    kafka_bootstrap_servers: str
    raw_topic: str
    processed_topic: str
    invalid_topic: str
    consumer_group_id: str
    consumer_max_messages: int
    consumer_idle_timeout_seconds: int
    consumer_progress_interval: int


def load_config() -> Config:
    return Config(
        kafka_bootstrap_servers=_get_env("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
        raw_topic=_get_env("KAFKA_RAW_TOPIC", DEFAULT_RAW_TOPIC),
        processed_topic=_get_env("KAFKA_PROCESSED_TOPIC", DEFAULT_PROCESSED_TOPIC),
        invalid_topic=_get_env("KAFKA_INVALID_TOPIC", DEFAULT_INVALID_TOPIC),
        consumer_group_id=_get_env("CONSUMER_GROUP_ID", DEFAULT_CONSUMER_GROUP_ID),
        consumer_max_messages=_get_env_int("CONSUMER_MAX_MESSAGES", DEFAULT_CONSUMER_MAX_MESSAGES),
        consumer_idle_timeout_seconds=_get_env_int(
            "CONSUMER_IDLE_TIMEOUT_SECONDS",
            DEFAULT_CONSUMER_IDLE_TIMEOUT_SECONDS,
        ),
        consumer_progress_interval=_get_env_positive_int(
            "CONSUMER_PROGRESS_INTERVAL",
            DEFAULT_CONSUMER_PROGRESS_INTERVAL,
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


def _get_env_positive_int(key: str, default: int) -> int:
    value = _get_env_int(key, default)
    if value <= 0:
        raise ValueError(f"{key} must be greater than zero")
    return value
