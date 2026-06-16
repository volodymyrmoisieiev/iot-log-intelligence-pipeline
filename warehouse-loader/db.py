from psycopg import Connection, connect
from psycopg.types.json import Jsonb

from config import Config
from mapper import InvalidLogRow, ProcessedLogRow


class WarehouseDatabase:
    def __init__(self, config: Config) -> None:
        self._config = config
        self._connection: Connection | None = None

    def connect(self) -> None:
        self._connection = connect(
            dbname=self._config.postgres_db,
            user=self._config.postgres_user,
            password=self._config.postgres_password,
            host=self._config.postgres_host,
            port=self._config.postgres_port,
            connect_timeout=10,
            autocommit=True,
        )

    def insert_processed(self, record: ProcessedLogRow) -> None:
        connection = self._require_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO processed_iot_logs (
                    event_timestamp,
                    device_id,
                    source_ip,
                    destination_ip,
                    protocol,
                    packet_size,
                    duration_ms,
                    event_type,
                    attack_type,
                    status,
                    ingestion_timestamp,
                    processed_at,
                    raw_payload
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    record.event_timestamp,
                    record.device_id,
                    record.source_ip,
                    record.destination_ip,
                    record.protocol,
                    record.packet_size,
                    record.duration_ms,
                    record.event_type,
                    record.attack_type,
                    record.status,
                    record.ingestion_timestamp,
                    record.processed_at,
                    Jsonb(record.raw_payload),
                ),
            )

    def insert_invalid(self, record: InvalidLogRow) -> None:
        connection = self._require_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO invalid_iot_logs (
                    raw_payload,
                    error_reason,
                    failed_at
                )
                VALUES (%s, %s, %s)
                """,
                (
                    record.raw_payload,
                    record.error_reason,
                    record.failed_at,
                ),
            )

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def _require_connection(self) -> Connection:
        if self._connection is None:
            raise RuntimeError("database connection has not been established")
        return self._connection
