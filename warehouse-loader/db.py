from collections.abc import Sequence

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
            autocommit=False,
        )

    def insert_processed(self, record: ProcessedLogRow) -> None:
        connection = self._require_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                self._processed_insert_sql(),
                self._processed_insert_params(record),
            )

    def insert_processed_batch(self, records: Sequence[ProcessedLogRow]) -> None:
        if not records:
            return

        connection = self._require_connection()
        with connection.cursor() as cursor:
            cursor.executemany(
                self._processed_insert_sql(),
                [self._processed_insert_params(record) for record in records],
            )

    def insert_invalid(self, record: InvalidLogRow) -> None:
        connection = self._require_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                self._invalid_insert_sql(),
                self._invalid_insert_params(record),
            )

    def insert_invalid_batch(self, records: Sequence[InvalidLogRow]) -> None:
        if not records:
            return

        connection = self._require_connection()
        with connection.cursor() as cursor:
            cursor.executemany(
                self._invalid_insert_sql(),
                [self._invalid_insert_params(record) for record in records],
            )

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def commit(self) -> None:
        self._require_connection().commit()

    def rollback(self) -> None:
        self._require_connection().rollback()

    def _require_connection(self) -> Connection:
        if self._connection is None:
            raise RuntimeError("database connection has not been established")
        return self._connection

    @staticmethod
    def _processed_insert_sql() -> str:
        return """
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
        """

    @staticmethod
    def _processed_insert_params(record: ProcessedLogRow) -> tuple[object, ...]:
        return (
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
        )

    @staticmethod
    def _invalid_insert_sql() -> str:
        return """
            INSERT INTO invalid_iot_logs (
                raw_payload,
                error_reason,
                failed_at
            )
            VALUES (%s, %s, %s)
        """

    @staticmethod
    def _invalid_insert_params(record: InvalidLogRow) -> tuple[object, ...]:
        return (
            record.raw_payload,
            record.error_reason,
            record.failed_at,
        )
