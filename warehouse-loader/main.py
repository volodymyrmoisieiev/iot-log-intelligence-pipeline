import logging
import time
from dataclasses import dataclass

from confluent_kafka import Consumer, TopicPartition

from config import load_config
from db import WarehouseDatabase
from mapper import InvalidLogRow, ProcessedLogRow, map_invalid_message, map_processed_message
from progress import ProgressReporter


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("warehouse-loader")


@dataclass(frozen=True)
class PendingProcessedMessage:
    message: object
    row: ProcessedLogRow


@dataclass(frozen=True)
class PendingInvalidMessage:
    message: object
    row: InvalidLogRow


def commit_offsets(consumer: Consumer, messages: list[object]) -> None:
    offsets_by_partition: dict[tuple[str, int], TopicPartition] = {}

    for message in messages:
        topic = message.topic()
        partition = message.partition()
        committed_offset = message.offset() + 1
        key = (topic, partition)
        existing = offsets_by_partition.get(key)
        if existing is None or committed_offset > existing.offset:
            offsets_by_partition[key] = TopicPartition(topic, partition, committed_offset)

    if offsets_by_partition:
        consumer.commit(offsets=list(offsets_by_partition.values()), asynchronous=False)


def flush_batches(
    *,
    consumer: Consumer,
    database: WarehouseDatabase,
    processed_batch: list[PendingProcessedMessage],
    invalid_batch: list[PendingInvalidMessage],
    processed_count: int,
    invalid_count: int,
    failed_count: int,
    batch_size: int,
) -> tuple[int, int, int, int]:
    buffered_messages = len(processed_batch) + len(invalid_batch)
    if buffered_messages == 0:
        return processed_count, invalid_count, failed_count, 0

    processed_rows = [entry.row for entry in processed_batch]
    invalid_rows = [entry.row for entry in invalid_batch]
    buffered_kafka_messages = [entry.message for entry in processed_batch] + [
        entry.message for entry in invalid_batch
    ]

    try:
        database.insert_processed_batch(processed_rows)
        database.insert_invalid_batch(invalid_rows)
        database.commit()
        processed_count += len(processed_rows)
        invalid_count += len(invalid_rows)
    except Exception as exc:
        database.rollback()
        logger.warning(
            "batch insert failed for %s buffered messages; falling back to per-message inserts: %s",
            buffered_messages,
            exc,
        )
    else:
        try:
            commit_offsets(consumer, buffered_kafka_messages)
        except Exception as exc:
            failed_count += buffered_messages
            logger.exception("failed to commit Kafka offsets for flushed warehouse batch: %s", exc)
        logger.info(
            "flushed warehouse batch batch_messages=%s processed_batch=%s invalid_batch=%s batch_size=%s inserted_processed=%s inserted_invalid=%s failed=%s",
            buffered_messages,
            len(processed_rows),
            len(invalid_rows),
            batch_size,
            processed_count,
            invalid_count,
            failed_count,
        )
        return processed_count, invalid_count, failed_count, 1

    for entry in processed_batch:
        try:
            database.insert_processed(entry.row)
            database.commit()
            processed_count += 1
            try:
                commit_offsets(consumer, [entry.message])
            except Exception as exc:
                failed_count += 1
                logger.exception("failed to commit Kafka offset for processed message: %s", exc)
        except Exception as exc:
            database.rollback()
            failed_count += 1
            logger.exception("failed to process buffered processed message: %s", exc)

    for entry in invalid_batch:
        try:
            database.insert_invalid(entry.row)
            database.commit()
            invalid_count += 1
            try:
                commit_offsets(consumer, [entry.message])
            except Exception as exc:
                failed_count += 1
                logger.exception("failed to commit Kafka offset for invalid message: %s", exc)
        except Exception as exc:
            database.rollback()
            failed_count += 1
            logger.exception("failed to process buffered invalid message: %s", exc)

    logger.info(
        "completed fallback warehouse flush batch_messages=%s batch_size=%s inserted_processed=%s inserted_invalid=%s failed=%s",
        buffered_messages,
        batch_size,
        processed_count,
        invalid_count,
        failed_count,
    )
    return processed_count, invalid_count, failed_count, 1


def main() -> int:
    config = load_config()
    consumer = Consumer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "group.id": config.warehouse_loader_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    database = WarehouseDatabase(config)

    processed_count = 0
    invalid_count = 0
    failed_count = 0
    consumed_count = 0
    batches_flushed = 0
    last_message_at = time.monotonic()
    processed_batch: list[PendingProcessedMessage] = []
    invalid_batch: list[PendingInvalidMessage] = []
    progress = ProgressReporter(
        logger=logger,
        component_name="warehouse loader",
        interval=config.warehouse_loader_progress_interval,
        total=config.warehouse_loader_max_messages,
        unit="msg",
    )

    logger.info(
        "starting warehouse loader brokers=%s processed_topic=%s invalid_topic=%s postgres_host=%s postgres_port=%s postgres_db=%s group_id=%s max_messages=%s idle_timeout_seconds=%s progress_interval=%s batch_size=%s",
        config.kafka_bootstrap_servers,
        config.processed_topic,
        config.invalid_topic,
        config.postgres_host,
        config.postgres_port,
        config.postgres_db,
        config.warehouse_loader_group_id,
        config.warehouse_loader_max_messages,
        config.warehouse_loader_idle_timeout_seconds,
        config.warehouse_loader_progress_interval,
        config.warehouse_loader_batch_size,
    )

    try:
        consumer.subscribe([config.processed_topic, config.invalid_topic])
        database.connect()

        while True:
            if config.warehouse_loader_max_messages and consumed_count >= config.warehouse_loader_max_messages:
                processed_count, invalid_count, failed_count, flushed = flush_batches(
                    consumer=consumer,
                    database=database,
                    processed_batch=processed_batch,
                    invalid_batch=invalid_batch,
                    processed_count=processed_count,
                    invalid_count=invalid_count,
                    failed_count=failed_count,
                    batch_size=config.warehouse_loader_batch_size,
                )
                batches_flushed += flushed
                processed_batch.clear()
                invalid_batch.clear()
                logger.info("reached max messages limit: %s", config.warehouse_loader_max_messages)
                break

            message = consumer.poll(timeout=1.0)

            if message is None:
                idle_for = time.monotonic() - last_message_at
                if idle_for >= config.warehouse_loader_idle_timeout_seconds:
                    processed_count, invalid_count, failed_count, flushed = flush_batches(
                        consumer=consumer,
                        database=database,
                        processed_batch=processed_batch,
                        invalid_batch=invalid_batch,
                        processed_count=processed_count,
                        invalid_count=invalid_count,
                        failed_count=failed_count,
                        batch_size=config.warehouse_loader_batch_size,
                    )
                    batches_flushed += flushed
                    processed_batch.clear()
                    invalid_batch.clear()
                    if consumed_count == 0:
                        logger.info(
                            "no messages received before timeout; group_id=%s idle_timeout_seconds=%s",
                            config.warehouse_loader_group_id,
                            config.warehouse_loader_idle_timeout_seconds,
                        )
                    else:
                        logger.info("idle timeout reached after %.2f seconds", idle_for)
                    break
                continue

            if message.error():
                failed_count += 1
                logger.error("consumer error: %s", message.error().str())
                continue

            last_message_at = time.monotonic()
            consumed_count += 1

            try:
                if message.topic() == config.processed_topic:
                    row = map_processed_message(message.value())
                    processed_batch.append(PendingProcessedMessage(message=message, row=row))
                    logger.debug(
                        "buffered processed record device_id=%s buffered_processed=%s",
                        row.device_id,
                        len(processed_batch),
                    )
                elif message.topic() == config.invalid_topic:
                    row = map_invalid_message(message.value())
                    invalid_batch.append(PendingInvalidMessage(message=message, row=row))
                    logger.debug(
                        "buffered invalid record reason=%s buffered_invalid=%s",
                        row.error_reason,
                        len(invalid_batch),
                    )
                else:
                    raise RuntimeError(f"received message from unexpected topic {message.topic()}")
            except Exception as exc:
                failed_count += 1
                database.rollback()
                logger.exception("failed to process warehouse message: %s", exc)

            buffered_count = len(processed_batch) + len(invalid_batch)
            if buffered_count >= config.warehouse_loader_batch_size:
                processed_count, invalid_count, failed_count, flushed = flush_batches(
                    consumer=consumer,
                    database=database,
                    processed_batch=processed_batch,
                    invalid_batch=invalid_batch,
                    processed_count=processed_count,
                    invalid_count=invalid_count,
                    failed_count=failed_count,
                    batch_size=config.warehouse_loader_batch_size,
                )
                batches_flushed += flushed
                processed_batch.clear()
                invalid_batch.clear()
                buffered_count = 0

            progress.update(
                consumed_count,
                inserted_processed=processed_count,
                inserted_invalid=invalid_count,
                failed=failed_count,
                buffered=buffered_count,
                batches_flushed=batches_flushed,
                group_id=config.warehouse_loader_group_id,
            )
    finally:
        if database is not None and (processed_batch or invalid_batch):
            processed_count, invalid_count, failed_count, flushed = flush_batches(
                consumer=consumer,
                database=database,
                processed_batch=processed_batch,
                invalid_batch=invalid_batch,
                processed_count=processed_count,
                invalid_count=invalid_count,
                failed_count=failed_count,
                batch_size=config.warehouse_loader_batch_size,
            )
            batches_flushed += flushed
            processed_batch.clear()
            invalid_batch.clear()
        consumer.close()
        database.close()
        progress.close()
        logger.info(
            "warehouse loader summary consumed=%s inserted_processed=%s inserted_invalid=%s failed=%s batches_flushed=%s batch_size=%s max_messages=%s group_id=%s",
            consumed_count,
            processed_count,
            invalid_count,
            failed_count,
            batches_flushed,
            config.warehouse_loader_batch_size,
            config.warehouse_loader_max_messages,
            config.warehouse_loader_group_id,
        )

    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
