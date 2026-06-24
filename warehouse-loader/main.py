import logging
import time

from confluent_kafka import Consumer

from config import load_config
from db import WarehouseDatabase
from mapper import map_invalid_message, map_processed_message
from progress import ProgressReporter


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("warehouse-loader")


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
    last_message_at = time.monotonic()
    progress = ProgressReporter(
        logger=logger,
        component_name="warehouse loader",
        interval=config.warehouse_loader_progress_interval,
        total=config.warehouse_loader_max_messages,
        unit="msg",
    )

    logger.info(
        "starting warehouse loader brokers=%s processed_topic=%s invalid_topic=%s postgres_host=%s postgres_port=%s postgres_db=%s group_id=%s max_messages=%s idle_timeout_seconds=%s progress_interval=%s",
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
    )

    try:
        consumer.subscribe([config.processed_topic, config.invalid_topic])
        database.connect()

        while True:
            if config.warehouse_loader_max_messages and consumed_count >= config.warehouse_loader_max_messages:
                logger.info("reached max messages limit: %s", config.warehouse_loader_max_messages)
                break

            message = consumer.poll(timeout=1.0)

            if message is None:
                idle_for = time.monotonic() - last_message_at
                if idle_for >= config.warehouse_loader_idle_timeout_seconds:
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
                    database.insert_processed(row)
                    processed_count += 1
                    logger.debug(
                        "inserted processed record device_id=%s count=%s",
                        row.device_id,
                        processed_count,
                    )
                elif message.topic() == config.invalid_topic:
                    row = map_invalid_message(message.value())
                    database.insert_invalid(row)
                    invalid_count += 1
                    logger.debug(
                        "inserted invalid record reason=%s count=%s",
                        row.error_reason,
                        invalid_count,
                    )
                else:
                    raise RuntimeError(f"received message from unexpected topic {message.topic()}")

                consumer.commit(message=message, asynchronous=False)
            except Exception as exc:
                failed_count += 1
                logger.exception("failed to process warehouse message: %s", exc)
            progress.update(
                consumed_count,
                inserted_processed=processed_count,
                inserted_invalid=invalid_count,
                failed=failed_count,
                group_id=config.warehouse_loader_group_id,
            )
    finally:
        consumer.close()
        database.close()
        progress.close()
        logger.info(
            "warehouse loader summary consumed=%s inserted_processed=%s inserted_invalid=%s failed=%s max_messages=%s group_id=%s",
            consumed_count,
            processed_count,
            invalid_count,
            failed_count,
            config.warehouse_loader_max_messages,
            config.warehouse_loader_group_id,
        )

    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
