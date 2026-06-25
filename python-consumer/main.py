import logging
import time

from config import load_config
from consumer import RawLogConsumer
from progress import ProgressReporter
from producer import TopicProducer
from validator import build_invalid_record, validate_message


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("python-consumer")


def main() -> int:
    config = load_config()
    consumer = RawLogConsumer(config)
    producer = TopicProducer(config)

    processed_count = 0
    invalid_count = 0
    failed_count = 0
    consumed_count = 0
    last_message_at = time.monotonic()
    progress = ProgressReporter(
        logger=logger,
        component_name="consumer",
        interval=config.consumer_progress_interval,
        total=config.consumer_max_messages,
        unit="msg",
    )

    logger.info(
        "starting python consumer brokers=%s raw_topic=%s processed_topic=%s invalid_topic=%s group_id=%s max_messages=%s idle_timeout_seconds=%s progress_interval=%s",
        config.kafka_bootstrap_servers,
        config.raw_topic,
        config.processed_topic,
        config.invalid_topic,
        config.consumer_group_id,
        config.consumer_max_messages,
        config.consumer_idle_timeout_seconds,
        config.consumer_progress_interval,
    )

    try:
        while True:
            if config.consumer_max_messages and consumed_count >= config.consumer_max_messages:
                progress.prepare_log_line()
                logger.info("reached max messages limit: %s", config.consumer_max_messages)
                break

            message_value = consumer.poll(timeout_ms=1000)

            if message_value is None:
                idle_for = time.monotonic() - last_message_at
                if idle_for >= config.consumer_idle_timeout_seconds:
                    progress.prepare_log_line()
                    if consumed_count == 0:
                        logger.info(
                            "no messages received before timeout; group_id=%s idle_timeout_seconds=%s",
                            config.consumer_group_id,
                            config.consumer_idle_timeout_seconds,
                        )
                    else:
                        logger.info("idle timeout reached after %.2f seconds", idle_for)
                    break
                continue

            last_message_at = time.monotonic()
            consumed_count += 1

            validation = validate_message(message_value)

            if validation.is_valid and validation.normalized_record is not None:
                try:
                    producer.publish_processed(validation.normalized_record)
                    processed_count += 1
                    logger.debug(
                        "published valid record to processed topic device_id=%s count=%s",
                        validation.normalized_record.get("device_id"),
                        processed_count,
                    )
                except Exception as exc:
                    failed_count += 1
                    progress.prepare_log_line()
                    logger.exception("failed to publish valid record: %s", exc)
                progress.update(
                    consumed_count,
                    processed=processed_count,
                    invalid=invalid_count,
                    failed=failed_count,
                    group_id=config.consumer_group_id,
                )
                continue

            invalid_record = build_invalid_record(message_value, validation.error_reason or "unknown validation error")
            try:
                producer.publish_invalid(invalid_record)
                invalid_count += 1
                logger.debug(
                    "published invalid record to invalid topic reason=%s count=%s",
                    invalid_record["error_reason"],
                    invalid_count,
                )
            except Exception as exc:
                failed_count += 1
                progress.prepare_log_line()
                logger.exception("failed to publish invalid record: %s", exc)
            progress.update(
                consumed_count,
                processed=processed_count,
                invalid=invalid_count,
                failed=failed_count,
                group_id=config.consumer_group_id,
            )
    finally:
        consumer.close()
        producer.close()
        progress.close()
        progress.prepare_log_line()
        logger.info(
            "consumer summary consumed=%s processed=%s invalid=%s failed=%s max_messages=%s group_id=%s",
            consumed_count,
            processed_count,
            invalid_count,
            failed_count,
            config.consumer_max_messages,
            config.consumer_group_id,
        )

    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
