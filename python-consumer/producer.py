import json

from confluent_kafka import Producer

from config import Config


class TopicProducer:
    def __init__(self, config: Config) -> None:
        self._producer = Producer({"bootstrap.servers": config.kafka_bootstrap_servers})
        self._processed_topic = config.processed_topic
        self._invalid_topic = config.invalid_topic

    def publish_processed(self, record: dict) -> None:
        device_id = record.get("device_id")
        self._produce(self._processed_topic, record, key=device_id)

    def publish_invalid(self, record: dict) -> None:
        self._produce(self._invalid_topic, record)

    def close(self) -> None:
        self._producer.flush()

    def _produce(self, topic: str, record: dict, key: str | None = None) -> None:
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8") if key else None,
            value=json.dumps(record).encode("utf-8"),
        )
        remaining = self._producer.flush(timeout=10)
        if remaining != 0:
            raise RuntimeError(f"failed to flush producer queue for topic {topic}")
