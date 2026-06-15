from confluent_kafka import Consumer

from config import Config


class RawLogConsumer:
    def __init__(self, config: Config) -> None:
        self._consumer = Consumer(
            {
                "bootstrap.servers": config.kafka_bootstrap_servers,
                "group.id": config.consumer_group_id,
                "auto.offset.reset": "earliest",
            }
        )
        self._consumer.subscribe([config.raw_topic])

    def poll(self, timeout_ms: int = 1000):
        message = self._consumer.poll(timeout=timeout_ms / 1000)
        if message is None:
            return None
        if message.error():
            raise RuntimeError(message.error().str())
        return message.value()

    def close(self) -> None:
        self._consumer.close()
