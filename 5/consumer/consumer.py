import json
import os
import signal
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException


OPERATION_NAMES = {
    "r": "SNAPSHOT",
    "c": "CREATE",
    "u": "UPDATE",
    "d": "DELETE",
}

running = True


def stop(_signum: int, _frame: Any) -> None:
    global running
    running = False


def describe_event(topic: str, raw_value: bytes) -> str:
    event = json.loads(raw_value.decode("utf-8"))
    # This also supports values produced with schemas.enable=true.
    payload = event.get("payload", event)
    source = payload.get("source") or {}
    operation = OPERATION_NAMES.get(payload.get("op"), payload.get("op", "UNKNOWN"))
    table = source.get("table", topic.rsplit(".", maxsplit=1)[-1])

    return json.dumps(
        {
            "operation": operation,
            "table": table,
            "before": payload.get("before"),
            "after": payload.get("after"),
            "timestamp_ms": payload.get("ts_ms"),
        },
        ensure_ascii=False,
        indent=2,
    )


def main() -> None:
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topics = os.getenv(
        "KAFKA_TOPICS",
        "postgres.public.users,postgres.public.orders",
    ).split(",")
    group_id = os.getenv("KAFKA_GROUP_ID", "postgres-cdc-python-consumer")

    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe(topics)
    print(f"Listening for CDC events in: {', '.join(topics)}", flush=True)

    try:
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() in (
                    KafkaError._PARTITION_EOF,
                    KafkaError.UNKNOWN_TOPIC_OR_PART,
                ):
                    continue
                raise KafkaException(message.error())
            if message.value() is None:
                # Debezium can emit tombstones after DELETE events.
                continue

            print(
                f"\nTopic: {message.topic()}, partition: {message.partition()}, "
                f"offset: {message.offset()}"
            )
            print(describe_event(message.topic(), message.value()), flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    main()
