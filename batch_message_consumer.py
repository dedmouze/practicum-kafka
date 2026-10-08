#!/usr/bin/env python3
"""
Шаг 2. BatchMessageConsumer:
- Считывает МИНИМУМ 10 сообщений за один вызов consume()
- Обрабатывает сообщения в цикле
- ОДИН раз коммитит оффсет после обработки всей пачки (manual commit)
- Уникальный group_id для параллельной работы
"""
import signal
from confluent_kafka import Consumer, KafkaError, KafkaException
from message_model import KafkaMessage

# ---- Конфигурация консьюмера ----
consumer_conf = {
    'bootstrap.servers': 'kafka-1:9092,kafka-2:9092',

    # Уникальная группа для этого типа консьюмера (Шаг 2)
    'group.id': 'batch-message-group',

    'auto.offset.reset': 'earliest',

    # РУЧНОЕ управление коммитами (Шаг 2)
    'enable.auto.commit': False,

    # --- Настройки для накопления пачки ---
    # fetch.min.bytes: минимальный объём данных (в байтах),
    # который консьюмер ждёт от брокера за один запрос.
    'fetch.min.bytes': 10240,

    # ВАЖНО: в confluent-kafka-python (обёртка над librdkafka)
    # параметр называется fetch.wait.max.ms (а не fetch.max.wait.ms как в Java).
    # Максимальное время ожидания (мс), если данных меньше fetch.min.bytes.
    'fetch.wait.max.ms': 1000,

    # Максимум записей за один poll/consume.
    'max.poll.records': 100,

    'session.timeout.ms': 30000,
}

consumer = Consumer(consumer_conf)
running = True


def signal_handler(sig, frame):
    global running
    print("\n⏹️  Завершение BatchMessageConsumer...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def main():
    topic = "my-topic"
    consumer.subscribe([topic])
    print(f"🔊 BatchMessageConsumer подписан на '{topic}'. Ожидание пачек...")

    try:
        while running:
            # consume() возвращает СПИСОК сообщений (до num_messages штук).
            # В отличие от poll(), который всегда возвращает ОДНО сообщение!
            msgs = consumer.consume(num_messages=100, timeout=1.0)

            if not msgs:
                continue

            # Фильтруем ошибки
            valid_messages = []
            for msg in msgs:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        # Логируем ошибку и продолжаем (Шаг 2)
                        print(f"[Consumer Error] {msg.error()}")
                        continue
                valid_messages.append(msg)

            if not valid_messages:
                continue

            print(f"\n📦 Получена пачка из {len(valid_messages)} сообщений. "
                  f"Начинаем обработку...")

            # ---- Обработка всей пачки в цикле ----
            processed_count = 0
            for msg in valid_messages:
                try:
                    value_bytes = msg.value()
                    if value_bytes is None:
                        print("[Warning] Пустое сообщение, пропускаем.")
                        continue

                    kafka_message = KafkaMessage.from_json(value_bytes)

                    if kafka_message is None:
                        print("[Deserialization Error] Пропускаем невалидное сообщение.")
                        continue

                    # Вывод полученного сообщения в консоль (Шаг 3)
                    print(f"   📥 [{processed_count + 1}] {kafka_message} "
                          f"(partition={msg.partition()}, offset={msg.offset()})")
                    processed_count += 1

                except Exception as e:
                    # Логируем ошибку и продолжаем (Шаг 2)
                    print(f"[Processing Error] Ошибка обработки: {e}")
                    continue

            # ---- РУЧНОЙ КОММИТ ОФФСЕТА после обработки всей пачки (Шаг 2) ----
            # commit(asynchronous=False) — синхронный коммит.
            try:
                consumer.commit(asynchronous=False)
                print(f"✅ Оффсет закоммичен после обработки "
                      f"{processed_count} сообщений.\n")
            except KafkaException as e:
                print(f"[Commit Error] Не удалось закоммитить оффсет: {e}")

    except KafkaException as e:
        print(f"[Kafka Exception] {e}")
    finally:
        consumer.close()
        print("✅ BatchMessageConsumer остановлен.")


if __name__ == "__main__":
    main()