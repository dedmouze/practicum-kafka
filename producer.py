#!/usr/bin/env python3
"""
Шаг 2 & 4. Продюсер с гарантией доставки At Least Once.
Отправляет сериализованные сообщения в Kafka (модель push).
"""
import time
import os
import signal
import sys
from confluent_kafka import Producer
from message_model import create_message

# ---- Конфигурация продюсера ----
producer_conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),  # Адрес брокера

    # At Least Once: acks='all' — ждём подтверждения от всех in-sync реплик.
    'acks': 'all',

    # Количество повторных попыток при временных сбоях.
    'retries': 5,

    # Задержка между повторными попытками (мс).
    'retry.backoff.ms': 500,

    # Идемпотентность: предотвращает дубликаты при retry.
    'enable.idempotence': True,

    # Размер клиентской очереди.
    'queue.buffering.max.messages': 10000,
}

producer = Producer(producer_conf)

# Флаг для graceful shutdown
running = True


def signal_handler(sig, frame):
    """Обработка Ctrl+C для корректного завершения."""
    global running
    print("\n⏹️  Завершение продюсера...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def delivery_report(err, msg):
    """
    Callback, вызываемый после доставки сообщения (или ошибки).
    При ошибке выводим в лог и продолжаем работу.
    """
    if err is not None:
        print(f"[Delivery Error] Сообщение не доставлено: {err}")
    else:
        print(f"[Delivery OK] Сообщение доставлено в {msg.topic()} "
              f"[партиция {msg.partition()}] @ offset {msg.offset()}")


def main():
    msg_id = 0
    topic = "my-topic"

    print("🚀 Продюсер запущен. Отправка сообщений...")

    while running:
        try:
            # Создаём объект сообщения
            message = create_message(
                msg_id=msg_id,
                payload=f"Данные #{msg_id}",
                source="producer"
            )

            # Сериализация в JSON
            value_bytes = message.to_json()
            key_bytes = str(message.id).encode('utf-8')

            # Вывод отправляемого сообщения в консоль (Шаг 3)
            print(f"📤 Отправка: {message}")

            # Асинхронная отправка (модель push)
            producer.produce(
                topic=topic,
                key=key_bytes,
                value=value_bytes,
                callback=delivery_report  # Обработка результата доставки
            )

            # Poll для обработки callback'ов (non-blocking)
            producer.poll(0)

            msg_id += 1
            time.sleep(0.5)  # Небольшая задержка между сообщениями

        except BufferError:
            print("[Warning] Локальная очередь продюсера переполнена, "
                  "ожидаем освобождения места...")
            producer.poll(1)  # Блокируемся на 1 секунду
        except Exception as e:
            print(f"[Error] Непредвиденная ошибка: {e}")
            time.sleep(1)

    # Ожидаем доставки всех сообщений перед выходом
    print("⏳ Ожидание доставки оставшихся сообщений...")
    remaining = producer.flush(timeout=30)
    if remaining > 0:
        print(f"[Warning] {remaining} сообщений не были доставлены.")
    print("✅ Продюсер остановлен.")


if __name__ == "__main__":
    main()