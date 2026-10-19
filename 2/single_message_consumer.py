#!/usr/bin/env python3
"""
Шаг 2. SingleMessageConsumer:
- Читает по одному сообщению за poll()
- Обрабатывает каждое сообщение
- Автоматически коммитит оффсет (enable.auto.commit=True)
- Уникальный group_id для параллельной работы
"""
import signal
import os
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException
from message_model import KafkaMessage

# ---- Конфигурация консьюмера ----
consumer_conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),

    # Уникальная группа для этого типа консьюмера (Шаг 2)
    'group.id': 'single-message-group',

    # Начинать с самого раннего сообщения, если нет сохранённого оффсета
    'auto.offset.reset': 'earliest',

    # АВТОМАТИЧЕСКИЙ коммит оффсета (Шаг 2)
    'enable.auto.commit': True,

    # Интервал автоматического коммита (мс)
    'auto.commit.interval.ms': 5000,

    # Максимум одно сообщение за poll() (по умолчанию и так 1,
    # но явно указываем для наглядности)
    'max.poll.records': 1,

    # Таймаут сессии (мс)
    'session.timeout.ms': 30000,
}

consumer = Consumer(consumer_conf)
running = True


def signal_handler(sig, frame):
    global running
    print("\n⏹️  Завершение SingleMessageConsumer...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def main():
    topic = "my-topic"
    consumer.subscribe([topic])
    print(f"🔊 SingleMessageConsumer подписан на '{topic}'. Ожидание сообщений...")

    try:
        while running:
            # poll() возвращает одно сообщение (max.poll.records=1)
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Конец партиции — не ошибка
                    print(f"📭 Достигнут конец партиции {msg.partition()}")
                    continue
                else:
                    # Логируем ошибку и продолжаем работу (Шаг 2)
                    print(f"[Consumer Error] {msg.error()}")
                    continue

            # ---- Десериализация (Шаг 3) ----
            try:
                value_bytes = msg.value()
                if value_bytes is None:
                    print("[Warning] Получено сообщение с пустым значением.")
                    continue

                kafka_message = KafkaMessage.from_json(value_bytes)

                if kafka_message is None:
                    print("[Deserialization Error] Пропускаем невалидное сообщение.")
                    continue

                # ---- Обработка сообщения ----
                print(f"📥 Получено (Single): {kafka_message}")
                print(f"   Метаданные: topic={msg.topic()}, "
                      f"partition={msg.partition()}, offset={msg.offset()}")

            except Exception as e:
                # Логируем ошибку и продолжаем (Шаг 2)
                print(f"[Processing Error] Ошибка обработки сообщения: {e}")
                continue

    except KafkaException as e:
        print(f"[Kafka Exception] {e}")
    finally:
        # Корректное закрытие консьюмера
        consumer.close()
        print("✅ SingleMessageConsumer остановлен.")


if __name__ == "__main__":
    main()