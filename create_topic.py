#!/usr/bin/env python3
"""
Шаг 1. Создание топика с 3 партициями и 2 репликами.
"""
import os

from confluent_kafka.admin import AdminClient, NewTopic

# Конфигурация подключения к Kafka
admin_conf = {
    # Локальный запуск (с хоста): localhost:29092,localhost:29093
    # Запуск внутри docker:           kafka-1:9092,kafka-2:9092
    'bootstrap.servers': os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:29092,localhost:29093"
    )
}

admin = AdminClient(admin_conf)

# Параметры топика:
# - num_partitions=3: три партиции для параллельной обработки
# - replication_factor=2: две реплики для отказоустойчивости
new_topic = NewTopic(
    "my-topic",
    num_partitions=3,
    replication_factor=2,
)

try:
    # Асинхронное создание топика
    futures = admin.create_topics([new_topic])
    for topic, future in futures.items():
        try:
            future.result()  # Ожидание завершения операции
            print(f"✅ Топик '{topic}' успешно создан (3 партиции, 2 реплики).")
        except Exception as e:
            print(f"❌ Ошибка создания топика '{topic}': {e}")
except Exception as e:
    print(f"❌ Общая ошибка: {e}")