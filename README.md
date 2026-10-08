# Домашнее задание: Apache Kafka на Python

Реализация взаимодействия с Apache Kafka через библиотеку `confluent-kafka-python`:
один продюсер, два типа консьюмеров, работающих параллельно в двух экземплярах каждый,
с гарантией доставки At Least Once и сериализацией сообщений в JSON.

---

## 📋 Содержание

- [Структура проекта](#-структура-проекта)
- [Архитектура](#-архитектура)
- [Ключевые параметры](#-ключевые-параметры)
- [Запуск](#-запуск)
- [Проверка](#-проверка)
- [Особенности реализации](#-особенности-реализации)
- [Требования](#-требования)

---

## 📁 Структура проекта

```
.
├── docker-compose.yml             # Кластер Kafka (2 брокера + Zookeeper) и приложения
├── Dockerfile                     # Образ для python-приложений
├── requirements.txt               # Зависимости Python
├── message_model.py               # Класс KafkaMessage + сериализация/десериализация
├── producer.py                    # Продюсер (push-модель, At Least Once)
├── single_message_consumer.py     # Консьюмер по одному сообщению (auto-commit)
├── batch_message_consumer.py      # Консьюмер пачками (manual commit)
├── create_topic.py                # Утилита создания топика (опционально)
├── topic.txt                      # Команды создания/описания топика
└── README.md                      # Этот файл
```

---

## 🏗️ Архитектура

### Компоненты

| Компонент | Файл | Назначение |
|---|---|---|
| **KafkaMessage** | `message_model.py` | Датакласс сообщения. JSON-сериализация в `to_json()`, десериализация в `from_json()`. |
| **Producer** | `producer.py` | Асинхронно отправляет сообщения (модель push). `acks='all'` + `retries=5` → At Least Once. |
| **SingleMessageConsumer** | `single_message_consumer.py` | Читает по одному сообщению, `enable.auto.commit=True`. |
| **BatchMessageConsumer** | `batch_message_consumer.py` | Читает пачки по 100 сообщений через `consume()`, `enable.auto.commit=False` + ручной `commit()`. |

### Схема потока

```
                    ┌────────────────┐
                    │  Zookeeper     │
                    └───────┬────────┘
                            │
             ┌──────────────┴──────────────┐
             │                             │
      ┌──────▼──────┐               ┌──────▼──────┐
      │  kafka-1    │◄──────────────┤  kafka-2    │
      │  (broker)   │  репликация   │  (broker)   │
      └──────┬──────┘               └──────┬──────┘
             │                             │
             └──────────────┬──────────────┘
                            │
                  ┌─────────▼─────────┐
                  │  Топик my-topic   │
                  │ 3 партиции, RF=2  │
                  └─────────┬─────────┘
                            │
       ┌────────────────────┼────────────────────┐
       │                    │                    │
 ┌─────▼─────┐        ┌─────▼─────┐        ┌─────▼─────┐
 │ producer  │        │  single-  │        │  batch-   │
 │  ×2       │        │ consumer  │        │ consumer  │
 │           │        │  ×2       │        │  ×2       │
 └───────────┘        └───────────┘        └───────────┘
```

### Группы консьюмеров

| Группа | Экземпляры | Особенность |
|---|---|---|
| `single-message-group` | 2 | Автоматический коммит, по одному сообщению за `poll()` |
| `batch-message-group`  | 2 | Ручной коммит один раз на пачку, `consume(num_messages=100)` |

Разные `group.id` позволяют обеим группам **независимо** читать одни и те же сообщения из топика.

---

## 🔧 Ключевые параметры

### Продюсер (`producer.py`)

| Параметр | Значение | Зачем |
|---|---|---|
| `acks` | `'all'` | Гарантия **At Least Once**: ждём подтверждения от всех in-sync реплик. |
| `retries` | `5` | Повторные попытки при временных сбоях брокера. |
| `retry.backoff.ms` | `500` | Пауза между retry. |
| `enable.idempotence` | `True` | Защита от дублей при повторной отправке. |
| `queue.buffering.max.messages` | `10000` | Размер локальной очереди продюсера. |
| `bootstrap.servers` | `$KAFKA_BOOTSTRAP_SERVERS` | В Docker: `kafka-1:9092,kafka-2:9092`. |

### SingleMessageConsumer

| Параметр | Значение | Зачем |
|---|---|---|
| `group.id` | `single-message-group` | Уникальная группа для параллельной работы. |
| `enable.auto.commit` | `True` | Автоматический коммит оффсета. |
| `auto.commit.interval.ms` | `5000` | Интервал автокоммита. |
| `max.poll.records` | `1` | Одно сообщение за один вызов `poll()`. |
| `auto.offset.reset` | `earliest` | Читать с самого начала, если оффсет не сохранён. |

### BatchMessageConsumer

| Параметр | Значение | Зачем |
|---|---|---|
| `group.id` | `batch-message-group` | Уникальная группа. |
| `enable.auto.commit` | `False` | **Ручной коммит** после обработки всей пачки. |
| `fetch.min.bytes` | `10240` | Минимальный объём данных для ответа брокера (накопление пачки). |
| `fetch.wait.max.ms` | `1000` | ⚠️ Внимание: в **librdkafka** параметр называется именно так (не `fetch.max.wait.ms` как в Java!). Максимальное ожидание накопления данных. |
| `max.poll.records` | `100` | Максимум записей за один вызов `consume()`. |
| Метод чтения | `consume(num_messages=100, timeout=1.0)` | Возвращает **список** сообщений (в отличие от `poll()`, который возвращает одно). |
| Метод коммита | `consumer.commit(asynchronous=False)` | Синхронный коммит после обработки всей пачки. |

---

## 🚀 Запуск

### 1. Запустить Docker Desktop

Убедиться, что Docker Desktop запущен и работает в режиме **Linux containers**.

Проверить:

```bash
docker version
```

### 2. Запустить кластер и приложения

Из корня проекта:

```bash
docker-compose up -d --build \
  --scale producer=2 \
  --scale single-consumer=2 \
  --scale batch-consumer=2
```

> `deploy.replicas: 2` в `docker-compose.yml` сработает только в Swarm-режиме.
> В обычном `docker-compose up` нужен флаг `--scale`.

Ожидаемо поднимутся 9 контейнеров:
- `zookeeper`
- `kafka-1`, `kafka-2`
- `producer-1`, `producer-2`
- `single-consumer-1`, `single-consumer-2`
- `batch-consumer-1`, `batch-consumer-2`

### 3. Создать топик `my-topic`

После того, как оба брокера станут `healthy`:

```bash
docker exec -it kafka-1 kafka-topics \
  --create \
  --topic my-topic \
  --bootstrap-server kafka-1:9092,kafka-2:9092 \
  --partitions 3 \
  --replication-factor 2
```

Ожидаемый ответ: `Created topic my-topic.`

### 4. Наблюдать за работой

```bash
docker-compose logs -f producer
docker-compose logs -f single-consumer
docker-compose logs -f batch-consumer
```

---

## ✅ Проверка

### 1. Топик создан правильно (3 партиции, RF=2)

```bash
docker exec -it kafka-1 kafka-topics \
  --describe \
  --topic my-topic \
  --bootstrap-server kafka-1:9092,kafka-2:9092
```

Ожидаемый вывод:

```
Topic: my-topic  PartitionCount: 3  ReplicationFactor: 2
    Partition: 0   Leader: 1   Replicas: 1,2   Isr: 1,2
    Partition: 1   Leader: 2   Replicas: 2,1   Isr: 2,1
    Partition: 2   Leader: 1   Replicas: 1,2   Isr: 1,2
```

### 2. Продюсер отправляет сообщения

```bash
docker-compose logs producer | head -50
```

Должны быть строки вида:

```
📤 Отправка: KafkaMessage(id=0, payload='Данные #0', timestamp='...', source='producer')
[Delivery OK] Сообщение доставлено в my-topic [партиция 0] @ offset 0
```

### 3. SingleMessageConsumer читает по одному сообщению

```bash
docker-compose logs single-consumer | head -20
```

Ожидаемо:

```
📥 Получено (Single): KafkaMessage(id=0, ...)
   Метаданные: topic=my-topic, partition=0, offset=0
```

### 4. BatchMessageConsumer читает пачки и коммитит один раз

```bash
docker-compose logs batch-consumer | head -120
```

Ожидаемо:

```
📦 Получена пачка из 100 сообщений. Начинаем обработку...
   📥 [1] KafkaMessage(id=...)
   ...
   📥 [100] KafkaMessage(id=...)
✅ Оффсет закоммичен после обработки 100 сообщений.
```

### 5. Две независимые consumer-группы

```bash
docker exec -it kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 --list
```

Ожидаемо:

```
single-message-group
batch-message-group
```

### 6. Параллельная работа в рамках одной группы

```bash
docker exec -it kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:9092 \
  --describe --group batch-message-group
```

В колонке `CONSUMER-ID` должны быть **разные** идентификаторы (2 экземпляра),
а партиции 0, 1, 2 — распределены между ними.

---

## 💡 Особенности реализации

### Сериализация / десериализация

Класс `KafkaMessage` (`message_model.py`):

- `to_json()` → `bytes` (JSON + UTF-8). Вызывается продюсером перед `produce()`.
- `from_json(bytes)` → `KafkaMessage | None`. Вызывается консьюмерами.
- Ошибки сериализации/десериализации ловятся и логируются, приложение продолжает работу.

### Обработка ошибок

Все консьюмеры в `try/except` вокруг десериализации и обработки сообщения:
при любой ошибке — `print()` в лог и `continue`, без падения.

### `poll()` vs `consume()` в confluent-kafka-python

Это ключевая ловушка при переходе с Java-клиента:

| Метод | Возвращает | Когда использовать |
|---|---|---|
| `consumer.poll(timeout)` | **одно** `Message` или `None` | SingleMessageConsumer |
| `consumer.consume(num_messages, timeout)` | **список** `Message` | BatchMessageConsumer |

### Разница имён параметров Java ↔ librdkafka

| Java Kafka | confluent-kafka-python (librdkafka) |
|---|---|
| `fetch.max.wait.ms` | `fetch.wait.max.ms` |
| `fetch.min.bytes` | `fetch.min.bytes` (совпадает) |

### Гарантия доставки At Least Once

Обеспечивается на стороне продюсера:
- `acks='all'` — ждём подтверждения от всех реплик;
- `retries=5` + `retry.backoff.ms=500` — повтор при сбое;
- `enable.idempotence=True` — защита от дублей.

В логах можно увидеть `[Delivery OK]` при успешной доставке и `[Delivery Error]`
при временных проблемах с брокером (например, если топик ещё не создан).

---

## 📦 Требования

- Docker Desktop (Linux containers)
- Docker Compose v2
- Python 3.11+ (только для локального запуска `create_topic.py`)
- Библиотека `confluent-kafka>=2.3.0` (устанавливается в образ автоматически)

---

## 🧹 Остановка

```bash
docker-compose down -v
```

Флаг `-v` удаляет volume'ы (очищает состояние Kafka/Zookeeper).

---

## 📝 Что проверяет преподаватель

1. **Топик**: 3 партиции, replication factor 2 — команда `kafka-topics --describe`.
2. **Продюсер**: работает в 2 экземплярах, отправляет JSON, `acks='all'` и `retries` в коде.
3. **SingleMessageConsumer**: `max.poll.records=1`, `enable.auto.commit=True`,
   отдельная `group.id` = `single-message-group`.
4. **BatchMessageConsumer**: `enable.auto.commit=False`, ручной `commit(asynchronous=False)`
   один раз на пачку, отдельная `group.id` = `batch-message-group`.
5. **Параллельность**: `kafka-consumer-groups --describe` показывает 2 консьюмера на группу.
6. **Сериализация**: класс `KafkaMessage`, вывод отправляемого и полученного сообщения в консоль.