# PostgreSQL CDC с Debezium, Kafka и мониторингом

Практическая работа демонстрирует передачу изменений из PostgreSQL в Apache
Kafka через Debezium, чтение CDC-событий на Python и мониторинг Kafka Connect с
помощью Prometheus и Grafana.

## Состав решения

- **PostgreSQL 16** — хранит таблицы `users` и `orders`. Для CDC включён
  `wal_level=logical`.
- **Apache Kafka** — принимает события изменений. Один брокер работает в
  KRaft-режиме без ZooKeeper.
- **Kafka Connect + Debezium** — читает PostgreSQL WAL через `pgoutput` и
  публикует изменения в Kafka.
- **connector-init** — автоматически регистрирует конфигурацию Debezium через
  Kafka Connect REST API.
- **Python consumer** — читает оба CDC-топика и выводит тип операции,
  состояние записи до и после изменения.
- **JMX Exporter** — экспортирует метрики Kafka Connect и Debezium на порту
  `9876`.
- **Prometheus** — собирает метрики Kafka Connect каждые пять секунд.
- **Grafana** — автоматически загружает источник Prometheus и dashboard для
  контроля состояния коннектора и передачи данных.

## Структура проекта

```text
5/
├── docker-compose.yaml
├── README.md
├── connector/
│   └── postgres-connector.json
├── consumer/
│   ├── consumer.py
│   ├── Dockerfile
│   └── requirements.txt
├── postgres/
│   └── init.sql
├── kafka-connect/
│   ├── Dockerfile
│   └── kafka-connect.yml
├── prometheus/
│   └── prometheus.yml
└── grafana/
    ├── dashboards/
    │   └── kafka-connect.json
    └── provisioning/
        ├── dashboards/dashboards.yml
        └── datasources/prometheus.yml
```

## Поток данных

```text
PostgreSQL WAL
      │
      ▼
Debezium PostgreSQL Connector ──► Kafka Connect JMX ──► Prometheus ──► Grafana
      │
      ▼
Apache Kafka
      ├── postgres.public.users
      └── postgres.public.orders
                    │
                    ▼
             Python consumer
```

1. PostgreSQL записывает `INSERT`, `UPDATE` и `DELETE` в WAL.
2. Debezium читает WAL из replication slot `debezium_shop`.
3. Коннектор создаёт отдельный Kafka-топик для каждой разрешённой таблицы.
4. Python-приложение десериализует Debezium envelope и выводит событие.
5. JMX Exporter предоставляет метрики Kafka Connect для Prometheus и Grafana.

## Настройка Debezium Connector

Конфигурация находится в `connector/postgres-connector.json`.

Основные параметры:

- `connector.class` — PostgreSQL Connector от Debezium;
- `database.*` — подключение к базе `shop`;
- `plugin.name=pgoutput` — встроенный logical decoding plugin PostgreSQL;
- `topic.prefix=postgres` — префикс создаваемых Kafka-топиков;
- `slot.name=debezium_shop` — replication slot;
- `publication.autocreate.mode=filtered` — publication создаётся только для
  разрешённых таблиц;
- `table.include.list=public.users,public.orders` — коннектор отслеживает
  **только** требуемые заданием таблицы;
- `snapshot.mode=initial` — при первом запуске существующие строки передаются
  как snapshot-события;
- JSON converters с `schemas.enable=false` делают сообщения удобными для чтения.

Конфигурация автоматически отправляется методом `PUT` на адрес:

```text
http://localhost:8083/connectors/postgres-cdc-connector/config
```

## Структура базы данных и исходные данные

Файл `postgres/init.sql` автоматически выполняется при создании PostgreSQL
volume. Он создаёт таблицы из задания:

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    product_name VARCHAR(100),
    quantity INT,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Туда же добавляются четыре пользователя и пять заказов из задания. После
регистрации коннектора Python-consumer покажет эти строки как операции
`SNAPSHOT`. Для обеих таблиц включён `REPLICA IDENTITY FULL`, поэтому события
`UPDATE` и `DELETE` содержат полное предыдущее состояние строки в поле
`before`.

Тестовые данные:

```sql
INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');
INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com');
INSERT INTO users (name, email) VALUES ('Alice Johnson', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob Brown', 'bob@example.com');

INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product A', 2);
INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product B', 1);
INSERT INTO orders (user_id, product_name, quantity) VALUES (2, 'Product C', 5);
INSERT INTO orders (user_id, product_name, quantity) VALUES (3, 'Product D', 3);
INSERT INTO orders (user_id, product_name, quantity) VALUES (4, 'Product E', 4);
```

## Запуск

Требуются Docker Engine или Docker Desktop с Docker Compose.

Перейдите в папку работы:

```bash
cd 5
```

Соберите и запустите все сервисы:

```bash
docker compose up --build -d
```

Первый запуск занимает несколько минут: Docker загружает образы и JMX Exporter.
Проверьте состояние:

```bash
docker compose ps
```

Сервисы `kafka`, `postgres` и `kafka-connect` должны иметь состояние
`healthy`. Контейнеры `kafka-init` и `connector-init` должны завершиться с
кодом `0` — это штатное поведение одноразовых инициализаторов.

Посмотреть общий процесс запуска:

```bash
docker compose logs -f kafka-connect connector-init consumer
```

Нажатие `Ctrl+C` останавливает только просмотр логов, но не контейнеры.

## Проверка коннектора

### 1. Проверить список коннекторов

Linux/macOS:

```bash
curl http://localhost:8083/connectors
```

Windows PowerShell:

```powershell
curl.exe http://localhost:8083/connectors
```

Ожидается:

```json
["postgres-cdc-connector"]
```

### 2. Проверить статус

```bash
curl http://localhost:8083/connectors/postgres-cdc-connector/status
```

В PowerShell используйте `curl.exe` вместо `curl`. Поля `connector.state` и
`tasks[0].state` должны иметь значение `RUNNING`:

```json
{
  "name": "postgres-cdc-connector",
  "connector": {
    "state": "RUNNING"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING"
    }
  ]
}
```

### 3. Убедиться, что созданы только нужные CDC-топики

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --list
```

Среди служебных топиков должны присутствовать:

```text
postgres.public.users
postgres.public.orders
```

Топиков для других таблиц быть не должно.

## Чтение событий на Python

Consumer запускается вместе с инфраструктурой. Его код находится в
`consumer/consumer.py`.

Показать события:

```bash
docker compose logs -f consumer
```

Для первоначальных строк вывод содержит события такого вида:

```text
Topic: postgres.public.users, partition: 0, offset: 0
{
  "operation": "SNAPSHOT",
  "table": "users",
  "before": null,
  "after": {
    "id": 1,
    "name": "John Doe",
    "email": "john@example.com"
  }
}
```

Значения операций:

- `SNAPSHOT` — строка получена во время начального снимка;
- `CREATE` — выполнен `INSERT`;
- `UPDATE` — выполнен `UPDATE`;
- `DELETE` — выполнен `DELETE`.

## Пошаговая проверка CDC

Откройте логи consumer в отдельном терминале:

```bash
docker compose logs -f consumer
```

В другом терминале выполните следующие команды.

### INSERT

```bash
docker compose exec postgres psql -U appuser -d shop -c \
  "INSERT INTO users (name, email) VALUES ('Test User', 'test@example.com');"
```

Consumer должен вывести операцию `CREATE` из таблицы `users`.

Добавьте заказ созданного пользователя:

```bash
docker compose exec postgres psql -U appuser -d shop -c \
  "INSERT INTO orders (user_id, product_name, quantity) VALUES (5, 'Product F', 2);"
```

Должна появиться операция `CREATE` из таблицы `orders`.

### UPDATE

```bash
docker compose exec postgres psql -U appuser -d shop -c \
  "UPDATE users SET email = 'updated@example.com' WHERE id = 5;"
```

Consumer выведет `UPDATE`: поле `before` содержит старую строку, а `after` —
обновлённую.

### DELETE

Сначала удалите связанный заказ, затем пользователя:

```bash
docker compose exec postgres psql -U appuser -d shop -c \
  "DELETE FROM orders WHERE user_id = 5;"
```

```bash
docker compose exec postgres psql -U appuser -d shop -c \
  "DELETE FROM users WHERE id = 5;"
```

В обоих топиках появятся операции `DELETE`.

Все команды выше можно выполнять в PowerShell одной строкой, убрав символ `\`,
который в примерах Bash обозначает перенос строки.

## Просмотр сырых сообщений Kafka

Для `users`:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic postgres.public.users \
  --from-beginning
```

Для `orders`:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic postgres.public.orders \
  --from-beginning
```

Завершить consumer можно сочетанием `Ctrl+C`.

## Мониторинг

### Prometheus

Откройте:

```text
http://localhost:9090
```

Статус сбора доступен на странице:

```text
http://localhost:9090/targets
```

Target `kafka-connect` должен иметь состояние `UP`.

Сырые метрики JMX Exporter:

```text
http://localhost:9876/metrics
```

Примеры запросов PromQL:

```promql
up{job="kafka-connect"}
```

```promql
kafka_connect_connector_task_status{connector="postgres-cdc-connector"}
```

```promql
sum(rate(kafka_connect_source_record_write_total{
  connector="postgres-cdc-connector"
}[1m]))
```

### Grafana

Откройте:

```text
http://localhost:3000
```

Учётные данные:

```text
login: admin
password: admin
```

Dashboard уже загружен в папку **Kafka Connect**:

```text
Kafka Connect / Debezium CDC
```

На нём отображаются:

- доступность Kafka Connect;
- состояние задачи Debezium;
- общее количество записей, переданных в Kafka;
- скорость чтения PostgreSQL и записи Kafka;
- число ошибок;
- среднее время обработки batch;
- количество записей в обработке;
- время с момента последнего CDC-события.

После выполнения тестовых SQL-команд графики обновятся в течение 5–10 секунд.

## Остановка и очистка

Остановить контейнеры, сохранив данные:

```bash
docker compose down
```

Полностью удалить контейнеры и все данные PostgreSQL, Kafka, Prometheus и
Grafana:

```bash
docker compose down -v --remove-orphans
```

Для повторения теста с чистого состояния используйте второй вариант, затем:

```bash
docker compose up --build -d
```

## Устранение проблем

Проверить логи коннектора:

```bash
docker compose logs kafka-connect
```

Проверить ответ инициализатора:

```bash
docker compose logs connector-init
```

Проверить конфигурацию, которую принял Kafka Connect:

```bash
curl http://localhost:8083/connectors/postgres-cdc-connector/config
```

Если нужно полностью повторить initial snapshot, недостаточно только
перезапустить контейнеры: удалите volumes командой `docker compose down -v`,
чтобы очистить offsets Kafka Connect и replication slot PostgreSQL.
