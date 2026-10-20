# Фильтрация сообщений с Kafka и Faust

Сервис принимает личные сообщения из Kafka, отбрасывает сообщения от
заблокированных отправителей, маскирует запрещённые слова и публикует результат
в отдельный топик.

## Топики Kafka

Используются следующие топики:

- `messages` — входящие сообщения;
- `filtered_messages` — сообщения, прошедшие фильтрацию;
- `blocked_users` — команды блокировки и разблокировки;
- `banned_words` — команды динамического изменения словаря цензуры.

Первые три топика требуются заданием. Дополнительный топик `banned_words`
позволяет обновлять словарь цензуры без остановки приложения.

## Классы и структура проекта

Классы сообщений находятся в `message_filter/models.py`:

- `Message` — входящее сообщение. Содержит `sender_id`, `recipient_id`, `text`
  и необязательное поле `timestamp`.
- `FilteredMessage` — сообщение после обработки. Содержит отправителя,
  получателя, очищенный текст и время сообщения.
- `BlockUpdate` — команда изменения блокировки. Поле `user_id` определяет
  владельца списка, `blocked_user_id` — блокируемого пользователя, а `action`
  принимает значение `block` или `unblock`.
- `BannedWordUpdate` — команда изменения словаря цензуры. Содержит слово и
  действие `add` или `remove`.

Остальные компоненты:

- `message_filter/app.py` создаёт Faust-приложение, топики, таблицы и три
  потоковых обработчика;
- `message_filter/censorship.py` содержит функцию `censor_text`, которая
  регистронезависимо заменяет запрещённые слова звёздочками и не изменяет
  совпадения внутри других слов;
- `tests/test_censorship.py` содержит unit-тесты функции цензуры;
- `docker-compose.yml` запускает Kafka, создаёт топики и запускает приложение.

## Логика работы

В `message_filter/app.py` работают три Faust Agent:

1. `update_blocked_users` читает команды из `blocked_users`. Для каждого
   `user_id` он хранит отдельный список заблокированных отправителей в таблице
   `blocked_users_by_recipient`.
2. `update_banned_words` читает команды из `banned_words` и обновляет глобальную
   таблицу `banned_words_dictionary`.
3. `filter_messages` читает `messages`. Если отправитель находится в списке
   блокировок получателя, сообщение отбрасывается. В противном случае текст
   проходит через `censor_text`, после чего результат публикуется в
   `filtered_messages`.

Если во входящем сообщении отсутствует `timestamp`, приложение подставляет
текущее время в UTC.

Таблицы используют RocksDB через Python-драйвер `rocksdict`. Локальные базы
хранятся в `/var/lib/message-filter`, подключённом к персистентному Docker
volume `app_data`. Кроме того, Faust создаёт для таблиц compacted
changelog-топики Kafka: если локальная база потеряна, состояние восстанавливается
из changelog.

Таким образом, RocksDB обеспечивает быстрое локальное персистентное состояние,
а Kafka — его резервное восстановление. В этой учебной конфигурации у каждого
топика одна партиция. Команды блокировки и сообщения необходимо отправлять с
ключом, равным ID владельца списка или получателя соответственно.

## Запуск

Требуются Docker и Docker Compose.

```bash
docker compose up --build -d
docker compose ps
docker compose logs -f app
```

Контейнер `kafka-init` автоматически создаёт все необходимые топики. Первый
запуск может занять около минуты, пока Kafka пройдёт healthcheck.

Остановка с сохранением данных:

```bash
docker compose down
```

Полное удаление контейнеров и сохранённого состояния:

```bash
docker compose down -v
```

## Ручное тестирование

Каждую команду producer ниже нужно запустить в отдельном терминале. После её
запуска вставьте указанные строки и завершите ввод сочетанием `Ctrl+D`.

### 1. Добавить блокировку

Пользователь `alice` блокирует `spammer`:

```bash
docker compose exec kafka kafka-console-producer --bootstrap-server kafka:29092 --topic blocked_users --property parse.key=true --property key.separator=:
docker compose exec kafka kafka-console-producer \
  --bootstrap-server kafka:29092 \
  --topic blocked_users \
  --property parse.key=true \
  --property key.separator=:
```

```text
alice:{"user_id":"alice","blocked_user_id":"spammer","action":"block"}
```

### 2. Добавить запрещённые слова

```bash
docker compose exec kafka kafka-console-producer --bootstrap-server kafka:29092 --topic banned_words --property parse.key=true --property key.separator=:
docker compose exec kafka kafka-console-producer \
  --bootstrap-server kafka:29092 \
  --topic banned_words \
  --property parse.key=true \
  --property key.separator=:
```

```text
bad:{"word":"bad","action":"add"}
rude:{"word":"rude","action":"add"}
```

Подождите 2–3 секунды, чтобы приложение применило обновления.

### 3. Запустить consumer результата

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic filtered_messages --from-beginning --property print.key=true --property key.separator=:
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic filtered_messages \
  --from-beginning \
  --property print.key=true \
  --property key.separator=:
```

### 4. Отправить тестовые сообщения

```bash
docker compose exec kafka kafka-console-producer --bootstrap-server kafka:29092 --topic messages --property parse.key=true --property key.separator=:
docker compose exec kafka kafka-console-producer \
  --bootstrap-server kafka:29092 \
  --topic messages \
  --property parse.key=true \
  --property key.separator=:
```

```text
alice:{"sender_id":"spammer","recipient_id":"alice","text":"You are rude","timestamp":"2026-10-19T10:00:00Z"}
alice:{"sender_id":"bob","recipient_id":"alice","text":"A bad and RUDE message","timestamp":"2026-10-19T10:01:00Z"}
charlie:{"sender_id":"bob","recipient_id":"charlie","text":"A normal message","timestamp":"2026-10-19T10:02:00Z"}
```

Ожидаемый результат:

- первое сообщение отсутствует, потому что `spammer` заблокирован пользователем
  `alice`;
- второе имеет текст `A *** and **** message`;
- третье проходит без изменений.

Пример выводимых JSON-значений (Faust может изменить порядок полей):

```text
alice:{"sender_id":"bob","recipient_id":"alice","text":"A *** and **** message","timestamp":"2026-10-19T10:01:00Z"}
charlie:{"sender_id":"bob","recipient_id":"charlie","text":"A normal message","timestamp":"2026-10-19T10:02:00Z"}
```

### Динамическое удаление правил

Для разблокировки отправьте в `blocked_users`:

```text
alice:{"user_id":"alice","blocked_user_id":"spammer","action":"unblock"}
```

Для удаления слова из словаря отправьте в `banned_words`:

```text
bad:{"word":"bad","action":"remove"}
```

## Unit-тесты

Локально, с Python 3.11:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest
```

Также можно выполнить тесты без локальной установки Python:

```bash
docker build --target test -t message-filter-test .
docker run --rm message-filter-test
```
