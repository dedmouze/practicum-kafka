import logging
import os

import faust

from .censorship import censor_text
from .models import (
    BannedWordUpdate,
    BlockUpdate,
    FilteredMessage,
    Message,
    utc_now,
)

logger = logging.getLogger(__name__)

broker = os.getenv("KAFKA_BROKER", "localhost:9092")
datadir = os.getenv("FAUST_DATADIR", ".faust")

app = faust.App(
    "message-filter",
    broker=f"kafka://{broker}",
    store="rocksdb://",
    datadir=datadir,
    topic_partitions=1,
)

messages_topic = app.topic("messages", value_type=Message)
filtered_messages_topic = app.topic(
    "filtered_messages",
    value_type=FilteredMessage,
)
blocked_users_topic = app.topic("blocked_users", value_type=BlockUpdate)
banned_words_topic = app.topic("banned_words", value_type=BannedWordUpdate)

# RocksDB stores local table state on disk. Faust also writes every update to a
# compacted Kafka changelog, which can rebuild RocksDB if local data is lost.
blocked_users = app.Table(
    "blocked_users_by_recipient",
    default=list,
    partitions=1,
)
banned_words = app.GlobalTable(
    "banned_words_dictionary",
    default=bool,
    partitions=1,
)


@app.agent(blocked_users_topic)
async def update_blocked_users(updates):
    """Maintain one block list for each recipient."""
    async for update in updates:
        action = update.action.lower()
        current = set(blocked_users[update.user_id])

        if action == "block":
            current.add(update.blocked_user_id)
        elif action == "unblock":
            current.discard(update.blocked_user_id)
        else:
            logger.warning("Ignored unknown block action: %s", update.action)
            continue

        # Reassignment is required so Faust records the table change.
        blocked_users[update.user_id] = sorted(current)
        logger.info(
            "Block list updated: user=%s blocked=%s",
            update.user_id,
            sorted(current),
        )


@app.agent(banned_words_topic)
async def update_banned_words(updates):
    """Dynamically maintain the global censorship dictionary."""
    async for update in updates:
        word = update.word.strip().casefold()
        action = update.action.lower()
        if not word:
            logger.warning("Ignored an empty banned word")
        elif action == "add":
            banned_words[word] = True
        elif action == "remove":
            banned_words.pop(word, None)
        else:
            logger.warning("Ignored unknown banned-word action: %s", update.action)


@app.agent(messages_topic)
async def filter_messages(messages):
    """Drop blocked messages, censor all others, then publish the result."""
    async for message in messages:
        blocked = blocked_users[message.recipient_id]
        if message.sender_id in blocked:
            logger.info(
                "Message dropped: sender=%s recipient=%s",
                message.sender_id,
                message.recipient_id,
            )
            continue

        clean_text = censor_text(message.text, banned_words.keys())
        await filtered_messages_topic.send(
            key=message.recipient_id,
            value=FilteredMessage(
                sender_id=message.sender_id,
                recipient_id=message.recipient_id,
                text=clean_text,
                timestamp=message.timestamp or utc_now(),
            ),
        )


if __name__ == "__main__":
    app.main()
