from datetime import datetime, timezone

import faust


class Message(faust.Record, serializer="json"):
    """A message sent from one user to another."""

    sender_id: str
    recipient_id: str
    text: str
    timestamp: str = ""


class FilteredMessage(faust.Record, serializer="json"):
    """A message that passed blocking and censorship checks."""

    sender_id: str
    recipient_id: str
    text: str
    timestamp: str


class BlockUpdate(faust.Record, serializer="json"):
    """Adds or removes a user from a recipient's block list."""

    user_id: str
    blocked_user_id: str
    action: str


class BannedWordUpdate(faust.Record, serializer="json"):
    """Adds or removes a word from the censorship dictionary."""

    word: str
    action: str


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
