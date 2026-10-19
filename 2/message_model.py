import json
from dataclasses import dataclass, asdict
from typing import Optional
from datetime import datetime


@dataclass
class KafkaMessage:
    """Класс сообщения для сериализации/десериализации."""
    id: int
    payload: str
    timestamp: str
    source: str

    def to_json(self) -> bytes:
        """Сериализация в JSON (bytes)."""
        try:
            return json.dumps(asdict(self)).encode('utf-8')
        except (TypeError, ValueError) as e:
            print(f"[Serialization Error] Не удалось сериализовать сообщение: {e}")
            raise

    @classmethod
    def from_json(cls, data: bytes) -> Optional['KafkaMessage']:
        """Десериализация из JSON (bytes)."""
        try:
            obj = json.loads(data.decode('utf-8'))
            return cls(
                id=obj['id'],
                payload=obj['payload'],
                timestamp=obj['timestamp'],
                source=obj['source']
            )
        except (json.JSONDecodeError, KeyError, UnicodeDecodeError) as e:
            print(f"[Deserialization Error] Не удалось десериализовать сообщение: {e}")
            return None

    def __str__(self):
        return (f"KafkaMessage(id={self.id}, payload='{self.payload}', "
                f"timestamp='{self.timestamp}', source='{self.source}')")


def create_message(msg_id: int, payload: str, source: str = "producer") -> KafkaMessage:
    """Фабрика для создания нового сообщения."""
    return KafkaMessage(
        id=msg_id,
        payload=payload,
        timestamp=datetime.utcnow().isoformat(),
        source=source
    )