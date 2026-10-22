"""Autarkia-Msgqueue: reliable offline message queue for WebSocket reconnects."""

from .queue import (
    DEFAULT_OFFLINE_TTL,
    PROACTIVE_OFFLINE_TTL,
    FlushResult,
    OfflineMessageQueue,
    QueueConfig,
)
from .replay_guard import (
    OFFLINE_REPLAY_MAX_AGE_SECONDS,
    is_stale_offline_replay,
    offline_replay_age,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "OfflineMessageQueue",
    "QueueConfig",
    "FlushResult",
    "DEFAULT_OFFLINE_TTL",
    "PROACTIVE_OFFLINE_TTL",
    "OFFLINE_REPLAY_MAX_AGE_SECONDS",
    "offline_replay_age",
    "is_stale_offline_replay",
]
