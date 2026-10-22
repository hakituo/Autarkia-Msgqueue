"""An in-memory offline message queue for WebSocket reconnects.

Semantics worth keeping:

- **Per-type TTL.** Proactive messages (goodnight, morning, status) are stale
  within minutes; ordinary messages can wait a day.
- **message_id dedupe.** Re-enqueuing the same outbound message never replays
  it twice.
- **Discard after delivery elsewhere.** If the same message reached the phone
  app, it must not be replayed to the desktop client.
- **Atomic replay.** Only successfully sent messages leave the queue; a failed
  send is pushed back to the front and stops the flush so order is preserved.
- **Per-user flush lock.** Two reconnect paths cannot pop the same deque.
- **Routing filter.** With two personas sharing one user id, a connection only
  takes messages addressed to it; others stay for the right connection.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

PROACTIVE_OFFLINE_TTL = 10 * 60
DEFAULT_OFFLINE_TTL = 24 * 3600
_PROACTIVE_MESSAGE_TYPES = frozenset({"proactive_message"})

Sender = Callable[[Dict[str, Any]], Awaitable[bool]]
Accepts = Callable[[Dict[str, Any]], bool]


@dataclass
class QueueConfig:
    default_ttl_seconds: float = DEFAULT_OFFLINE_TTL
    proactive_ttl_seconds: float = PROACTIVE_OFFLINE_TTL
    proactive_types: frozenset[str] = _PROACTIVE_MESSAGE_TYPES


@dataclass
class FlushResult:
    sent: int = 0
    expired: int = 0
    failed: int = 0
    skipped: int = 0
    remaining: int = 0

    @property
    def ok(self) -> bool:
        return self.failed == 0


class OfflineMessageQueue:
    """Per-user offline queues with atomic replay."""

    def __init__(
        self,
        config: Optional[QueueConfig] = None,
        *,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        self.config = config or QueueConfig()
        self._clock = clock or time.time
        self._queues: Dict[str, Deque[Tuple[float, Dict[str, Any]]]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._locks_loop: Optional[asyncio.AbstractEventLoop] = None

    # -- storage -----------------------------------------------------------
    def _ttl_for(self, message: Dict[str, Any]) -> float:
        msg_type = str(message.get("type") or "").strip().lower()
        is_proactive = msg_type in self.config.proactive_types or bool(
            message.get("is_proactive")
        )
        return (
            float(self.config.proactive_ttl_seconds)
            if is_proactive
            else float(self.config.default_ttl_seconds)
        )

    def store_offline_message(
        self, user_id: str, message: Dict[str, Any], *, now: Optional[float] = None
    ) -> bool:
        """Deep-copy and enqueue one message. Returns whether it was stored."""
        try:
            message_id = str(message.get("message_id") or "").strip()
            queue = self._queues.setdefault(user_id, deque())
            if message_id and any(
                str(queued.get("message_id") or "").strip() == message_id
                for _, queued in queue
            ):
                logger.debug(
                    "Duplicate offline message ignored for %s (message_id=%s)",
                    user_id,
                    message_id,
                )
                return False
            queue.append((float(now if now is not None else self._clock()), copy.deepcopy(message)))
            logger.info(
                "Stored offline message for %s, queue size: %s", user_id, len(queue)
            )
            return True
        except Exception as error:  # noqa: BLE001 - one bad payload must not break the queue
            logger.error("Failed to store offline message: %s", error)
            return False

    def discard_offline_message(self, user_id: str, message_id: str) -> int:
        """Remove every queued message with this id (delivered elsewhere)."""
        target = str(message_id or "").strip()
        queue = self._queues.get(user_id)
        if not target or queue is None:
            return 0
        kept = [
            (queued_at, message)
            for queued_at, message in queue
            if str(message.get("message_id") or "").strip() != target
        ]
        removed = len(queue) - len(kept)
        if removed:
            queue.clear()
            queue.extend(kept)
            if not queue:
                self._queues.pop(user_id, None)
            logger.info(
                "Discarded %s offline message(s) for %s (message_id=%s)",
                removed,
                user_id,
                target,
            )
        return removed

    def pending_count(self, user_id: str) -> int:
        return len(self._queues.get(user_id, ()))

    def pending_users(self) -> List[str]:
        return list(self._queues)

    def clear(self) -> None:
        self._queues.clear()

    # -- replay ------------------------------------------------------------
    def _get_lock(self, user_id: str) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        if self._locks_loop is not loop:
            self._locks = {}
            self._locks_loop = loop
        lock = self._locks.get(user_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[user_id] = lock
        return lock

    async def flush(
        self,
        user_id: str,
        sender: Sender,
        *,
        accepts: Optional[Accepts] = None,
        now: Optional[float] = None,
    ) -> FlushResult:
        """Deliver queued messages in order until one fails or the queue empties.

        Args:
            user_id: Queue owner.
            sender: ``async (payload) -> bool``; false means "retry later".
            accepts: Optional routing predicate; false leaves the message for
                another connection and continues scanning.
            now: Timestamp override (tests).
        """
        queue = self._queues.get(user_id)
        if not queue:
            return FlushResult()

        async with self._get_lock(user_id):
            queue = self._queues.get(user_id)
            if not queue:
                return FlushResult()

            result = FlushResult()
            current_time = float(now if now is not None else self._clock())
            scan_count = len(queue)

            for _ in range(scan_count):
                queued_at, message = queue.popleft()

                if current_time - queued_at >= self._ttl_for(message):
                    result.expired += 1
                    logger.info(
                        "Dropping expired offline message: type=%s age=%.1fmin",
                        message.get("type"),
                        (current_time - queued_at) / 60.0,
                    )
                    continue

                if accepts is not None and not accepts(message):
                    queue.append((queued_at, message))
                    result.skipped += 1
                    continue

                payload = copy.deepcopy(message)
                payload["is_offline_replay"] = True
                payload["offline_queued_at"] = queued_at

                try:
                    sent = await sender(payload)
                    if not sent:
                        raise ConnectionError("offline send returned false")
                except Exception as error:  # noqa: BLE001 - keep the message
                    queue.appendleft((queued_at, message))
                    result.failed += 1
                    logger.warning(
                        "Failed to flush offline message to %s: %s. Kept for retry.",
                        user_id,
                        error,
                    )
                    break

                result.sent += 1

            result.remaining = len(queue)
            if not queue:
                self._queues.pop(user_id, None)
            logger.info(
                "Offline flush done for %s: sent=%s expired=%s failed=%s "
                "skipped=%s remaining=%s",
                user_id,
                result.sent,
                result.expired,
                result.failed,
                result.skipped,
                result.remaining,
            )
            return result


__all__ = [
    "OfflineMessageQueue",
    "QueueConfig",
    "FlushResult",
    "PROACTIVE_OFFLINE_TTL",
    "DEFAULT_OFFLINE_TTL",
]
