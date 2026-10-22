"""Receiver-side staleness guard for replayed messages.

The sender already applies a TTL, but old payloads, manual flushes, a raised
TTL, or mismatched client/server versions can still deliver a stale proactive
message. The receiver checks the age independently — the second half of the
two-sided TTL.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, Optional

OFFLINE_REPLAY_MAX_AGE_SECONDS = 10 * 60


def offline_replay_age(
    data: Dict[str, Any],
    *,
    now: Optional[float] = None,
    clock: Optional[Callable[[], float]] = None,
) -> Optional[float]:
    """Age in seconds for a replayed message; ``None`` when not a replay."""
    if not isinstance(data, dict) or not data.get("is_offline_replay"):
        return None
    queued_at = data.get("offline_queued_at")
    if queued_at is None:
        queued_at = data.get("timestamp")
    try:
        current = float(now if now is not None else (clock or time.time)())
        return max(0.0, current - float(queued_at))
    except (TypeError, ValueError):
        return None


def is_stale_offline_replay(
    data: Dict[str, Any],
    *,
    max_age_seconds: float = OFFLINE_REPLAY_MAX_AGE_SECONDS,
    now: Optional[float] = None,
    clock: Optional[Callable[[], float]] = None,
) -> bool:
    """Whether a replay is older than the accepted age (non-replays are fresh)."""
    age = offline_replay_age(data, now=now, clock=clock)
    return age is not None and age > float(max_age_seconds)


__all__ = [
    "OFFLINE_REPLAY_MAX_AGE_SECONDS",
    "offline_replay_age",
    "is_stale_offline_replay",
]
