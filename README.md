# Autarkia-Msgqueue

**Reliable offline queues for WebSocket clients** — per-type TTL, message-id dedupe, discard-after-elsewhere-delivery, atomic replay, per-user flush locks, and a receiver-side staleness guard.

Extracted from the [Autarkia](https://qishihao.icu/projects/autarkia) agent runtime, where a phone app and a desktop bot shared one user id and kept re-delivering the same goodnight message. See the [component roadmap](https://github.com/hakituo/Autarkia-tool/blob/main/ROADMAP.md).

## Why

Storing messages for offline clients is easy. Not annoying the user is not:

- **Replayed after delivery.** The same proactive message reached the phone app, so the queued copy for the desktop client is a duplicate. It has to be discarded by id.
- **Overnight replays.** "Goodnight" is worthless at 9 a.m. Proactive messages need minutes-scale TTL, ordinary messages a day; one TTL for both is wrong either way.
- **Duplicates in queue.** The same `message_id` enqueued twice replays twice.
- **Lost on failure.** Popping before a successful send loses the message when the socket dies mid-flush.
- **Concurrent flushes.** Two reconnect paths can empty the same deque under each other's feet.
- **Wrong recipient.** With two personas sharing a user id, a connection must only take messages addressed to it.
- **Stale receivers.** A client with an old TTL, a manual flush, or a mismatched version can still receive a replay hours later, so the receiver checks age independently.

## Install

```bash
pip install autarkia-msgqueue
```

No dependencies. Python 3.10+.

## Quick start

```python
import asyncio
from autarkia_msgqueue import OfflineMessageQueue

queue = OfflineMessageQueue()

# The client is offline: park the message (deep-copied).
queue.store_offline_message("user-1", {"type": "proactive_message", "message_id": "evt-9", "text": "晚安"})

# Delivered elsewhere (phone app) — never replay it to the desktop client.
queue.discard_offline_message("user-1", "evt-9")

async def on_reconnect(user_id):
    async def send(payload) -> bool:
        return await websocket_send(user_id, payload)   # False / raise => retry later

    result = await queue.flush(user_id, send, accepts=lambda msg: msg.get("persona") == "a")
    print(result)   # FlushResult(sent=0, expired=0, failed=0, skipped=0, remaining=0)
```

### Receiver side

```python
from autarkia_msgqueue import is_stale_offline_replay

if is_stale_offline_replay(payload, max_age_seconds=600):
    return  # drop a replay that is too old to be meaningful
```

## Concepts

| API | Guarantee |
| --- | --- |
| `OfflineMessageQueue.store_offline_message` | deep copy; returns `False` for duplicates or bad payloads |
| `OfflineMessageQueue.discard_offline_message` | removes every copy of that `message_id`, returns the count |
| `OfflineMessageQueue.flush` | FIFO replay; stops at the first failure and keeps the rest |
| `QueueConfig` | `default_ttl_seconds`, `proactive_ttl_seconds`, `proactive_types` |
| `FlushResult` | `sent` / `expired` / `failed` / `skipped` / `remaining`, plus `.ok` |
| `offline_replay_age`, `is_stale_offline_replay` | receiver-side age check on replays |

Replayed payloads carry `is_offline_replay: True` and `offline_queued_at` (epoch seconds); the stored copy is never mutated.

## Design notes

- **TTL per message type.** `type == "proactive_message"` or `is_proactive` uses `proactive_ttl_seconds` (default 10 min); everything else uses `default_ttl_seconds` (default 24 h). Expiry is `age >= ttl`.
- **Atomic replay.** A message leaves the queue only after `sender` returns truthy. On failure it is pushed back to the front and the flush stops, so ordering survives a broken socket.
- **Scan window.** `flush` only walks the messages present when it takes the lock; anything enqueued mid-flush is left for the next round.
- **Routing predicate.** `accepts(payload)` returning `False` leaves the message queued and keeps scanning, so one connection never steals another persona's messages.
- **Locks are per user per event loop.** Locks are recreated when the queue is used from a different event loop, so a module-level singleton does not leak locks across `asyncio.run` calls.
- **In memory, by design.** The queue is a reconnect buffer, not a message store: a process restart drops it. Persist elsewhere if you need durability.
- **Deterministic tests.** `clock=` on the queue and `now=` on `store` / `flush` / the guard remove all wall-clock dependence.

## The bigger picture

- Project page: <https://qishihao.icu/projects/autarkia>
- Runtime repository: <https://github.com/hakituo/Autarkia>
- This package: <https://github.com/hakituo/Autarkia-Msgqueue>
- Roadmap: <https://github.com/hakituo/Autarkia-tool/blob/main/ROADMAP.md>
- Author: Shihao Qi — <https://qishihao.icu>

## License

MIT
