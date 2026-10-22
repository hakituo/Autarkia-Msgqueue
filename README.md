<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/hakituo/Autarkia-Msgqueue/main/autarkia-dark.svg">
    <img src="https://raw.githubusercontent.com/hakituo/Autarkia-Msgqueue/main/autarkia.svg" alt="Autarkia" width="300">
  </picture>
</p>

<p align="center">
  <b>Autarkia-Msgqueue</b><br>
  <sub>WebSocket 离线消息可靠性队列 · Reliable offline queues for WebSocket clients</sub>
</p>

<p align="center">
  <a href="https://pypi.org/project/Autarkia-Msgqueue/"><img alt="PyPI" src="https://img.shields.io/pypi/v/autarkia-msgqueue.svg"></a>
  <a href="https://pypi.org/project/Autarkia-Msgqueue/"><img alt="Python" src="https://img.shields.io/pypi/pyversions/autarkia-msgqueue.svg"></a>
  <a href="https://github.com/hakituo/Autarkia-Msgqueue/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/hakituo/Autarkia-Msgqueue/actions/workflows/ci.yml/badge.svg"></a>
  <a href="LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-blue.svg"></a>
</p>

<p align="center">
  <a href="#中文">简体中文</a> · <a href="#english">English</a>
</p>

---

## 中文

**按类型分档的 TTL、`message_id` 去重、已在别端投递后丢弃、原子重放、per-user flush 锁、接收端时效守卫**——把「客户端离线时暂存消息」这件小事做对。

来自 [Autarkia](https://qishihao.icu/projects/autarkia) agent 运行时：手机 App 和桌面 bot 共用一个 user id，同一条「晚安」被反复投递。整个系列见[组件库路线图](https://github.com/hakituo/Autarkia-tool/blob/main/ROADMAP.md)。

### 为什么

存消息很容易，不烦人很难：

- **已经投递过还会重放**：主动消息已经送到手机 App，队列里给桌面端留的那份就是重复打扰，必须能按 id 丢掉。
- **隔夜重放**：「晚安」第二天早上九点再发毫无意义。主动消息要分钟级 TTL，普通消息要一天——一个 TTL 两边都不对。
- **队列内重复**：同一个 `message_id` 入队两次就会重放两次。
- **失败即丢**：先出队再发送，socket 一断消息就没了。
- **并发 flush**：两条重连路径同时掏同一个 deque。
- **投错人**：两个人设共用一个 user id 时，一条连接只能取走发给自己的消息。
- **接收端也过期**：客户端 TTL 调大、手工重放、版本不一致，都可能让旧消息晚到，所以接收端独立再查一次年龄。

### 安装

```bash
pip install autarkia-msgqueue
```

零依赖，Python 3.10+。

### 快速开始

```python
import asyncio
from autarkia_msgqueue import OfflineMessageQueue

queue = OfflineMessageQueue()

# 客户端离线：入队（深拷贝）
queue.store_offline_message(
    "user-1",
    {"type": "proactive_message", "message_id": "evt-9", "text": "晚安"},
)

# 已在别处（手机 App）投递过：不要再重放给桌面端
queue.discard_offline_message("user-1", "evt-9")

async def on_reconnect(user_id):
    async def send(payload) -> bool:
        return await websocket_send(user_id, payload)   # 返回假值/抛异常 = 稍后重试

    result = await queue.flush(user_id, send, accepts=lambda m: m.get("persona") == "a")
    print(result)   # FlushResult(sent=0, expired=0, failed=0, skipped=0, remaining=0)
```

接收端守卫：

```python
from autarkia_msgqueue import is_stale_offline_replay

if is_stale_offline_replay(payload, max_age_seconds=600):
    return  # 太旧的重放直接丢
```

### 接口一览

| API | 保证 |
| --- | --- |
| `store_offline_message` | 深拷贝入队；重复或非法载荷返回 `False` |
| `discard_offline_message` | 删掉该 `message_id` 的全部副本，返回删除条数 |
| `flush` | FIFO 重放；遇到第一条失败即停止，其余保留 |
| `QueueConfig` | `default_ttl_seconds`、`proactive_ttl_seconds`、`proactive_types` |
| `FlushResult` | `sent` / `expired` / `failed` / `skipped` / `remaining`，以及 `.ok` |
| `offline_replay_age`、`is_stale_offline_replay` | 接收端对重放消息的年龄判断 |

重放载荷会带 `is_offline_replay: True` 与 `offline_queued_at`（epoch 秒）；队列里的原始消息不会被改写。

### 设计说明

- **TTL 按消息类型**：`type == "proactive_message"` 或 `is_proactive` 走 `proactive_ttl_seconds`（默认 10 分钟），其余走 `default_ttl_seconds`（默认 24 小时）。过期判据是 `age >= ttl`。
- **原子重放**：只有 `sender` 返回真值后消息才出队；失败则推回队首并停止本轮，顺序在 socket 抖动后仍然保持。
- **扫描窗口**：`flush` 只遍历拿到锁时已存在的消息，重放期间新入队的留给下一轮。
- **路由谓词**：`accepts(payload)` 返回 `False` 时消息留在队列继续扫描，一条连接不会偷走另一个人设的消息。
- **锁按 user + 事件循环**：队列被另一个事件循环使用时锁会重建，模块级单例不会把锁漏到 `asyncio.run` 之间。
- **刻意只在内存里**：它是重连缓冲，不是消息存储；进程重启即丢，要持久化请自己做。
- **测试可确定性**：队列的 `clock=`、`store` / `flush` 的 `now=`、守卫的 `now=` 都能注入，不依赖墙钟。

### 系列与链接

- 项目页：<https://qishihao.icu/projects/autarkia>
- 运行时仓库：<https://github.com/hakituo/Autarkia>
- 本包：<https://github.com/hakituo/Autarkia-Msgqueue>
- 组件库路线图：<https://github.com/hakituo/Autarkia-tool/blob/main/ROADMAP.md>
- 作者：Shihao Qi — <https://qishihao.icu>

### 许可

MIT

---

## English

**Per-type TTL, message-id dedupe, discard-after-elsewhere-delivery, atomic replay, per-user flush locks, and a receiver-side staleness guard** — the small "store messages while the client is offline" problem, done correctly.

Extracted from the [Autarkia](https://qishihao.icu/projects/autarkia) agent runtime, where a phone app and a desktop bot shared one user id and kept re-delivering the same goodnight message. See the [component roadmap](https://github.com/hakituo/Autarkia-tool/blob/main/ROADMAP.md).

### Why

Storing messages for offline clients is easy. Not annoying the user is not:

- **Replayed after delivery.** The same proactive message reached the phone app, so the queued copy for the desktop client is a duplicate. It has to be discarded by id.
- **Overnight replays.** "Goodnight" is worthless at 9 a.m. Proactive messages need minutes-scale TTL, ordinary messages a day; one TTL for both is wrong either way.
- **Duplicates in queue.** The same `message_id` enqueued twice replays twice.
- **Lost on failure.** Popping before a successful send loses the message when the socket dies mid-flush.
- **Concurrent flushes.** Two reconnect paths can empty the same deque under each other's feet.
- **Wrong recipient.** With two personas sharing a user id, a connection must only take messages addressed to it.
- **Stale receivers.** A client with an old TTL, a manual flush, or a mismatched version can still receive a replay hours later, so the receiver checks age independently.

### Install

```bash
pip install autarkia-msgqueue
```

No dependencies. Python 3.10+.

### Quick start

```python
import asyncio
from autarkia_msgqueue import OfflineMessageQueue

queue = OfflineMessageQueue()

# The client is offline: park the message (deep-copied).
queue.store_offline_message(
    "user-1",
    {"type": "proactive_message", "message_id": "evt-9", "text": "晚安"},
)

# Delivered elsewhere (phone app) — never replay it to the desktop client.
queue.discard_offline_message("user-1", "evt-9")

async def on_reconnect(user_id):
    async def send(payload) -> bool:
        return await websocket_send(user_id, payload)   # False / raise => retry later

    result = await queue.flush(user_id, send, accepts=lambda msg: msg.get("persona") == "a")
    print(result)   # FlushResult(sent=0, expired=0, failed=0, skipped=0, remaining=0)
```

#### Receiver side

```python
from autarkia_msgqueue import is_stale_offline_replay

if is_stale_offline_replay(payload, max_age_seconds=600):
    return  # drop a replay that is too old to be meaningful
```

### Concepts

| API | Guarantee |
| --- | --- |
| `store_offline_message` | deep copy; returns `False` for duplicates or bad payloads |
| `discard_offline_message` | removes every copy of that `message_id`, returns the count |
| `flush` | FIFO replay; stops at the first failure and keeps the rest |
| `QueueConfig` | `default_ttl_seconds`, `proactive_ttl_seconds`, `proactive_types` |
| `FlushResult` | `sent` / `expired` / `failed` / `skipped` / `remaining`, plus `.ok` |
| `offline_replay_age`, `is_stale_offline_replay` | receiver-side age check on replays |

Replayed payloads carry `is_offline_replay: True` and `offline_queued_at` (epoch seconds); the stored copy is never mutated.

### Design notes

- **TTL per message type.** `type == "proactive_message"` or `is_proactive` uses `proactive_ttl_seconds` (default 10 min); everything else uses `default_ttl_seconds` (default 24 h). Expiry is `age >= ttl`.
- **Atomic replay.** A message leaves the queue only after `sender` returns truthy. On failure it is pushed back to the front and the flush stops, so ordering survives a broken socket.
- **Scan window.** `flush` only walks the messages present when it takes the lock; anything enqueued mid-flush is left for the next round.
- **Routing predicate.** `accepts(payload)` returning `False` leaves the message queued and keeps scanning, so one connection never steals another persona's messages.
- **Locks are per user per event loop.** Locks are recreated when the queue is used from a different event loop, so a module-level singleton does not leak locks across `asyncio.run` calls.
- **In memory, by design.** The queue is a reconnect buffer, not a message store: a process restart drops it. Persist elsewhere if you need durability.
- **Deterministic tests.** `clock=` on the queue and `now=` on `store` / `flush` / the guard remove all wall-clock dependence.

### The bigger picture

- Project page: <https://qishihao.icu/projects/autarkia>
- Runtime repository: <https://github.com/hakituo/Autarkia>
- This package: <https://github.com/hakituo/Autarkia-Msgqueue>
- Roadmap: <https://github.com/hakituo/Autarkia-tool/blob/main/ROADMAP.md>
- Author: Shihao Qi — <https://qishihao.icu>

### License

MIT
