# AGENTS.md — Autarkia-Msgqueue（Autarkia 组件库系列）

给改动本仓库的 agent 看的说明书。读完应该能回答：这是什么、语义以谁为准、怎么验证、怎么发布。

## 这是什么

Autarkia 运行时组件库系列的第 7 个库：离线消息可靠性队列——按消息类型的双档 TTL、`message_id` 去重、已在别端投递后的 discard、原子重放（失败留队首 + 停止扫描）、per-user flush 锁、路由谓词，以及接收端重放时效守卫（双保险 TTL）。

- PyPI：<https://pypi.org/project/Autarkia-Msgqueue/>
- 系列路线图：<https://github.com/hakituo/Autarkia-tool/blob/main/ROADMAP.md>（**先读它**）
- 主运行时（语义权威源）：本地 `D:\AI\xiaoyou-public`，GitHub <https://github.com/hakituo/Autarkia>
- 作者站点：<https://qishihao.icu/projects/autarkia>

## 了解背景：先看主仓库的哪些文件

| 本库模块 | 主仓库源头（D:\AI\xiaoyou-public\） |
| --- | --- |
| `src/autarkia_msgqueue/queue.py` | `core\interfaces\websocket\offline_queue.py`（`OfflineQueueMixin`） |
| `src/autarkia_msgqueue/replay_guard.py` | `clients\bots\qq\session\replay_guard.py` |

宿主调用方（理解语义用，不要把它们的依赖带进来）：`core\interfaces\websocket\connection_management.py` 的 `_flush_offline_messages` 调用点、`core\services\active_care\proactive_messaging.py`（离线补投后会调 `discard_offline_message`）。

抽取时做过的通用化改造（不要再改回去）：

- `WebSocketManager` 上的 mixin → 独立类 `OfflineMessageQueue`；`self.offline_queue` / `self.offline_ttl` / `self.proactive_offline_ttl` → `QueueConfig` + 内部 `dict`。
- `send_with_retry(websocket, json.dumps(...))` → 注入 `sender: async (payload: dict) -> bool`；库不碰序列化与传输。
- `qq_connection_accepts_message(websocket, msg)` → 注入 `accepts: (payload) -> bool` 路由谓词。
- `time.time` → 可注入 `clock`，`store` / `flush` 支持 `now=` 覆盖，测试不依赖真实时钟。
- `store_offline_message` 返回 `None` → 返回 `bool`（是否入队），宿主原来的"总是成功"语义明确化。
- flush 锁由 mixin 的类级 `_offline_flush_locks`（跨事件循环泄漏）改为实例级 + 事件循环变更时重建。
- `core.utils.logger` → stdlib `logging`。
- 无持久化：与宿主一致，队列是重连缓冲，进程重启即丢。

## 本仓库结构

```text
AGENTS.md                 本文件
src/autarkia_msgqueue/    包本体（import 名 autarkia_msgqueue）
tests/                    行为测试（75 项，纯内存、无网络、无真实时钟）
pyproject.toml            hatchling；零运行时依赖；Python >=3.10
```

## 硬规则

1. **零运行时依赖**。只允许 `asyncio` / `collections` / `copy` / `logging` / `time` 等标准库。
2. **宿主 import 一个不许进 src/**：`config.*`、`core.*`、`clients.*` 一律禁止；传输层（websocket/QQ）只能以 `sender` / `accepts` 注入。
3. **TTL 是双档语义**：主动消息默认 10 分钟、普通消息 24 小时；过期判据是 `age >= ttl`。接收端守卫默认 600 秒，只对 `is_offline_replay` 生效。
4. **成功才出队**：`sender` 返回假值或抛异常时必须把消息推回队首并停止本轮扫描；禁止改成"先删后发"。
5. 行为变更先改/加测试，全绿再提交（当前 75 项）。
6. **每次 `twine upload` 前必须递增 `pyproject.toml` 的 version**。
7. PyPI token 不进 git；只存在于 `%USERPROFILE%\.pypirc`。

## 命令（本机环境）

venv 与主库共用：`D:\AI\autarkia\.venv\`（Python 3.10）。

| 用途 | 命令 |
| --- | --- |
| 测试 | `$env:PYTHONPATH="src"; D:\AI\autarkia\.venv\Scripts\python -m pytest -q` |
| 构建 | `D:\AI\autarkia\.venv\Scripts\python -m build --no-isolation` |
| 发布检查 | `D:\AI\autarkia\.venv\Scripts\python -m twine check dist\*` |
| 发布 | `$env:HTTP_PROXY="http://127.0.0.1:7897"; $env:HTTPS_PROXY=$env:HTTP_PROXY; D:\AI\autarkia\.venv\Scripts\python -m twine upload dist\* --non-interactive` |

**本机 pip 坑**：不加 `-i https://pypi.org/simple --proxy http://127.0.0.1:7897` 会报 `from versions: none`。**twine 坑**：不认 `--proxy`，走环境变量。

## 提交

- commit message 中文一句话；Windows 下写进临时文件用 `git commit -F` 防乱码。
- 改完自己提交并推送 `origin main`，不要停在"改好了"等确认。
- 推送前 `git status --porcelain` 确认只含本次任务涉及的文件。
