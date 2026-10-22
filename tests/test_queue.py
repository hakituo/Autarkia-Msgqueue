"""Offline queue tests: storage, TTL, dedupe, discard, atomic replay, locks."""

import asyncio

import pytest

from autarkia_msgqueue import (
    DEFAULT_OFFLINE_TTL,
    PROACTIVE_OFFLINE_TTL,
    FlushResult,
    OfflineMessageQueue,
    QueueConfig,
)

PROACTIVE = {"type": "proactive_message", "message_id": "p1", "text": "晚安"}
ORDINARY = {"type": "text", "message_id": "m1", "text": "在吗"}


class Sender:
    """Records every payload; optional id-based failure and in-flight hook."""

    def __init__(self, fail_ids=(), raises_ids=(), on_send=None, returns=True):
        self.payloads = []
        self.fail_ids = set(fail_ids)
        self.raises_ids = set(raises_ids)
        self.on_send = on_send
        self.returns = returns

    async def __call__(self, payload):
        self.payloads.append(payload)
        if self.on_send is not None:
            await self.on_send(payload)
        message_id = payload.get("message_id")
        if message_id in self.raises_ids:
            raise RuntimeError("socket gone")
        if message_id in self.fail_ids:
            return False
        return self.returns

    @property
    def ids(self):
        return [payload.get("message_id") for payload in self.payloads]


def flush(queue, sender, user_id="u1", **kwargs):
    return asyncio.run(queue.flush(user_id, sender, **kwargs))


class TestStore:
    def test_store_returns_true(self):
        queue = OfflineMessageQueue()
        assert queue.store_offline_message("u1", ORDINARY) is True

    def test_pending_count_increments(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        queue.store_offline_message("u1", PROACTIVE)
        assert queue.pending_count("u1") == 2

    def test_pending_count_unknown_user(self):
        assert OfflineMessageQueue().pending_count("nobody") == 0

    def test_deep_copies_message(self):
        queue = OfflineMessageQueue()
        message = {"type": "text", "message_id": "m1", "nested": {"n": 1}}
        queue.store_offline_message("u1", message)
        message["nested"]["n"] = 99
        message["text"] = "mutated"
        sender = Sender()
        flush(queue, sender)
        assert sender.payloads[0]["nested"] == {"n": 1}
        assert "text" not in sender.payloads[0]

    def test_duplicate_message_id_ignored(self):
        queue = OfflineMessageQueue()
        assert queue.store_offline_message("u1", ORDINARY) is True
        assert queue.store_offline_message("u1", dict(ORDINARY)) is False
        assert queue.pending_count("u1") == 1

    def test_distinct_ids_both_stored(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        queue.store_offline_message("u1", dict(ORDINARY, message_id="m2"))
        assert queue.pending_count("u1") == 2

    def test_missing_message_id_not_deduped(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", {"type": "text"})
        queue.store_offline_message("u1", {"type": "text"})
        assert queue.pending_count("u1") == 2

    def test_message_id_whitespace_deduped(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", dict(ORDINARY, message_id=" m1 "))
        queue.store_offline_message("u1", dict(ORDINARY, message_id="m1"))
        assert queue.pending_count("u1") == 1

    def test_numeric_message_id_deduped(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", dict(ORDINARY, message_id=7))
        queue.store_offline_message("u1", dict(ORDINARY, message_id="7"))
        assert queue.pending_count("u1") == 1

    def test_users_are_isolated(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        queue.store_offline_message("u2", PROACTIVE)
        assert queue.pending_count("u1") == 1
        assert queue.pending_count("u2") == 1

    def test_rejects_non_mapping(self):
        queue = OfflineMessageQueue()
        assert queue.store_offline_message("u1", "not a payload") is False
        assert queue.pending_count("u1") == 0

    def test_pending_users(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        queue.store_offline_message("u2", ORDINARY)
        assert sorted(queue.pending_users()) == ["u1", "u2"]

    def test_clear(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        queue.clear()
        assert queue.pending_users() == []


class TestTtl:
    def test_ordinary_uses_default_ttl(self):
        assert PROACTIVE_OFFLINE_TTL == 600
        assert DEFAULT_OFFLINE_TTL == 24 * 3600
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY, now=0.0)
        result = flush(queue, Sender(), now=PROACTIVE_OFFLINE_TTL + 1)
        assert (result.sent, result.expired) == (1, 0)

    def test_proactive_type_uses_short_ttl(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", PROACTIVE, now=0.0)
        result = flush(queue, Sender(), now=PROACTIVE_OFFLINE_TTL + 1)
        assert (result.sent, result.expired) == (0, 1)

    def test_is_proactive_flag_uses_short_ttl(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message(
            "u1", {"type": "text", "message_id": "m1", "is_proactive": True}, now=0.0
        )
        assert flush(queue, Sender(), now=601).expired == 1

    def test_type_is_case_and_whitespace_insensitive(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", {"type": "  PROACTIVE_MESSAGE "}, now=0.0)
        assert flush(queue, Sender(), now=601).expired == 1

    def test_config_overrides_ttls(self):
        config = QueueConfig(default_ttl_seconds=5.0, proactive_ttl_seconds=1.0)
        queue = OfflineMessageQueue(config=config)
        queue.store_offline_message("u1", ORDINARY, now=0.0)
        queue.store_offline_message("u1", PROACTIVE, now=0.0)
        result = flush(queue, Sender(), now=2.0)
        assert (result.sent, result.expired) == (1, 1)

    def test_config_proactive_types(self):
        config = QueueConfig(proactive_ttl_seconds=1.0, proactive_types=frozenset({"alert"}))
        queue = OfflineMessageQueue(config=config)
        queue.store_offline_message("u1", {"type": "alert"}, now=0.0)
        queue.store_offline_message("u1", PROACTIVE, now=0.0)
        result = flush(queue, Sender(), now=2.0)
        assert (result.sent, result.expired) == (1, 1)


class TestDiscard:
    def test_discard_removes_message(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        assert queue.discard_offline_message("u1", "m1") == 1
        assert queue.pending_count("u1") == 0

    def test_discard_returns_zero_for_unknown_id(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        assert queue.discard_offline_message("u1", "other") == 0
        assert queue.pending_count("u1") == 1

    def test_discard_returns_zero_for_empty_id(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        assert queue.discard_offline_message("u1", "  ") == 0
        assert queue.pending_count("u1") == 1

    def test_discard_unknown_user(self):
        assert OfflineMessageQueue().discard_offline_message("nobody", "m1") == 0

    def test_discard_drops_empty_bucket(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        queue.discard_offline_message("u1", "m1")
        assert "u1" not in queue.pending_users()

    def test_discard_keeps_other_messages_in_order(self):
        queue = OfflineMessageQueue()
        for index in range(4):
            queue.store_offline_message("u1", {"type": "text", "message_id": f"m{index}"})
        assert queue.discard_offline_message("u1", "m1") == 1
        sender = Sender()
        flush(queue, sender)
        assert sender.ids == ["m0", "m2", "m3"]


class TestFlush:
    def test_empty_queue_result(self):
        assert flush(OfflineMessageQueue(), Sender()) == FlushResult()

    def test_unknown_user_result(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        assert flush(queue, Sender(), user_id="u2") == FlushResult()

    def test_sends_all_in_order(self):
        queue = OfflineMessageQueue()
        for index in range(3):
            queue.store_offline_message("u1", {"type": "text", "message_id": f"m{index}"})
        result = flush(queue, Sender())
        assert (result.sent, result.remaining) == (3, 0)

    def test_marks_replay_payload(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY, now=1000.0)
        sender = Sender()
        flush(queue, sender, now=1001.0)
        assert sender.payloads[0]["is_offline_replay"] is True
        assert sender.payloads[0]["offline_queued_at"] == 1000.0

    def test_queued_at_matches_store_time(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY, now=42.5)
        sender = Sender()
        flush(queue, sender, now=43.0)
        assert sender.payloads[0]["offline_queued_at"] == 42.5

    def test_original_message_not_mutated(self):
        queue = OfflineMessageQueue()
        message = dict(ORDINARY)
        queue.store_offline_message("u1", message)
        flush(queue, Sender())
        assert "is_offline_replay" not in message
        assert "offline_queued_at" not in message

    def test_expires_old_proactive(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", PROACTIVE, now=0.0)
        result = flush(queue, Sender(), now=PROACTIVE_OFFLINE_TTL)
        assert (result.sent, result.expired, result.remaining) == (0, 1, 0)

    def test_expires_old_ordinary(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY, now=0.0)
        result = flush(queue, Sender(), now=DEFAULT_OFFLINE_TTL + 1)
        assert (result.sent, result.expired) == (0, 1)

    def test_boundary_age_equal_ttl_expires(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY, now=0.0)
        assert flush(queue, Sender(), now=DEFAULT_OFFLINE_TTL).expired == 1

    def test_just_under_ttl_survives(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY, now=0.0)
        assert flush(queue, Sender(), now=DEFAULT_OFFLINE_TTL - 0.001).sent == 1

    def test_sender_false_keeps_message(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        result = flush(queue, Sender(returns=False))
        assert (result.sent, result.failed, result.remaining) == (0, 1, 1)
        assert queue.pending_count("u1") == 1

    def test_sender_exception_keeps_message(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        result = flush(queue, Sender(raises_ids={"m1"}))
        assert (result.failed, result.remaining) == (1, 1)

    def test_failure_stops_flush(self):
        queue = OfflineMessageQueue()
        for index in range(3):
            queue.store_offline_message("u1", {"type": "text", "message_id": f"m{index}"})
        sender = Sender(fail_ids={"m0"})
        result = flush(queue, sender)
        assert sender.ids == ["m0"]
        assert result.remaining == 3

    def test_failed_message_returns_to_front(self):
        queue = OfflineMessageQueue()
        for index in range(3):
            queue.store_offline_message("u1", {"type": "text", "message_id": f"m{index}"})
        flush(queue, Sender(fail_ids={"m1"}))
        sender = Sender()
        flush(queue, sender)
        assert sender.ids == ["m1", "m2"]

    def test_result_ok_property(self):
        assert FlushResult().ok is True
        assert FlushResult(failed=1).ok is False

    def test_remaining_counts(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", PROACTIVE, now=0.0)
        queue.store_offline_message("u1", ORDINARY, now=0.0)
        result = flush(queue, Sender(), now=601)
        assert (result.expired, result.sent, result.remaining) == (1, 1, 0)

    def test_bucket_removed_when_empty(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        flush(queue, Sender())
        assert queue.pending_users() == []

    def test_bucket_kept_when_remaining(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        flush(queue, Sender(fail_ids={"m1"}))
        assert queue.pending_users() == ["u1"]

    def test_second_flush_is_noop(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        flush(queue, Sender())
        assert flush(queue, Sender()) == FlushResult()

    def test_mixed_expiry_counts(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", PROACTIVE, now=0.0)
        queue.store_offline_message("u1", ORDINARY, now=0.0)
        sender = Sender()
        result = flush(queue, sender, now=601)
        assert (result.sent, result.expired) == (1, 1)
        assert sender.ids == ["m1"]

    def test_new_messages_during_flush_wait_for_next_round(self):
        queue = OfflineMessageQueue()

        async def arrive(_payload):
            queue.store_offline_message("u1", dict(ORDINARY, message_id="late"))

        queue.store_offline_message("u1", ORDINARY)
        result = flush(queue, Sender(on_send=arrive))
        assert (result.sent, result.remaining) == (1, 1)

    def test_flush_result_defaults(self):
        result = FlushResult()
        assert (result.sent, result.expired, result.failed, result.skipped, result.remaining) == (
            0,
            0,
            0,
            0,
            0,
        )


class TestRouting:
    def test_accepts_none_delivers_all(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        queue.store_offline_message("u1", PROACTIVE)
        assert flush(queue, Sender()).sent == 2

    def test_accepts_false_skips_and_continues(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", {"type": "text", "message_id": "mine"})
        queue.store_offline_message("u1", {"type": "text", "message_id": "theirs"})
        sender = Sender()
        result = flush(queue, sender, accepts=lambda m: m.get("message_id") == "mine")
        assert (result.sent, result.skipped, result.remaining) == (1, 1, 1)
        assert sender.ids == ["mine"]

    def test_skipped_messages_stay_queued(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        flush(queue, Sender(), accepts=lambda _message: False)
        assert queue.pending_count("u1") == 1

    def test_all_skipped_result(self):
        queue = OfflineMessageQueue()
        for index in range(3):
            queue.store_offline_message("u1", {"type": "text", "message_id": f"m{index}"})
        result = flush(queue, Sender(), accepts=lambda _message: False)
        assert (result.sent, result.skipped, result.failed, result.remaining) == (0, 3, 0, 3)

    def test_skip_preserves_relative_order(self):
        queue = OfflineMessageQueue()
        for index in range(4):
            queue.store_offline_message("u1", {"type": "text", "message_id": f"m{index}"})
        flush(queue, Sender(), accepts=lambda m: m["message_id"] in {"m1", "m3"})
        sender = Sender()
        flush(queue, sender)
        assert sender.ids == ["m0", "m2"]


class TestConcurrency:
    def test_concurrent_flush_sends_once(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        sender = Sender()

        async def scenario():
            return await asyncio.gather(queue.flush("u1", sender), queue.flush("u1", sender))

        results = asyncio.run(scenario())
        assert sum(result.sent for result in results) == 1
        assert len(sender.payloads) == 1

    def test_concurrent_flush_different_users_independent(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        queue.store_offline_message("u2", dict(ORDINARY, message_id="m2"))
        sender = Sender()

        async def scenario():
            return await asyncio.gather(queue.flush("u1", sender), queue.flush("u2", sender))

        results = asyncio.run(scenario())
        assert [result.sent for result in results] == [1, 1]

    def test_flush_across_event_loops(self):
        queue = OfflineMessageQueue()
        queue.store_offline_message("u1", ORDINARY)
        assert flush(queue, Sender(fail_ids={"m1"})).failed == 1
        sender = Sender()
        assert flush(queue, sender).sent == 1
        assert sender.ids == ["m1"]


class TestApi:
    def test_version(self):
        import autarkia_msgqueue

        assert autarkia_msgqueue.__version__ == "0.1.0"

    def test_all_exports(self):
        import autarkia_msgqueue

        for name in autarkia_msgqueue.__all__:
            assert hasattr(autarkia_msgqueue, name)
