"""Receiver-side staleness guard tests."""

from autarkia_msgqueue import (
    OFFLINE_REPLAY_MAX_AGE_SECONDS,
    is_stale_offline_replay,
    offline_replay_age,
)

REPLAY = {"is_offline_replay": True, "offline_queued_at": 1000.0}


class TestAge:
    def test_default_constant(self):
        assert OFFLINE_REPLAY_MAX_AGE_SECONDS == 600

    def test_non_dict_is_not_a_replay(self):
        assert offline_replay_age("payload") is None
        assert offline_replay_age(None) is None

    def test_non_replay_returns_none(self):
        assert offline_replay_age({"offline_queued_at": 1000.0}, now=2000.0) is None

    def test_age_from_queued_at(self):
        assert offline_replay_age(REPLAY, now=1300.0) == 300.0

    def test_falls_back_to_timestamp(self):
        data = {"is_offline_replay": True, "timestamp": 900.0}
        assert offline_replay_age(data, now=1000.0) == 100.0

    def test_missing_both_fields(self):
        assert offline_replay_age({"is_offline_replay": True}, now=1000.0) is None

    def test_invalid_value(self):
        data = {"is_offline_replay": True, "offline_queued_at": "not a number"}
        assert offline_replay_age(data, now=1000.0) is None

    def test_future_timestamp_clamped(self):
        assert offline_replay_age(REPLAY, now=500.0) == 0.0

    def test_numeric_string_accepted(self):
        data = {"is_offline_replay": True, "offline_queued_at": "1000"}
        assert offline_replay_age(data, now=1500.0) == 500.0

    def test_now_override_wins(self):
        assert offline_replay_age(REPLAY, now=1100.0, clock=lambda: 9999.0) == 100.0

    def test_clock_injection(self):
        assert offline_replay_age(REPLAY, clock=lambda: 1040.0) == 40.0


class TestStaleness:
    def test_stale_beyond_max_age(self):
        assert is_stale_offline_replay(REPLAY, now=1000.0 + 601) is True

    def test_not_stale_at_boundary(self):
        assert is_stale_offline_replay(REPLAY, now=1000.0 + 600) is False

    def test_fresh_replay(self):
        assert is_stale_offline_replay(REPLAY, now=1010.0) is False

    def test_non_replay_never_stale(self):
        assert is_stale_offline_replay({"offline_queued_at": 0}, now=10_000.0) is False

    def test_custom_max_age(self):
        assert is_stale_offline_replay(REPLAY, max_age_seconds=10, now=1020.0) is True

    def test_missing_timestamp_not_stale(self):
        assert is_stale_offline_replay({"is_offline_replay": True}, now=10_000.0) is False

    def test_clock_injection(self):
        assert is_stale_offline_replay(REPLAY, clock=lambda: 1000.0 + 900) is True
