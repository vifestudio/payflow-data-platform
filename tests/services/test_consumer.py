"""Streaming consumer: lag gauge and idempotent writes."""
import pytest

from tests.conftest import load_module


@pytest.fixture(scope="module")
def consumer():
    return load_module("consumer", "streaming/consumer.py")


class FakeConsumer:
    def __init__(self, ends, positions, assigned=True):
        self._ends = ends
        self._positions = positions
        self._assigned = assigned

    def assignment(self):
        return set(self._ends) if self._assigned else set()

    def end_offsets(self, parts):
        return {tp: self._ends[tp] for tp in parts}

    def position(self, tp):
        return self._positions[tp]


def gauge_value(consumer):
    return consumer.CONSUMER_LAG._value.get()


def test_lag_is_the_sum_over_assigned_partitions(consumer):
    fake = FakeConsumer(ends={"p0": 100, "p1": 50}, positions={"p0": 90, "p1": 45})
    consumer.update_lag(fake)
    assert gauge_value(consumer) == 15


def test_lag_is_zero_when_fully_caught_up(consumer):
    fake = FakeConsumer(ends={"p0": 10}, positions={"p0": 10})
    consumer.update_lag(fake)
    assert gauge_value(consumer) == 0


def test_lag_never_goes_negative(consumer):
    fake = FakeConsumer(ends={"p0": 5}, positions={"p0": 9})
    consumer.update_lag(fake)
    assert gauge_value(consumer) == 0


def test_lag_is_left_alone_when_no_partitions_are_assigned(consumer):
    consumer.CONSUMER_LAG.set(7)
    consumer.update_lag(FakeConsumer(ends={"p0": 1}, positions={"p0": 0}, assigned=False))
    assert gauge_value(consumer) == 7


def test_lag_update_never_raises(consumer):
    class Broken:
        def assignment(self):
            raise RuntimeError("broker down")

    consumer.update_lag(Broken())  # must not propagate: a metric must never stop scoring


@pytest.mark.parametrize("name", ["UPSERT_ACCOUNT", "INSERT_TXN", "INSERT_LABEL", "INSERT_SCORE"])
def test_every_write_is_idempotent(consumer, name):
    """Replaying a Kafka message (consumer restart, rebalance) must not duplicate rows."""
    assert "ON CONFLICT" in getattr(consumer, name)


def test_writes_use_bound_parameters_not_string_formatting(consumer):
    for name in ("UPSERT_ACCOUNT", "INSERT_TXN", "INSERT_LABEL", "INSERT_SCORE"):
        sql = getattr(consumer, name)
        assert "%s" in sql
        assert "{" not in sql and "'" not in sql
