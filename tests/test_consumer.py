"""Unit tests for validation, retries and DLQ routing in the ingestion consumer."""
from datetime import datetime, timedelta, timezone

import pytest

import consumer


def good_record(**overrides):
    record = {"event_id": "e1", "device_id": "dev-0001", "tenant_id": "t-01",
              "event_ts": datetime.now(timezone.utc)}
    record.update(overrides)
    return record


class FakeMessage:
    def __init__(self, value=b"x", topic="gateway.request_log", partition=2, offset=41):
        self._value, self._topic, self._partition, self._offset = value, topic, partition, offset

    def value(self): return self._value
    def topic(self): return self._topic
    def partition(self): return self._partition
    def offset(self): return self._offset


# validate()
def test_valid_record_passes():
    assert consumer.validate(good_record()) is None


@pytest.mark.parametrize("field", consumer.REQUIRED_FIELDS)
def test_missing_required_field_fails(field):
    assert consumer.validate(good_record(**{field: None})) == f"missing {field}"


def test_future_event_time_fails():
    error = consumer.validate(good_record(event_ts=datetime.now(timezone.utc) + timedelta(hours=1)))
    assert "future" in error


# with_retries()
def test_retries_until_success(monkeypatch):
    monkeypatch.setattr(consumer.time, "sleep", lambda s: None)
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("S3 timeout")
        return "written"

    assert consumer.with_retries(flaky) == "written"
    assert calls["n"] == 3


def test_retries_give_up_and_raise(monkeypatch):
    delays = []
    monkeypatch.setattr(consumer.time, "sleep", delays.append)

    def always_fails():
        raise RuntimeError("still down")

    with pytest.raises(RuntimeError):
        consumer.with_retries(always_fails, attempts=4)
    assert delays == [1.0, 2.0, 4.0]          # exponential backoff between attempts


# route_message(): where each message goes
def test_good_message_goes_to_batch_with_lineage():
    outcome, record = consumer.route_message(FakeMessage(), lambda value, ctx: good_record())
    assert outcome == "ok"
    assert record["_kafka_partition"] == 2 and record["_kafka_offset"] == 41


def test_undecodable_message_goes_to_dlq():
    def broken(value, ctx):
        raise ValueError("unknown magic byte")

    outcome, error = consumer.route_message(FakeMessage(value=b'{"not": "avro"}'), broken)
    assert outcome == "dlq" and "deserialisation failed" in error


def test_invalid_record_goes_to_dlq():
    outcome, error = consumer.route_message(FakeMessage(), lambda v, c: good_record(device_id=""))
    assert outcome == "dlq" and error == "missing device_id"
