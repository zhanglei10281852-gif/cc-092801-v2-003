from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.core.clock import FrozenClock, day_bucket, day_start, from_storage, to_storage
from app.core.errors import ValidationError
from app.core.security import hash_password, normalize_username, request_fingerprint, verify_password
from app.services.jobs import JobService


def test_password_hashing_and_username_normalization():
    encoded = hash_password("Strong!23456")
    assert verify_password("Strong!23456", encoded)
    assert not verify_password("wrong", encoded)
    assert normalize_username(" Admin.User ") == "admin.user"
    with pytest.raises(ValidationError):
        normalize_username("不支持空格")


def test_request_fingerprint_is_order_independent():
    assert request_fingerprint({"a": 1, "b": 2}) == request_fingerprint({"b": 2, "a": 1})


def test_clock_storage_round_trip():
    value = datetime(2026, 9, 24, 8, 30, tzinfo=UTC)
    assert from_storage(to_storage(value)) == value


def test_unified_day_boundary_uses_utc_midnight():
    late = datetime(2026, 10, 3, 23, 59, 59, tzinfo=UTC)
    early = datetime(2026, 10, 4, 0, 0, 0, tzinfo=UTC)
    assert to_storage(day_start(late)) == "2026-10-03T00:00:00+00:00"
    assert to_storage(day_start(early)) == "2026-10-04T00:00:00+00:00"
    assert day_bucket(late) == "2026-10-03"
    assert day_bucket(early) == "2026-10-04"
    naive = datetime(2026, 10, 3, 12, 0, 0)
    assert day_bucket(naive) == "2026-10-03"


def test_job_claim_complete_and_deduplicate(client):
    from app.database import get_connection, transaction
    clock = FrozenClock(datetime(2026, 9, 24, 8, 30, tzinfo=UTC))
    with transaction(immediate=True) as connection:
        service = JobService(connection, clock)
        first = service.enqueue("daily", "daily:2026-09-24", {"date": "2026-09-24"})
        second = service.enqueue("daily", "daily:2026-09-24", {"date": "2026-09-24"})
        assert first["id"] == second["id"]
        claimed = service.claim("worker-1")
        assert claimed and claimed["status"] == "running"
        completed = service.complete(claimed["id"], "worker-1", {"count": 3})
        assert completed["status"] == "completed"
