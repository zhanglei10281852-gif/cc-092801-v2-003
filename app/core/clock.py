from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol


def utc_now() -> datetime:
    return datetime.now(UTC)


def to_storage(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="seconds")


def from_storage(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def day_start(value: datetime) -> datetime:
    """返回该时刻所属结算日的起始时刻。

    全系统统一的跨日结算边界：UTC 零点。额度日结、用量统计和
    审计台账的日桶都以此函数为准，避免各处自行取舍时区。
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


def day_bucket(value: datetime) -> str:
    """返回该时刻所属结算日的日期桶标识（如 2026-10-03）。"""
    return to_storage(day_start(value))[:10]


class Clock(Protocol):
    def now(self) -> datetime: ...


@dataclass(slots=True)
class SystemClock:
    def now(self) -> datetime:
        return utc_now()


@dataclass(slots=True)
class FrozenClock:
    current: datetime

    def now(self) -> datetime:
        return self.current

    def advance(self, **values: int) -> datetime:
        self.current += timedelta(**values)
        return self.current
