from uuid import UUID
from datetime import datetime, timezone
from uuid_utils import uuid7
from ._datetime import now_floor_to_millisecond
from ._timestamp import to_uuid7_seed, floor_to_millisecond


def from_now(tz=timezone.utc):
    dt = now_floor_to_millisecond(tz)
    return from_datetime(dt)


def from_datetime(dt: datetime):
    return from_timestamp(dt.timestamp())


def from_timestamp(timestamp: float):
    ts, nanos = to_uuid7_seed(timestamp)
    return uuid7(ts, nanos)


def to_datetime(u: UUID, tz=timezone.utc) -> datetime:
    timestamp = to_timestamp(u)
    return datetime.fromtimestamp(timestamp, tz=tz)


def to_timestamp(u: UUID):
    if isinstance(u, str):
        u = UUID(u)

    if u.version != 7:
        raise Exception()
    # UUID は 128ビット。上位48ビットが timestamp（ミリ秒）
    timestamp_ms = (u.int >> 80) & ((1 << 48) - 1)
    return floor_to_millisecond(timestamp_ms / 1000)
