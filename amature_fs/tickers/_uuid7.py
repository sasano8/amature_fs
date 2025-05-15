from uuid import UUID
from datetime import datetime, timezone
from uuid_utils import uuid7
from ._datetime import to_uuid7_seed, now_rounded_to_millisecond


def from_now(tz=timezone.utc):
    dt = now_rounded_to_millisecond(tz)
    return from_datetime(dt)


def from_datetime(dt: datetime):
    ts, nanos = to_uuid7_seed(dt)
    return uuid7(ts, nanos)


def from_timestamp(timestamp: float):
    ts = int(timestamp)
    # 小数点以下の秒をマイクロ秒単位で切り捨て
    microsecond = int((timestamp - ts) * 1_000_000)
    nanos = microsecond * 1000
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
    # uuid7 から得られるのはミリ秒。pythonではミリ秒は少数で表現しているので合わせる
    return timestamp_ms / 1000
