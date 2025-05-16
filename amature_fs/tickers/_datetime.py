from datetime import datetime, timezone
from ._timestamp import floor_to_millisecond as _timestamp_to_floor_to_millisecond


def from_now_millisecond(tz=timezone.utc):
    dt = datetime.now(tz)
    return floor_to_millisecond(dt)


def floor_to_millisecond(dt: datetime):
    timestamp = _timestamp_to_floor_to_millisecond(dt.timestamp())
    return datetime.fromtimestamp(timestamp, tz=dt.tzinfo)
