from datetime import datetime, timezone


def now_rounded_to_millisecond(tz=timezone.utc):
    dt = datetime.now(tz)
    return normalize_to_millisecond(dt)


def to_uuid7_seed(dt: datetime):
    ts = int(dt.timestamp())
    nanos = dt.microsecond * 1000
    return ts, nanos


def normalize_to_millisecond(dt: datetime):
    microsecond = round(dt.microsecond / 1000) * 1000
    if microsecond == 1_000_000:  # 桁あふれ
        return dt.replace(second=dt.second + 1, microsecond=0)
    else:
        return dt.replace(microsecond=microsecond)
