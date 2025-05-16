def _extract_timestamp_parts(timestamp: float) -> tuple[int, int]:
    """timestamp を (秒, ミリ秒) に分解（ミリ秒は誤差補正 + 桁あふれ対策）"""
    ts = int(timestamp)
    frac = round(timestamp - ts, 5)  # 小数点第位に四捨五入
    millis = int(frac * 1000)  # ミリ秒に切り捨て

    # 近似値・桁あふれ対策
    if millis == 1000:
        ts += 1
        millis = 0

    return ts, millis


def to_uuid7_seed(timestamp: float):
    ts, millis = _extract_timestamp_parts(timestamp)
    nanos = millis * 1_000_000
    return ts, nanos


def floor_to_millisecond(timestamp: float) -> float:
    ts, millis = _extract_timestamp_parts(timestamp)
    return ts + (millis / 1000)
