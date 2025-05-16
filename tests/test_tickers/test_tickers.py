from datetime import datetime, timezone
from dateutil import tz
from amature_fs import tickers
import pytest
from uuid_utils import uuid7


def test_floor_to_millisecond():
    dt = tickers._datetime.floor_to_millisecond(datetime(2025, 5, 14, 0, 0, 0, 0))
    assert dt.isoformat() == "2025-05-14T00:00:00"

    dt = tickers._datetime.floor_to_millisecond(datetime(2025, 5, 14, 0, 0, 0, 123_499))
    assert dt.isoformat() == "2025-05-14T00:00:00.123000"

    dt = tickers._datetime.floor_to_millisecond(datetime(2025, 5, 14, 0, 0, 0, 123_500))
    assert dt.isoformat() == "2025-05-14T00:00:00.123000"

    dt = tickers._datetime.floor_to_millisecond(datetime(2025, 5, 14, 0, 0, 0, 123_900))
    assert dt.isoformat() == "2025-05-14T00:00:00.123000"

    dt = tickers._datetime.floor_to_millisecond(datetime(2025, 5, 14, 0, 0, 0, 123_990))
    assert dt.isoformat() == "2025-05-14T00:00:00.123000"

    dt = tickers._datetime.floor_to_millisecond(datetime(2025, 5, 14, 0, 0, 0, 123_994))
    assert dt.isoformat() == "2025-05-14T00:00:00.123000"

    dt = tickers._datetime.floor_to_millisecond(datetime(2025, 5, 14, 0, 0, 0, 123_995))
    assert dt.isoformat() == "2025-05-14T00:00:00.124000"

    dt = tickers._datetime.floor_to_millisecond(datetime(2025, 5, 14, 0, 0, 0, 123_999))
    assert dt.isoformat() == "2025-05-14T00:00:00.124000"

    dt = tickers._datetime.floor_to_millisecond(datetime(2025, 5, 14, 0, 0, 0, 999_999))
    assert dt.isoformat() == "2025-05-14T00:00:01"


def test_now_floor_to_millisecond(n=100):
    """ミリ秒精度の現在日が得られているか確認する。
    N 回試行し、一定のヒット率が得られていればよしとする。
    """
    now = tickers._datetime.now_floor_to_millisecond()
    normalized_now = tickers._datetime.floor_to_millisecond(now)

    assert now == normalized_now
    assert now.tzinfo == timezone.utc

    now = tickers._datetime.now_floor_to_millisecond(tz.gettz("Asia/Tokyo"))
    normalized_now = tickers._datetime.floor_to_millisecond(now)

    assert now == normalized_now
    assert now.tzinfo == tz.gettz("Asia/Tokyo")

    results = []
    for i in range(n):
        dt = datetime.now(tz=timezone.utc)
        now = tickers._datetime.now_floor_to_millisecond()
        normalized_now = tickers._datetime.floor_to_millisecond(dt)
        results.append(normalized_now == now)

    hit_rate = sum(results) / len(results)
    assert 0.95 < hit_rate


def test_to_uuid7_seed():
    timstamp = 1.123456789
    dt = datetime.fromtimestamp(timstamp, tz=timezone.utc)
    ts, nanos = tickers._timestamp.to_uuid7_seed(dt.timestamp())
    assert ts == 1
    assert nanos == 123000000

    dt = datetime(2025, 5, 15, 18, 9, 18, 664000, tzinfo=timezone.utc)
    ts, nanos = tickers._timestamp.to_uuid7_seed(dt.timestamp())
    assert ts == 1747332558
    assert nanos == 664000000

    dt = datetime(2025, 5, 15, 18, 9, 18, 664123, tzinfo=timezone.utc)
    ts, nanos = tickers._timestamp.to_uuid7_seed(dt.timestamp())
    assert ts == 1747332558
    assert nanos == 664000000

    dt = datetime(2025, 5, 15, 18, 9, 18, 665000, tzinfo=timezone.utc)
    ts, nanos = tickers._timestamp.to_uuid7_seed(dt.timestamp())
    assert ts == 1747332558
    assert nanos == 665000000


def test_uuid7(n=100):
    """uuid7に関する挙動に一貫性があるか確認する"""
    for i in range(n):
        now = tickers._datetime.now_floor_to_millisecond()
        u1 = tickers._uuid7.from_datetime(now)
        u2 = tickers._uuid7.from_timestamp(now.timestamp())
        ts, nanos = tickers._timestamp.to_uuid7_seed(now.timestamp())
        u3 = uuid7(ts, nanos)

        dt1 = tickers._uuid7.to_datetime(u1)
        dt2 = tickers._uuid7.to_datetime(u2)
        dt3 = tickers._uuid7.to_datetime(u3)
        dt4 = datetime.fromtimestamp(tickers._uuid7.to_timestamp(u1), tz=timezone.utc)
        dt5 = datetime.fromtimestamp(tickers._uuid7.to_timestamp(u2), tz=timezone.utc)
        dt6 = datetime.fromtimestamp(tickers._uuid7.to_timestamp(u3), tz=timezone.utc)

        if not dt1 == dt2:
            print(now)
            assert dt1 == dt2

        if not dt1 == dt3:
            print(now)
            assert dt1 == dt3

        if not dt1 == dt4:
            print(now)
            assert dt1 == dt4

        if not dt1 == dt5:
            print(now)
            assert dt1 == dt5

        if not dt1 == dt6:
            print(now)
            assert dt1 == dt6

        assert dt1 == now
        assert dt1 == tickers._datetime.floor_to_millisecond(now)


def test_timestamp_ticker_to_datetime():
    ticker = tickers.RealtimeTicker(interval_sec=1)
    ts = ticker.tick()
    dt1 = tickers._uuid7.to_datetime(tickers._uuid7.from_now())
    dt2 = datetime.fromtimestamp(ts, tz=tz.gettz("Asia/Tokyo"))
    assert dt1 == dt2  # ミリ秒の切れ目でエラーになる可能性はある

    dt1 = tickers._uuid7.to_datetime(tickers._uuid7.from_now())
    ticker = tickers.VirtualTicker(dt1.timestamp(), 0.001)
    ts = ticker.tick()
    dt2 = datetime.fromtimestamp(ts, tz=tz.gettz("Asia/Tokyo"))
    assert dt1 == dt2


@pytest.mark.parametrize(
    "expected_interval,n,min_hit_rate",
    [(0.001, 100, 0.8), (0.002, 100, 0.9), (0.003, 100, 0.9)],
)
def test_realtime_ticker(expected_interval, n, min_hit_rate):
    """ "あるインターバルでN回試行し、インターバルのヒット率を検証する"""
    ticker = tickers.RealtimeTicker(interval_sec=expected_interval)
    results = list(ticker.take(n))
    assert len(results) == n

    diffs = []
    prev = results[0]
    for result in results[1:]:
        diffs.append(round(result - prev, 3))
        prev = result

    hit_count = sum(1 for d in diffs if expected_interval <= d)
    hit_rate = hit_count / len(diffs)
    if not (hit_rate == 1):
        print(results)
        print(diffs)
        assert hit_rate == 1, "interval 以上の差分があることはマスト条件"

    # 全てが interval 通りの差分になるわけではないのでヒット率でテストする
    hit_count = sum(1 for d in diffs if d == expected_interval)
    hit_rate = hit_count / len(diffs)

    if not (hit_rate >= min_hit_rate):
        print(results)
        print(diffs)
        assert hit_rate >= min_hit_rate, (
            f"Hit rate too low: {hit_rate:.2%} < {min_hit_rate:.2%}  {hit_count} / {len(diffs)}"
        )

    # TODO: 差分の最大値を検証する


def test_virtual_ticker(expected_interval=1, n=100, min_hit_rate=1):
    ticker = tickers.VirtualTicker(0, expected_interval)
    results = list(ticker.take(n))
    assert len(results) == n

    diffs = []
    prev = results[0]
    for result in results[1:]:
        diffs.append(result - prev)
        prev = result

    hit_count = sum(1 for d in diffs if d == expected_interval)
    hit_rate = hit_count / len(diffs)

    assert hit_rate >= min_hit_rate, (
        f"Hit rate too low: {hit_rate:.2%} < {min_hit_rate:.2%}  {hit_count} / {len(diffs)}"
    )
