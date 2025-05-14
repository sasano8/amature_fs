import time
import threading
from datetime import datetime
from . import _uuid7


class Ticker:
    def tick(self):
        raise NotImplementedError()

    def __iter__(self):
        while True:
            yield self.tick()

    def take(self, n):
        for i in range(n):
            yield self.tick()


class TimestampTicker(Ticker):
    def tick(self) -> float:
        raise NotImplementedError()


class RealtimeTicker(TimestampTicker):
    """ミリ精度のティッカー。スレッドレベルでユニークなミリ秒を返す"""

    def __init__(self, interval_sec: float = 0.001, spin_sleep_sec: float = 0):
        self._last_ns = time.time_ns()
        self._interval_ns = int(interval_sec * 1_000_000_000)  # ナノ秒単位にする
        self._sleep_sec = spin_sleep_sec
        self._lock = threading.Lock()

    def tick(self) -> float:
        with self._lock:
            target_ns = self._last_ns + self._interval_ns
            while True:
                current_ns = time.time_ns()
                if target_ns <= current_ns:
                    self._last_ns = current_ns
                    ms = current_ns // 1_000_000  # ミリ秒未満切り捨て
                    return ms / 1_000  # 秒単位に
                else:
                    time.sleep(0)


class VirtualTicker(TimestampTicker):
    """
    仮想時間ベースのミリ精度ティッカー。
    待機なしで、tick()を呼ぶたびに仮想時間を1msずつ進める。
    """

    @classmethod
    def from_datetime(cls, dt: datetime, interval_sec: int = 0.001) -> "VirtualTicker":
        """
        指定された datetime を起点に仮想ティッカーを生成する。
        :param dt: タイムゾーン付き datetime（UTCが望ましい）
        :param interval_ms: 進むミリ秒単位の間隔
        :return: VirtualMsTicker インスタンス
        """
        return cls(start_sec=dt.timestamp(), interval_sec=interval_sec)

    def __init__(self, start_sec: float = 0, interval_sec: float = 0.001):
        """
        :param start_ms: 開始時刻（ミリ秒単位、エポック起点でなくてもよい）
        :param interval_ms: tick() ごとに進めるミリ秒数
        """
        self._start_sec = start_sec
        self._current_sec = start_sec - interval_sec
        self._interval_sec = interval_sec
        self._lock = threading.Lock()

    def tick(self) -> float:
        """
        次の仮想ミリ秒値を返し、内部時刻を進める。
        :return: 仮想ミリ秒値（int）
        """
        with self._lock:
            self._current_sec += self._interval_sec
            return self._current_sec


class Uuid7Ticker(Ticker):
    def __init__(self, ticker: TimestampTicker):
        self._ticker = ticker

    def tick(self):
        timestamp = self._ticker.tick()
        u = _uuid7.from_timestamp(timestamp)
        return timestamp, u
