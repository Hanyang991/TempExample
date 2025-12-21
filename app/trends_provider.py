from __future__ import annotations
from dataclasses import dataclass
from typing import List
import pandas as pd
import time
import random

@dataclass
class TrendResult:
    term: str
    geo: str
    timeframe: str
    series: pd.Series

class TrendsProvider:
    def interest_over_time(self, terms: List[str], geo: str, timeframe: str) -> List[TrendResult]:
        raise NotImplementedError

class PyTrendsProvider(TrendsProvider):
    def __init__(self, hl: str = "en-US", tz: int = 0, retries: int = 6, base_sleep: float = 2.0):
        from pytrends.request import TrendReq
        self.pytrends = TrendReq(hl=hl, tz=tz)
        self.retries = retries
        self.base_sleep = base_sleep

    def _sleep_jitter(self, seconds: float):
        time.sleep(seconds + random.uniform(0.2, 0.9))

    def interest_over_time(self, terms: List[str], geo: str, timeframe: str) -> List[TrendResult]:
        out: List[TrendResult] = []
        batch_size = 3  # ✅ 5 → 3으로 줄여서 한번에 덜 때리기

        for i in range(0, len(terms), batch_size):
            batch = terms[i:i + batch_size]

            # ✅ 배치 사이 기본 딜레이
            self._sleep_jitter(self.base_sleep)

            # ✅ 429 대응 재시도
            attempt = 0
            while True:
                try:
                    self.pytrends.build_payload(batch, timeframe=timeframe, geo=geo)
                    df = self.pytrends.interest_over_time()
                    break
                except Exception as e:
                    msg = str(e)
                    is_429 = ("429" in msg) or ("TooManyRequests" in e.__class__.__name__)
                    attempt += 1
                    if (not is_429) or (attempt > self.retries):
                        raise

                    # 지수 백오프: 2s, 4s, 8s, 16s...
                    wait = (2 ** (attempt - 1)) * 4.0
                    self._sleep_jitter(wait)

            if df is None or df.empty:
                continue
            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])

            for t in batch:
                if t in df.columns:
                    out.append(TrendResult(term=t, geo=geo, timeframe=timeframe, series=df[t]))

        return out
