from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Any
import numpy as np
import pandas as pd

@dataclass
class Signal:
    term: str
    geo: str
    wow_change: float
    z_score: float
    slope_7d: float
    latest: float
    intent_flag: bool
    severity: str  # EMERGING / WATCH / RISING / BREAKOUT
    evidence: Dict[str, Any]

INTENT_PATTERNS = [
    "best", "routine", "where to buy", "near me", "in korea",
    "korean", "k beauty", "k-beauty"
]

def _safe_pct(a: float, b: float) -> float:
    denom = b if abs(b) > 1e-9 else 1.0
    return (a - b) / denom

def _nonzero_streak(values: np.ndarray, thr: float = 1.0) -> int:
    """Count consecutive days from the end with value >= thr."""
    k = 0
    for v in values[::-1]:
        if v >= thr:
            k += 1
        else:
            break
    return k

def compute_signal(series: pd.Series, term: str, geo: str) -> Optional[Signal]:
    s = series.dropna()
    if len(s) < 21:
        return None

    # -----------------------
    # Core aggregates (existing)
    # -----------------------
    last7 = float(s.iloc[-7:].mean())
    prev7 = float(s.iloc[-14:-7].mean()) if len(s) >= 14 else float(s.iloc[:-7].mean())
    wow = float(_safe_pct(last7, prev7))

    window = s.iloc[-56:] if len(s) >= 56 else s
    mu = float(window.mean())
    sigma = float(window.std(ddof=0))
    z = float((last7 - mu) / (sigma if sigma > 1e-9 else 1.0))

    y7 = s.iloc[-7:].to_numpy(dtype=float)
    x7 = np.arange(len(y7), dtype=float)
    slope = float(np.polyfit(x7, y7, 1)[0])

    latest = float(s.iloc[-1])

    low_term = term.lower()
    intent_flag = any(p in low_term for p in INTENT_PATTERNS)

    # -----------------------
    # EARLY SIGNAL FEATURES (new)
    # -----------------------
    # 최근 1~3일 vs 직전 7/14일 비교 (짧은 폭발 감지)
    last3_avg = float(s.iloc[-3:].mean()) if len(s) >= 3 else float(s.iloc[-1])
    prev14 = s.iloc[-17:-3] if len(s) >= 17 else s.iloc[:-3]  # last3 제외한 이전 구간
    prev14_avg = float(prev14.mean()) if len(prev14) > 0 else float(s.iloc[:-3].mean() if len(s) > 3 else 0.0)

    spike_3v14 = float(_safe_pct(last3_avg, prev14_avg))

    # DoD 변화율 + 가속도 (최근 3일)
    # (GoogleTrends 0/값 튐 문제 때문에 절대 변화량도 같이 봄)
    vals = s.to_numpy(dtype=float)
    d1 = vals[-1] - vals[-2] if len(vals) >= 2 else 0.0
    d2 = vals[-2] - vals[-3] if len(vals) >= 3 else 0.0
    accel = d1 - d2

    # 0 -> 비제로 전환 및 연속 비제로 streak
    nz_thr = 1.0
    streak = _nonzero_streak(vals[-14:], thr=nz_thr)  # 최근 14일 내 연속성
    prev_latest = float(vals[-2]) if len(vals) >= 2 else 0.0
    revived = (prev_latest < nz_thr) and (latest >= nz_thr)

    # 베이스라인이 너무 낮을 때 z가 불안정하니 절대값 기준도 둠
    abs_ok = (latest >= 5.0) or (last3_avg >= 5.0)  # 기존 latest<5 필터보다 조금 완화 가능

    # -----------------------
    # Severity rules (with Early)
    # -----------------------
    severity: Optional[str] = None

    # 0/저검색량 키워드의 "막 살아남" 포착
    # - revive(0->1+) 또는 연속 2일 이상 비제로
    # - 최근 3일이 이전 14일 대비 유의미하게 급증 (배수/비율)
    # - 일간 증가가 양수이거나 가속도가 양수
    early_gate = (
        abs_ok and
        (revived or streak >= 2) and
        (spike_3v14 > 0.80 or (last3_avg >= 10 and prev14_avg <= 5)) and
        (d1 > 0 or accel > 0)
    )

    if early_gate:
        severity = "EMERGING"

    # 기존 룰은 유지하되, early가 있으면 상위 단계로 자연스럽게 승급되게
    if z > 2.5 and wow > 0.35 and slope > 0:
        severity = "BREAKOUT" if intent_flag else "RISING"
    elif z > 2.0 and wow > 0.25 and slope > 0:
        severity = "RISING"
    elif (z > 1.5) or (wow > 0.25):
        severity = severity or "WATCH"  # early가 있으면 EMERGING 유지, 없으면 WATCH
    else:
        # early만으로도 올릴지 여부: 너무 노이즈면 여기서 컷
        if severity is None:
            return None

    # 기존의 너무 강한 필터는 early를 죽일 수 있어서,
    # final gate를 약간 유연하게: 최신과 최근3일 평균이 모두 극저(예: <2)면 제외
    if latest < 2 and last3_avg < 2:
        return None

    return Signal(
        term=term,
        geo=geo,
        wow_change=wow,
        z_score=z,
        slope_7d=slope,
        latest=latest,
        intent_flag=bool(intent_flag),
        severity=severity,
        evidence={
            "last7_avg": last7,
            "prev7_avg": prev7,
            "mu": mu,
            "sigma": sigma,

            # early evidence
            "last3_avg": last3_avg,
            "prev14_avg_excl_last3": prev14_avg,
            "spike_3v14": spike_3v14,
            "dod_delta": d1,
            "accel_2d": accel,
            "nonzero_streak_14d": streak,
            "revived_0_to_nonzero": revived,
        }
    )
