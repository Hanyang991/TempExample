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
    severity: str  # WATCH / RISING / BREAKOUT
    evidence: Dict[str, Any]

INTENT_PATTERNS = [
    "best", "routine", "where to buy", "near me", "in korea",
    "korean", "k beauty", "k-beauty"
]

def _safe_pct(a: float, b: float) -> float:
    denom = b if abs(b) > 1e-9 else 1.0
    return (a - b) / denom

def compute_signal(series: pd.Series, term: str, geo: str) -> Optional[Signal]:
    s = series.dropna()
    if len(s) < 21:
        return None

    last7 = float(s.iloc[-7:].mean())
    prev7 = float(s.iloc[-14:-7].mean()) if len(s) >= 14 else float(s.iloc[:-7].mean())
    wow = float(_safe_pct(last7, prev7))

    window = s.iloc[-56:] if len(s) >= 56 else s
    mu = float(window.mean())
    sigma = float(window.std(ddof=0))
    z = float((last7 - mu) / (sigma if sigma > 1e-9 else 1.0))

    y = s.iloc[-7:].to_numpy(dtype=float)
    x = np.arange(len(y), dtype=float)
    slope = float(np.polyfit(x, y, 1)[0])

    latest = float(s.iloc[-1])
    if latest < 5 and last7 < 5:
        return None

    low_term = term.lower()
    intent_flag = any(p in low_term for p in INTENT_PATTERNS)

    # Severity rules (MVP)
    severity: Optional[str] = None
    if z > 2.5 and wow > 0.35 and slope > 0:
        severity = "BREAKOUT" if intent_flag else "RISING"
    elif z > 2.0 and wow > 0.25 and slope > 0:
        severity = "RISING"
    elif z > 1.5 or wow > 0.25:
        severity = "WATCH"
    else:
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
        }
    )
