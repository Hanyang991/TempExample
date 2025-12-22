# app/backfill.py
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date, timedelta
from typing import Optional, List, Dict, Any

import pandas as pd

from app.db import engine  # uses POSTGRES_DSN from config :contentReference[oaicite:1]{index=1}
from app.detector import compute_signal  # backfill uses the same rules :contentReference[oaicite:2]{index=2}

from sqlalchemy import text


def _iso(d: date) -> str:
    return d.isoformat()


def backfill_events(
    months: int = 3,
    warmup_days: int = 70,
    only_severities: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Recompute historical RISING/BREAKOUT signals using stored Google Trends daily series (trend_series).

    - months: how many months of events you want to REPORT
    - warmup_days: extra history pulled BEFORE the reporting window so z-score/means stabilize
      (compute_signal uses up to ~56 days, plus it requires >=21 points) :contentReference[oaicite:3]{index=3}
    - only_severities: e.g. ["RISING", "BREAKOUT"]
    """
    if only_severities is None:
        only_severities = ["RISING", "BREAKOUT"]

    # Reporting window: last N months (approx by days to avoid month arithmetic complexity)
    report_end = date.today()
    report_start = report_end - timedelta(days=months * 30)

    # Pull more history for stable calculation, but only keep events within report window
    pull_start = report_start - timedelta(days=warmup_days)

    q = text("""
        SELECT term, geo, date::date AS date, value::float8 AS value
        FROM trend_series
        WHERE date >= :start_date
        ORDER BY term, geo, date ASC;
    """)
    df = pd.read_sql(q, engine, params={"start_date": _iso(pull_start)})

    if df.empty:
        return pd.DataFrame(columns=[
            "as_of_date", "term", "geo", "severity",
            "wow_change", "z_score", "slope_7d", "latest",
            "last7_avg", "prev7_avg", "mu", "sigma",
        ])

    # Ensure correct dtypes
    df["date"] = pd.to_datetime(df["date"]).dt.date

    events: List[Dict[str, Any]] = []

    for (term, geo), g in df.groupby(["term", "geo"], sort=False):
        g = g.sort_values("date")
        # Build series indexed by date
        s = pd.Series(g["value"].to_numpy(), index=pd.to_datetime(g["date"]), dtype=float)

        # Need enough history for compute_signal() (>=21 points) :contentReference[oaicite:4]{index=4}
        if len(s) < 21:
            continue

        # Slide "as_of" day by day:
        # Use each date as if it were "today" by passing s up to that point.
        idx = s.index.to_list()
        for j in range(20, len(idx)):  # 0-based; j=20 means 21st point
            as_of_ts = idx[j]
            as_of_date = as_of_ts.date()

            # Only keep events in reporting range (but still compute using warmup history)
            if as_of_date < report_start:
                continue

            window = s.loc[:as_of_ts]
            sig = compute_signal(window, term=term, geo=geo)
            if sig is None:
                continue

            if sig.severity not in only_severities:
                continue

            events.append({
                "as_of_date": as_of_date.isoformat(),
                "term": sig.term,
                "geo": sig.geo,
                "severity": sig.severity,
                "wow_change": float(sig.wow_change),
                "z_score": float(sig.z_score),
                "slope_7d": float(sig.slope_7d),
                "latest": float(sig.latest),
                # evidence from compute_signal :contentReference[oaicite:5]{index=5}
                "last7_avg": float(sig.evidence.get("last7_avg", 0.0)),
                "prev7_avg": float(sig.evidence.get("prev7_avg", 0.0)),
                "mu": float(sig.evidence.get("mu", 0.0)),
                "sigma": float(sig.evidence.get("sigma", 0.0)),
            })

    out = pd.DataFrame(events)
    if out.empty:
        return out

    # Sort: newest first, then stronger signal
    out = out.sort_values(
        ["as_of_date", "severity", "z_score", "wow_change"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    return out


def main():
    parser = argparse.ArgumentParser(description="Backfill RISING/BREAKOUT events from trend_series.")
    parser.add_argument("--months", type=int, default=3, help="Reporting window in months (approx by 30 days).")
    parser.add_argument("--warmup-days", type=int, default=70, help="Extra days of history before window.")
    parser.add_argument("--out", type=str, default="backfill_events_last3m.csv", help="Output CSV path.")
    parser.add_argument("--severity", type=str, default="RISING,BREAKOUT",
                        help="Comma-separated severities to keep (default: RISING,BREAKOUT)")
    args = parser.parse_args()

    severities = [s.strip().upper() for s in args.severity.split(",") if s.strip()]
    df = backfill_events(months=args.months, warmup_days=args.warmup_days, only_severities=severities)

    if df.empty:
        print("No events found in the window.")
        return

    df.to_csv(args.out, index=False, encoding="utf-8")
    print(f"Saved {len(df)} events → {args.out}")
    print(df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
