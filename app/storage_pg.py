from __future__ import annotations
from datetime import datetime, timedelta
from typing import Iterable, Tuple
from sqlalchemy import text
from app.db import engine
from typing import List, Dict, Any, Optional
import json

def get_top_features(as_of_date: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    오늘(as_of_date) 기준 z_score 내림차순 TOP N 가져오기
    """
    q = text("""
        SELECT term, geo, wow_change, z_score, slope_7d, latest
        FROM trend_features
        WHERE as_of_date = :as_of_date
        ORDER BY z_score DESC
        LIMIT :limit
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"as_of_date": as_of_date, "limit": limit}).fetchall()

    return [
        {
            "term": r[0],
            "geo": r[1],
            "wow_change": float(r[2]),
            "z_score": float(r[3]),
            "slope_7d": float(r[4]),
            "latest": float(r[5]),
        }
        for r in rows
    ]


def upsert_trend_series(rows: Iterable[Tuple[str, str, str, float, str]]):
    """
    rows: (term, geo, date_iso(YYYY-MM-DD), value, source)
    """
    q = text("""
        INSERT INTO trend_series(term, geo, date, value, source)
        VALUES (:term, :geo, :date, :value, :source)
        ON CONFLICT (term, geo, date)
        DO UPDATE SET value=EXCLUDED.value, collected_at=NOW();
    """)

    payload = [
        {"term": t, "geo": g, "date": d, "value": v, "source": s}
        for (t, g, d, v, s) in rows
    ]
    if not payload:
        return

    with engine.begin() as conn:
        conn.execute(q, payload)

def upsert_feature(term: str, geo: str, as_of_date: str, wow: float, z: float, slope: float, latest: float):
    q = text("""
        INSERT INTO trend_features(term, geo, as_of_date, wow_change, z_score, slope_7d, latest)
        VALUES (:term, :geo, :as_of_date, :wow, :z, :slope, :latest)
        ON CONFLICT (term, geo, as_of_date)
        DO UPDATE SET wow_change=EXCLUDED.wow_change,
                    z_score=EXCLUDED.z_score,
                    slope_7d=EXCLUDED.slope_7d,
                    latest=EXCLUDED.latest,
                    computed_at=NOW();
    """)
    with engine.begin() as conn:
        conn.execute(q, {
            "term": term, "geo": geo, "as_of_date": as_of_date,
            "wow": wow, "z": z, "slope": slope, "latest": latest
        })

def fired_recently(term: str, geo: str, severity: str, cooldown_hours: int = 72) -> bool:
    q = text("""
        SELECT fired_at FROM alerts
        WHERE term=:term AND geo=:geo AND severity=:severity
        ORDER BY fired_at DESC LIMIT 1
    """)
    with engine.begin() as conn:
        row = conn.execute(q, {"term": term, "geo": geo, "severity": severity}).fetchone()
    if not row:
        return False
    last = row[0].replace(tzinfo=None)
    return datetime.utcnow() - last < timedelta(hours=cooldown_hours)

def log_alert(term: str, geo: str, severity: str, slack_channel: str | None = None, slack_ts: str | None = None, cooldown_hours: int = 72):
    q = text("""
        INSERT INTO alerts(term, geo, severity, slack_channel, slack_ts, cooldown_until)
        VALUES (:term, :geo, :severity, :slack_channel, :slack_ts,
                NOW() + (:cooldown || ' hours')::interval)
    """)
    with engine.begin() as conn:
        conn.execute(q, {
            "term": term, "geo": geo, "severity": severity,
            "slack_channel": slack_channel, "slack_ts": slack_ts,
            "cooldown": cooldown_hours
        })

def was_rising_last_week(term: str, geo: str, as_of_date: str) -> bool:
    q = text("""
        SELECT 1 FROM alerts
        WHERE term=:term AND geo=:geo
        AND fired_at >= (:as_of_date::date - INTERVAL '14 days')
        AND fired_at <  (:as_of_date::date)
        AND severity IN ('RISING','BREAKOUT')
        LIMIT 1
    """)
    with engine.begin() as conn:
        row = conn.execute(q, {"term": term, "geo": geo, "as_of_date": as_of_date}).fetchone()
    return row is not None


def upsert_hourly_snapshot(snapshot_at_iso: str, geo_count: int, term_count: int, timeframe: str) -> int:
    """
    snapshot_at_iso: ISO8601 (예: 2025-12-20T16:00:00+09:00)
    같은 snapshot_at이면 이미 존재하는 id 반환
    """
    q = text("""
      INSERT INTO hourly_snapshots(snapshot_at, geo_count, term_count, timeframe)
      VALUES (:snapshot_at, :geo_count, :term_count, :timeframe)
      ON CONFLICT (snapshot_at) DO UPDATE SET
        geo_count=EXCLUDED.geo_count,
        term_count=EXCLUDED.term_count,
        timeframe=EXCLUDED.timeframe
      RETURNING id;
    """)
    with engine.begin() as conn:
        sid = conn.execute(q, {
            "snapshot_at": snapshot_at_iso,
            "geo_count": geo_count,
            "term_count": term_count,
            "timeframe": timeframe
        }).scalar_one()
    return int(sid)

def insert_hourly_snapshot_features(snapshot_id: int, rows: List[Dict[str, Any]]):
    """
    rows: [{term, geo, wow_change, z_score, slope_7d, latest, severity}, ...]
    """
    if not rows:
        return
    q = text("""
      INSERT INTO hourly_snapshot_features(
        snapshot_id, term, geo, wow_change, z_score, slope_7d, latest, severity
      ) VALUES (
        :snapshot_id, :term, :geo, :wow_change, :z_score, :slope_7d, :latest, :severity
      )
      ON CONFLICT (snapshot_id, term, geo) DO UPDATE SET
        wow_change=EXCLUDED.wow_change,
        z_score=EXCLUDED.z_score,
        slope_7d=EXCLUDED.slope_7d,
        latest=EXCLUDED.latest,
        severity=EXCLUDED.severity;
    """)
    payload = [{"snapshot_id": snapshot_id, **r} for r in rows]
    with engine.begin() as conn:
        conn.execute(q, payload)

def get_previous_snapshot_id(snapshot_at_iso: str) -> Optional[int]:
    q = text("""
      SELECT id
      FROM hourly_snapshots
      WHERE snapshot_at < :t
      ORDER BY snapshot_at DESC
      LIMIT 1;
    """)
    with engine.begin() as conn:
        row = conn.execute(q, {"t": snapshot_at_iso}).fetchone()
    return int(row[0]) if row else None

def get_snapshot_top_features(snapshot_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    q = text("""
      SELECT term, geo, wow_change, z_score, slope_7d, latest, severity
      FROM hourly_snapshot_features
      WHERE snapshot_id = :sid
      ORDER BY
        CASE severity WHEN 'BREAKOUT' THEN 3 WHEN 'RISING' THEN 2 ELSE 1 END DESC,
        z_score DESC
      LIMIT :limit;
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"sid": snapshot_id, "limit": limit}).fetchall()

    return [{
        "term": r[0],
        "geo": r[1],
        "wow_change": float(r[2]),
        "z_score": float(r[3]),
        "slope_7d": float(r[4]),
        "latest": float(r[5]),
        "severity": r[6],
    } for r in rows]

def get_snapshot_feature_map(snapshot_id: int) -> Dict[str, Dict[str, Any]]:
    """
    key = term|geo
    """
    q = text("""
      SELECT term, geo, wow_change, z_score, slope_7d, latest, severity
      FROM hourly_snapshot_features
      WHERE snapshot_id = :sid;
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"sid": snapshot_id}).fetchall()
    m = {}
    for r in rows:
        key = f"{r[0]}|{r[1]}"
        m[key] = {
            "term": r[0], "geo": r[1],
            "wow_change": float(r[2]),
            "z_score": float(r[3]),
            "slope_7d": float(r[4]),
            "latest": float(r[5]),
            "severity": r[6],
        }
    return m

def upsert_daily_rollup(report_date: str, payload: Dict[str, Any]):
    q = text("""
      INSERT INTO daily_rollups(report_date, payload_json)
      VALUES (:d, CAST(:p AS jsonb))
      ON CONFLICT (report_date) DO UPDATE SET
        payload_json=EXCLUDED.payload_json,
        updated_at=NOW();
    """)
    with engine.begin() as conn:
        conn.execute(q, {"d": report_date, "p": json.dumps(payload)})

def get_daily_rollup(report_date: str) -> Optional[Dict[str, Any]]:
    q = text("SELECT payload_json FROM daily_rollups WHERE report_date=:d;")
    with engine.begin() as conn:
        row = conn.execute(q, {"d": report_date}).fetchone()
    return row[0] if row else None

def compute_daily_rollup(report_date: str, tz_offset: str = "+09:00",
                         min_support: int = 2, limit: int = 10) -> Dict[str, Any]:
    """
    report_date: 'YYYY-MM-DD' (Asia/Seoul 기준)
    tz_offset: '+09:00'
    """
    # Seoul 기준 하루 범위를 timestamptz로 계산
    q = text(f"""
    WITH bounds AS (
      SELECT
        (('{report_date}'::date)::timestamptz + TIME '00:00') AT TIME ZONE '{tz_offset}' AS start_ts,
        (('{report_date}'::date + 1)::timestamptz + TIME '00:00') AT TIME ZONE '{tz_offset}' AS end_ts
    ),
    base AS (
      SELECT hsf.term, hsf.geo, hsf.wow_change, hsf.z_score, hsf.slope_7d, hsf.latest, hsf.severity
      FROM hourly_snapshot_features hsf
      JOIN hourly_snapshots hs ON hs.id = hsf.snapshot_id
      JOIN bounds b ON hs.snapshot_at >= b.start_ts AND hs.snapshot_at < b.end_ts
    ),
    agg AS (
      SELECT
        term,
        geo,
        COUNT(*) AS support,
        MAX(z_score) AS max_z,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY wow_change) AS median_wow,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY slope_7d)  AS median_slope,
        MAX(latest) AS max_latest,
        SUM(CASE WHEN severity='BREAKOUT' THEN 1 ELSE 0 END) AS breakout_hits,
        SUM(CASE WHEN severity='RISING' THEN 1 ELSE 0 END) AS rising_hits
      FROM base
      GROUP BY term, geo
    ),
    ranked AS (
      SELECT *,
        CASE
          WHEN breakout_hits >= 1 THEN 'BREAKOUT'
          WHEN rising_hits >= 2 THEN 'RISING'
          ELSE 'WATCH'
        END AS severity_day
      FROM agg
      WHERE support >= :min_support
    )
    SELECT term, geo, support, max_z, median_wow, median_slope, max_latest, severity_day
    FROM ranked
    ORDER BY
      CASE severity_day WHEN 'BREAKOUT' THEN 3 WHEN 'RISING' THEN 2 ELSE 1 END DESC,
      max_z DESC
    LIMIT :limit;
    """)

    with engine.begin() as conn:
        rows = conn.execute(q, {"min_support": min_support, "limit": limit}).fetchall()

    items = []
    for r in rows:
        items.append({
            "term": r[0], "geo": r[1],
            "support": int(r[2]),
            "max_z": float(r[3]),
            "median_wow": float(r[4]),
            "median_slope": float(r[5]),
            "max_latest": float(r[6]),
            "severity_day": r[7],
        })

    return {
        "report_date": report_date,
        "top": items,
        "min_support": min_support,
    }