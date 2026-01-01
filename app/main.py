from __future__ import annotations
import sys
import yaml
from datetime import datetime, timezone, timedelta
from tqdm import tqdm
import warnings

from app.config import settings
from app.trends_provider import PyTrendsProvider
from app.detector import compute_signal
from app.insights import make_insight
from app.slack_notifier import blocks_for_alert, send_alert, send_daily_summary
from app.db import init_schema
from app.storage_pg import (
    upsert_trend_series, upsert_feature,
    fired_recently, log_alert, was_rising_last_week,
    get_top_features,
    upsert_hourly_snapshot, insert_hourly_snapshot_features,
    get_previous_snapshot_id, get_snapshot_feature_map, get_snapshot_top_features,
    compute_daily_rollup, upsert_daily_rollup,
    get_approved_terms,
    get_candidates_for_slack,   # ✅ 추가
)

warnings.filterwarnings("ignore", category=FutureWarning, module="pytrends")

KST = timezone(timedelta(hours=9))
SEVERITIES = ["EMERGING", "WATCH", "RISING", "BREAKOUT"]


def kst_hour_floor(dt: datetime) -> datetime:
    dt = dt.astimezone(KST)
    return dt.replace(minute=0, second=0, microsecond=0)


def load_seeds(path: str = "app/seeds.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_provider():
    return PyTrendsProvider(hl=settings.pytrends_hl, tz=settings.pytrends_tz)


def run():
    """
    ✅ DB 저장 전용 (Slack 발송 X)
    - trend_series 저장
    - trend_features 저장 (severity/evidence 포함)
    - daily summary는 그대로 보냄(요약 채널)
    """
    init_schema()

    cfg = load_seeds()
    geos = cfg["geos"]
    timeframe = cfg["timeframe"]

    terms: list[str] = []
    for _, arr in cfg["seed_groups"].items():
        terms.extend(arr)

    # ✅ 승인된 후보도 합치기 (중복 제거)
    terms = list(dict.fromkeys(terms + get_approved_terms(limit=500)))

    provider = get_provider()

    fired = {k: 0 for k in SEVERITIES}
    total_signals = 0
    today = datetime.now(KST).date().isoformat()

    for geo in tqdm(geos, desc="🌍 GEO 처리 중", unit="geo"):
        results = provider.interest_over_time(terms=terms, geo=geo, timeframe=timeframe)

        # (1) 원천 시계열 저장
        rows = []
        for r in results:
            s = r.series.dropna()
            for idx, val in s.items():
                rows.append((r.term, r.geo, idx.strftime("%Y-%m-%d"), float(val), "google_trends"))
        if rows:
            upsert_trend_series(rows)

        # (2) 탐지 + 피처 저장 (Slack 발송 X)
        for r in results:
            sig = compute_signal(r.series, term=r.term, geo=r.geo)
            if not sig:
                continue

            total_signals += 1

            # Breakout 품질: 최근 14일 내 Rising 이상 이력이 없으면 Breakout을 Rising으로 낮춤
            severity = sig.severity
            if severity == "BREAKOUT" and not was_rising_last_week(sig.term, sig.geo, today):
                severity = "RISING"

            upsert_feature(
                term=sig.term,
                geo=sig.geo,
                as_of_date=today,
                wow=sig.wow_change,
                z=sig.z_score,
                slope=sig.slope_7d,
                latest=sig.latest,
                severity=sig.severity,          # ✅ 추가
                evidence=sig.evidence,          # ✅ 추가 (없으면 제거 가능)
            )

            fired[severity] = fired.get(severity, 0) + 1

    # ✅ TOP 조회 (요약용)
    top = get_top_features(
        as_of_date=today,
        limit=5,
        severities=["EMERGING", "WATCH", "RISING"],
        min_latest=2.0,
        prefer_positive_slope=True,
    )

    lines = []
    lines.append(f"📌 오늘의 글로벌 K-beauty 트렌드 요약 ({today})")
    lines.append(
        f"- BREAKOUT {fired['BREAKOUT']} / RISING {fired['RISING']} / WATCH {fired['WATCH']} / EMERGING {fired['EMERGING']}"
    )
    lines.append(f"- 탐지 후보 수(total signals): {total_signals}")
    lines.append("")

    if top:
        lines.append("🔎 오늘의 EMERGING/WATCH/RISING 후보 TOP 5 (z-score 기준)")
        for r in top:
            wow_pct = float(r["wow_change"]) * 100.0
            card = make_insight(r["term"])
            sev = r.get("severity", "WATCH")
            lines.append(
                f"- {sev} | {r['geo']} | {r['term']}  "
                f"(WoW {wow_pct:+.0f}%, z {r['z_score']:.2f}, slope {r['slope_7d']:.2f}, latest {r['latest']:.0f})\n"
                f"  · 기대 포인트: {card.expectation}"
            )
    else:
        lines.append("오늘은 trend_features가 비어 있습니다. (수집/피처 계산/저장 확인 필요)")

    lines.append("")
    lines.append("(DB) Postgres: trend_series / trend_features / alerts 저장")

    summary = "\n".join(lines)
    send_daily_summary(settings.slack_webhook_url, settings.slack_channel_daily, summary)


def run_hourly():
    init_schema()

    cfg = load_seeds()
    geos = cfg["geos"]
    timeframe = cfg["timeframe"]

    terms: list[str] = []
    for _, arr in cfg["seed_groups"].items():
        terms.extend(arr)
    terms = list(dict.fromkeys(terms + get_approved_terms(limit=500)))

    now_kst = datetime.now(KST)
    today = now_kst.date().isoformat()
    snap_at = kst_hour_floor(now_kst).isoformat()

    sid = upsert_hourly_snapshot(
        snapshot_at_iso=snap_at,
        geo_count=len(geos),
        term_count=len(terms),
        timeframe=timeframe,
    )

    top = get_top_features(
        as_of_date=today,
        limit=10,
        severities=["EMERGING", "WATCH", "RISING", "BREAKOUT"],
        min_latest=2.0,
        prefer_positive_slope=True,
    )

    hrows = []
    for r in top:
        sev = r.get("severity") or "WATCH"
        hrows.append({
            "term": r["term"],
            "geo": r["geo"],
            "wow_change": float(r["wow_change"]),
            "z_score": float(r["z_score"]),
            "slope_7d": float(r["slope_7d"]),
            "latest": float(r["latest"]),
            "severity": sev,
        })

    insert_hourly_snapshot_features(sid, hrows)

    # (선택) hourly 변화 감지 알림은 "DB only slack 발송" 정책이면 여기서 보내지 않는 게 깔끔함.
    # 필요하면 send_slack_from_db()를 더 자주 돌려서 해결 가능.


def run_daily(report_date: str | None = None):
    init_schema()
    if report_date is None:
        report_date = datetime.now(KST).date().isoformat()

    roll = compute_daily_rollup(report_date=report_date, min_support=2, limit=10)

    lines = []
    lines.append(f"📌 Daily 글로벌 K-beauty 트렌드 종합 ({roll['report_date']})")
    lines.append(f"- 기준: hourly 스냅샷 집계 (support≥{roll['min_support']})")
    lines.append("")

    top = roll["top"]
    if not top:
        lines.append("오늘은 종합할 신호가 없습니다.")
    else:
        for r in top:
            card = make_insight(r["term"])
            wow_pct = r["median_wow"] * 100.0
            lines.append(
                f"- {r['severity_day']} | {r['geo']} | {r['term']} "
                f"(max z {r['max_z']:.2f}, median WoW {wow_pct:+.0f}%, support {r['support']})\n"
                f"  · 기대 포인트: {card.expectation}"
            )

    text = "\n".join(lines)
    upsert_daily_rollup(report_date, {"text": text, **roll})
    send_daily_summary(settings.slack_webhook_url, settings.slack_channel_daily, text)


def send_slack_from_db(as_of_date: str | None = None):
    """
    ✅ Slack 알림은 DB만 보고 발송 (SSOT)
    - trend_features에서 후보 조회
    - cooldown은 alerts 테이블로 제어
    """
    init_schema()

    if as_of_date is None:
        as_of_date = datetime.now(KST).date().isoformat()

    severities = ["BREAKOUT", "RISING", "EMERGING"]  # WATCH는 summary로만
    candidates = get_candidates_for_slack(
        as_of_date=as_of_date,
        severities=severities,
        limit=20,
        min_latest=2.0,
    )

    for c in candidates:
        sev = c["severity"]
        term = c["term"]
        geo = c["geo"]

        cooldown = 72 if sev == "BREAKOUT" else (12 if sev == "RISING" else 6)
        if fired_recently(term, geo, sev, cooldown_hours=cooldown):
            continue

        card = make_insight(term)
        blocks = blocks_for_alert(
            severity=sev,
            geo=geo,
            term=term,
            expectation=card.expectation,
            why=card.why,
            action=card.action,
            metrics={
                "wow_change": c["wow_change"],
                "z_score": c["z_score"],
                "slope_7d": c.get("slope_7d", 0.0),
                "latest": c.get("latest", 0.0),
                "evidence": c.get("evidence", {}),
            },
        )

        send_alert(settings.slack_webhook_url, settings.slack_channel_alert, blocks)
        log_alert(term, geo, sev, slack_channel=settings.slack_channel_alert, cooldown_hours=cooldown)


def _usage():
    return (
        "Usage:\n"
        "  python -m app.main run\n"
        "  python -m app.main hourly\n"
        "  python -m app.main daily\n"
        "  python -m app.main slack\n"
        "  python -m app.main slack YYYY-MM-DD\n"
    )


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"

    if cmd == "run":
        run()
    elif cmd == "hourly":
        run_hourly()
    elif cmd == "daily":
        run_daily()
    elif cmd == "slack":
        date_arg = sys.argv[2] if len(sys.argv) > 2 else None
        send_slack_from_db(date_arg)
    else:
        print(_usage())
        raise SystemExit(2)
