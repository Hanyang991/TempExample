from __future__ import annotations
import yaml
from datetime import datetime
from app.config import settings
from app.trends_provider import PyTrendsProvider
from app.detector import compute_signal
from app.insights import make_insight
from app.slack_notifier import blocks_for_alert, send_alert, send_daily_summary
from app.db import init_schema
from app.storage_pg import (
    upsert_trend_series, upsert_feature,
    fired_recently, log_alert, was_rising_last_week,
    get_top_features,  # 기존 daily top 조회 있으면 유지(선택)
    upsert_hourly_snapshot, insert_hourly_snapshot_features,
    get_previous_snapshot_id, get_snapshot_feature_map, get_snapshot_top_features,
    compute_daily_rollup, upsert_daily_rollup
)
from datetime import datetime, timezone, timedelta
from tqdm import tqdm
from app.storage_pg import get_approved_terms

import warnings
warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    module="pytrends"
)

KST = timezone(timedelta(hours=9))

def kst_hour_floor(dt: datetime) -> datetime:
    dt = dt.astimezone(KST)
    return dt.replace(minute=0, second=0, microsecond=0)

def load_seeds(path: str = "app/seeds.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_provider():
    return PyTrendsProvider(
        hl=settings.pytrends_hl,
        tz=settings.pytrends_tz
    )


def run():
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

    fired = {"BREAKOUT": 0, "RISING": 0, "WATCH": 0}
    total_signals = 0
    today = datetime.now().strftime("%Y-%m-%d")

    for geo in tqdm(geos, desc="🌍 GEO 처리 중", unit="geo"):
        results = provider.interest_over_time(
            terms=terms,
            geo=geo,
            timeframe=timeframe
        )

        # (1) 원천 시계열 저장
        rows = []
        for r in results:
            s = r.series.dropna()
            for idx, val in s.items():
                rows.append((r.term, r.geo, idx.strftime("%Y-%m-%d"), float(val), "google_trends"))
        if rows:
            upsert_trend_series(rows)

        # (2) 탐지 + 피처 저장 + 알림
        for r in results:
            sig = compute_signal(r.series, term=r.term, geo=r.geo)
            if not sig:
                continue

            total_signals += 1

            upsert_feature(
                term=sig.term,
                geo=sig.geo,
                as_of_date=today,
                wow=sig.wow_change,
                z=sig.z_score,
                slope=sig.slope_7d,
                latest=sig.latest,
            )

            # Breakout 품질: 최근 14일 내 Rising 이상 이력이 없으면 Breakout을 Rising으로 낮춤
            severity = sig.severity
            if severity == "BREAKOUT" and not was_rising_last_week(sig.term, sig.geo, today):
                severity = "RISING"

            fired[severity] += 1

            # Breakout만 즉시 알림 (Rising/Watch는 daily summary로만)
            if severity == "BREAKOUT":
                if fired_recently(sig.term, sig.geo, severity, cooldown_hours=72):
                    continue

                card = make_insight(sig.term)
                blocks = blocks_for_alert(
                    severity=severity,
                    geo=sig.geo,
                    term=sig.term,
                    expectation=card.expectation,
                    why=card.why,
                    action=card.action,
                    metrics={"wow_change": sig.wow_change, "z_score": sig.z_score},
                )
                send_alert(settings.slack_webhook_url, settings.slack_channel_alert, blocks)
                log_alert(sig.term, sig.geo, severity, slack_channel=settings.slack_channel_alert, cooldown_hours=72)

    top = get_top_features(as_of_date=today, limit=5)

    lines = []
    lines.append(f"📌 오늘의 글로벌 K-beauty 트렌드 요약 ({today})")
    lines.append(f"- BREAKOUT {fired['BREAKOUT']} / RISING {fired['RISING']} / WATCH {fired['WATCH']}")
    lines.append(f"- 탐지 후보 수(total signals): {total_signals}")
    lines.append("")

    if top:
        lines.append("🔎 오늘의 WATCH/RISING 후보 TOP 5 (z-score 기준)")
        for r in top:
            wow_pct = r["wow_change"] * 100.0
            # InsightCard 규칙 기반 기대포인트(간단) 붙이기
            card = make_insight(r["term"])
            lines.append(
                f"- {r['geo']} | {r['term']}  "
                f"(WoW {wow_pct:+.0f}%, z {r['z_score']:.2f}, slope {r['slope_7d']:.2f}, latest {r['latest']:.0f})\n"
                f"  · 기대 포인트: {card.expectation}"
            )
    else:
        lines.append("오늘은 trend_features가 비어 있습니다. (수집/피처 계산 확인 필요)")

    lines.append("")
    lines.append("(DB) Postgres: trend_series / trend_features / alerts 저장")

    summary = "\n".join(lines)
    send_daily_summary(settings.slack_webhook_url, settings.slack_channel_daily, summary)

def run_hourly():
    init_schema()

    # (기존) cfg 로드, provider로 수집, upsert_trend_series, upsert_feature,
    # (기존) signals 계산 로직까지 수행한다고 가정
    # 여기서 signals/top은 trend_features 기반으로 이미 계산됐다고 치자.

    now_kst = datetime.now(KST)
    snap_at = kst_hour_floor(now_kst).isoformat()

    # 스냅샷 헤더 저장
    sid = upsert_hourly_snapshot(
        snapshot_at_iso=snap_at,
        geo_count=len(geos),
        term_count=len(terms),
        timeframe=timeframe
    )

    # 이번 실행에서 “오늘 기준 top 후보”를 DB에서 다시 가져오든,
    # 이미 계산된 signals를 rows로 만들든 둘 중 하나면 됨.
    # 여기서는 trend_features(today) 기반 top을 사용했다고 가정:
    top = get_top_features(as_of_date=today, limit=10)

    # hourly_snapshot_features 저장용 rows 생성
    hrows = []
    for r in top:
        sev = "WATCH"
        # 기존 detector 결과가 있다면 그 값을 쓰고, 없으면 간단한 rule로 severity 추정
        # (권장: 너의 detector 결과 severity를 그대로 넣기)
        if r["z_score"] >= 2.2 and r["wow_change"] >= 0.30 and r["slope_7d"] > 0:
            sev = "BREAKOUT"
        elif r["z_score"] >= 1.6 and r["wow_change"] >= 0.18 and r["slope_7d"] > 0:
            sev = "RISING"
        else:
            sev = "WATCH"

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

    # ----- 변화 감지(Delta) & Slack 알림 -----
    prev_id = get_previous_snapshot_id(snap_at)
    prev_map = get_snapshot_feature_map(prev_id) if prev_id else {}

    # 이번 스냅샷 top 중 변화가 큰 것만 추림
    candidates = []
    for cur in get_snapshot_top_features(sid, limit=10):
        key = f"{cur['term']}|{cur['geo']}"
        prev = prev_map.get(key)

        is_new = prev is None
        z_delta = cur["z_score"] - (prev["z_score"] if prev else 0.0)

        # 알림 기준 (스팸 줄이기)
        should_notify = False
        tag = None
        if cur["severity"] in ("BREAKOUT", "RISING"):
            should_notify = True
            tag = "NEW" if is_new else ("UP" if z_delta >= 0.5 else "HIT")
        elif is_new and cur["z_score"] >= 1.8:
            should_notify = True
            tag = "NEW"
        elif (not is_new) and z_delta >= 0.5 and cur["z_score"] >= 1.5:
            should_notify = True
            tag = "UP"

        if should_notify:
            candidates.append((tag, cur, z_delta))

    # cooldown 6시간: 같은 term-geo-severity 알림 반복 방지
    notify_lines = []
    for tag, cur, z_delta in candidates[:5]:
        if fired_recently(cur["term"], cur["geo"], cur["severity"], cooldown_hours=6):
            continue

        card = make_insight(cur["term"])
        wow_pct = cur["wow_change"] * 100.0
        notify_lines.append(
            f"- [{tag}] {cur['geo']} | {cur['term']} "
            f"(z {cur['z_score']:.2f}, Δz {z_delta:+.2f}, WoW {wow_pct:+.0f}%, latest {cur['latest']:.0f})\n"
            f"  · 기대 포인트: {card.expectation}"
        )

        # 알림 기록(재발송 방지)
        log_alert(cur["term"], cur["geo"], cur["severity"],
                  slack_channel=settings.slack_channel_alert, cooldown_hours=6)

    if notify_lines:
        header = f"⏱️ 시간대 트렌드 업데이트 ({snap_at})"
        msg = header + "\n" + "\n".join(notify_lines)
        send_alert(settings.slack_webhook_url, settings.slack_channel_alert, msg)

def run_daily(report_date: str | None = None):
    init_schema()
    if report_date is None:
        report_date = datetime.now(KST).date().isoformat()

    roll = compute_daily_rollup(report_date=report_date, min_support=2, limit=10)

    # Slack용 텍스트 생성
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


if __name__ == "__main__":
    run()
