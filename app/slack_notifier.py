from __future__ import annotations
import requests
from typing import Dict, Any, List, Optional

def post_webhook(webhook_url: str, payload: Dict[str, Any]) -> None:
    if not webhook_url:
        raise RuntimeError("SLACK_WEBHOOK_URL is empty.")
    r = requests.post(webhook_url, json=payload, timeout=15)
    r.raise_for_status()

def _sev_meta(severity: str) -> Dict[str, str]:
    # EMERGING 추가 + 기본 문구
    emoji = {
        "EMERGING": "⚡",
        "WATCH": "⚠️",
        "RISING": "🔥",
        "BREAKOUT": "🚨",
    }.get(severity, "📌")

    label = {
        "EMERGING": "EARLY SIGNAL",
        "WATCH": "WATCH",
        "RISING": "RISING",
        "BREAKOUT": "BREAKOUT",
    }.get(severity, severity)

    return {"emoji": emoji, "label": label}

def _default_copy(severity: str, term: str) -> Dict[str, str]:
    """
    expectation/why/action을 호출부에서 안 넣어도 되는 기본 템플릿.
    (원하면 너 프로젝트 톤에 맞게 문장만 바꾸면 됨)
    """
    if severity == "EMERGING":
        return {
            "expectation": f"'{term}' 관심이 막 살아나는 구간. 24~72시간 내 추가 확산 가능성 체크.",
            "why": "초기 급등은 콘텐츠/바이럴/이슈 트리거 가능성이 높아 선제 대응 가치가 큼.",
            "action": "TikTok/IG/YouTube에서 관련 키워드·해시태그·크리에이터 동향 확인 → 소재/카피 후보 수집.",
        }
    if severity == "WATCH":
        return {
            "expectation": f"'{term}' 수요가 평소 대비 움직임. 추가 상승 시 RISING 전환 가능.",
            "why": "초기 반응이 잡히면 제품/콘텐츠 기획 리드타임을 확보할 수 있음.",
            "action": "연관 키워드/추천 검색어 확장 조사 + 경쟁사/리테일 검색 결과 스냅샷 저장.",
        }
    if severity == "RISING":
        return {
            "expectation": f"'{term}' 상승 추세가 확인됨. 단기적으로 관심 확대 가능.",
            "why": "상승 구간에서 선점하면 광고/콘텐츠 효율이 좋아지는 구간을 놓치지 않음.",
            "action": "콘텐츠 1~2개 빠른 제작(훅/전후/루틴) + 랜딩/상품 상세페이지 문구 업데이트 후보 준비.",
        }
    if severity == "BREAKOUT":
        return {
            "expectation": f"'{term}' 급등 구간. 빠르게 확산될 확률 높음.",
            "why": "폭발 구간은 트래픽/전환이 몰리기 쉬워 실행 속도가 곧 성과로 연결됨.",
            "action": "우선순위 상향(캠페인/재고/SEO/크리에이터 협업) + 유사 키워드 번들링으로 확장.",
        }
    return {
        "expectation": f"'{term}' 변화 감지.",
        "why": "모니터링 필요.",
        "action": "추가 확인.",
    }

def _fmt_pct(x: Any) -> str:
    try:
        return f"{float(x) * 100.0:.1f}%"
    except Exception:
        return "n/a"

def _fmt_num(x: Any, nd: int = 2) -> str:
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return "n/a"

def blocks_for_alert(
    severity: str,
    geo: str,
    term: str,
    expectation: Optional[str],
    why: Optional[str],
    action: Optional[str],
    metrics: Dict[str, Any],
):
    meta = _sev_meta(severity)
    header = f"{meta['emoji']} {meta['label']} | {term} ({geo})"

    # 기존 지표
    wow = _fmt_pct(metrics.get("wow_change", 0.0))
    z = _fmt_num(metrics.get("z_score", 0.0), 2)
    slope = _fmt_num(metrics.get("slope_7d", 0.0), 2)
    latest = _fmt_num(metrics.get("latest", 0.0), 0)

    # early evidence (있으면 표시)
    ev = metrics.get("evidence", {}) if isinstance(metrics.get("evidence", {}), dict) else {}
    has_early = any(k in ev for k in ("last3_avg", "spike_3v14", "dod_delta", "accel_2d", "nonzero_streak_14d", "revived_0_to_nonzero"))

    # expectation/why/action 기본값 채우기
    defaults = _default_copy(severity, term)
    expectation = expectation or defaults["expectation"]
    why = why or defaults["why"]
    action = action or defaults["action"]

    blocks: List[Dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": header}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*기대 포인트*\n{expectation}"},
            {"type": "mrkdwn", "text": f"*핵심 지표*\nWoW {wow}\nz {z}\nslope7d {slope}\nlatest {latest}"},
        ]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*왜 중요한가*\n{why}"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*추천 액션*\n{action}"}},
    ]

    if has_early:
        last3 = _fmt_num(ev.get("last3_avg"), 1)
        prev14 = _fmt_num(ev.get("prev14_avg_excl_last3"), 1)
        spike = _fmt_pct(ev.get("spike_3v14"))
        dod = _fmt_num(ev.get("dod_delta"), 1)
        accel = _fmt_num(ev.get("accel_2d"), 1)
        streak = str(ev.get("nonzero_streak_14d", "n/a"))
        revived = "yes" if bool(ev.get("revived_0_to_nonzero", False)) else "no"

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text":
                "*Early signal evidence*\n"
                f"• last3 avg: {last3} / prev14 avg: {prev14} (Δ {spike})\n"
                f"• DoD Δ: {dod} / accel: {accel}\n"
                f"• non-zero streak(14d): {streak} / revived: {revived}"
            }
        })

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "(MVP) Google Trends 기반 자동 탐지 + Early signal(EMERGING)"}],
    })

    return blocks

def send_alert(webhook_url: str, channel: str, blocks: List[Dict[str, Any]]):
    # text를 blocks header와 최대한 맞추면 모바일/알림 프리뷰가 좋아짐
    # (blocks[0]이 header라는 가정)
    fallback = "K-beauty trend alert"
    try:
        header_txt = blocks[0]["text"]["text"]
        fallback = header_txt
    except Exception:
        pass

    post_webhook(
        webhook_url,
        {"channel": channel, "blocks": blocks, "text": fallback}
    )

def send_daily_summary(webhook_url: str, channel: str, text: str):
    post_webhook(webhook_url, {"channel": channel, "text": text})
