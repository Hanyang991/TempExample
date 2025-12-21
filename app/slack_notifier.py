from __future__ import annotations
import requests
from typing import Dict, Any, List

def post_webhook(webhook_url: str, payload: Dict[str, Any]) -> None:
    if not webhook_url:
        raise RuntimeError("SLACK_WEBHOOK_URL is empty.")
    r = requests.post(webhook_url, json=payload, timeout=15)
    r.raise_for_status()

def blocks_for_alert(
    severity: str,
    geo: str,
    term: str,
    expectation: str,
    why: str,
    action: str,
    metrics: Dict[str, Any],
):
    emoji = {"WATCH": "⚠️", "RISING": "🔥", "BREAKOUT": "🚨"}.get(severity, "📌")
    header = f"{emoji} {severity} | {term} ({geo})"
    wow = float(metrics.get("wow_change", 0.0)) * 100.0
    z = float(metrics.get("z_score", 0.0))

    return [
        {"type": "header", "text": {"type": "plain_text", "text": header}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*기대 포인트*\n{expectation}"},
            {"type": "mrkdwn", "text": f"*근거 지표*\nWoW {wow:.1f}% / z {z:.2f}"},
        ]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*왜 중요한가*\n{why}"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*추천 액션*\n{action}"}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": "(MVP) Google Trends 기반 자동 탐지"}]},
    ]

def send_alert(webhook_url: str, channel: str, blocks: List[Dict[str, Any]]):
    post_webhook(webhook_url, {"channel": channel, "blocks": blocks, "text": "K-beauty trend alert"})

def send_daily_summary(webhook_url: str, channel: str, text: str):
    post_webhook(webhook_url, {"channel": channel, "text": text})


