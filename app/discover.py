# app/discover.py
from __future__ import annotations

import argparse
import yaml
from typing import Dict, Any, List
from datetime import datetime

from app.db import init_schema
from app.config import settings
from app.trends_provider import PyTrendsProvider
from app.storage_pg import upsert_discovered_terms


def load_seeds(path: str = "app/seeds.yaml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def discover_related_queries(
    terms: List[str],
    geo: str,
    timeframe: str,
    max_per_term: int = 10,
) -> List[Dict[str, Any]]:
    """
    related_queries는 multi-keyword payload에서 400이 자주 나므로
    ✅ term 1개씩 build_payload → related_queries 호출로 안정화
    """
    p = PyTrendsProvider(hl=settings.pytrends_hl, tz=settings.pytrends_tz)
    pt = p.pytrends

    rows: List[Dict[str, Any]] = []

    for source_term in terms:
        # ✅ provider에 있는 429 대응 sleep/jitter 사용
        p._sleep_jitter(p.base_sleep)

        attempt = 0
        while True:
            try:
                pt.build_payload([source_term], timeframe=timeframe, geo=geo)
                rq = pt.related_queries() or {}
                break
            except Exception as e:
                msg = str(e)
                is_429 = ("429" in msg) or ("TooManyRequests" in e.__class__.__name__)
                attempt += 1

                # 400은 보통 "이 조합 불가/무효"라 재시도해봐야 소용 없음 → skip
                if " 400" in msg or "code 400" in msg or "returned a response with code 400" in msg:
                    rq = {}
                    break

                if (not is_429) or (attempt > p.retries):
                    # 다른 에러는 그대로 올려서 원인 보이게
                    raise

                wait = (2 ** (attempt - 1)) * 4.0
                p._sleep_jitter(wait)

        bundle = rq.get(source_term)
        if not bundle:
            continue

        for kind in ("rising", "top"):
            df = bundle.get(kind)
            if df is None or getattr(df, "empty", True):
                continue

            df = df.head(max_per_term)

            for rank, r in enumerate(df.itertuples(index=False), start=1):
                query = getattr(r, "query", None) or (r[0] if len(r) > 0 else None)
                value = getattr(r, "value", None) or (r[1] if len(r) > 1 else None)
                if not query:
                    continue

                rows.append({
                    "term": str(query),
                    "geo": geo,
                    "source_term": str(source_term),
                    "kind": f"related_queries_{kind}",
                    "rank": int(rank),
                    "score": float(value) if value is not None else None,
                    "status": "new",
                })

    return rows



def main():
    parser = argparse.ArgumentParser(description="Discover new keywords via Google Trends related queries.")
    parser.add_argument("--max-per-term", type=int, default=10)
    parser.add_argument("--seed-limit", type=int, default=30, help="limit #seed terms per group to reduce load")
    parser.add_argument("--geos", type=str, default="", help="comma separated geos override (e.g. US,JP)")
    args = parser.parse_args()

    init_schema()

    cfg = load_seeds()
    timeframe = cfg["timeframe"]

    # seeds.yaml의 모든 그룹 키워드 합치기
    all_terms: List[str] = []
    for _, arr in cfg["seed_groups"].items():
        all_terms.extend(arr)

    # 너무 많은 seed로 related_queries 호출하면 부담되므로 MVP는 제한 추천
    all_terms = all_terms[: args.seed_limit]

    geos = cfg["geos"]
    if args.geos.strip():
        geos = [x.strip() for x in args.geos.split(",") if x.strip()]

    total = 0
    for geo in geos:
        rows = discover_related_queries(
            terms=all_terms,
            geo=geo,
            timeframe=timeframe,
            max_per_term=args.max_per_term,
        )
        upsert_discovered_terms(rows)
        total += len(rows)
        print(f"[{geo}] discovered rows: {len(rows)}")

    print(f"done. total discovered rows: {total}")


if __name__ == "__main__":
    main()
