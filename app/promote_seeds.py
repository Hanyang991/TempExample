# app/promote_seeds.py
from __future__ import annotations

import argparse
from typing import Dict, Any, List, Tuple

import yaml
from sqlalchemy import text

from app.db import engine, init_schema


def load_seeds(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def dump_seeds(path: str, data: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


def normalize_term(t: str) -> str:
    # "겹치지 않게"를 좀 더 강하게: 공백 정리 + 소문자
    return " ".join(t.strip().lower().split())


def existing_seed_terms(cfg: Dict[str, Any]) -> Tuple[List[str], set]:
    seed_groups = cfg.get("seed_groups", {}) or {}
    all_terms: List[str] = []
    for _, arr in seed_groups.items():
        if not arr:
            continue
        all_terms.extend([str(x) for x in arr])

    norm_set = {normalize_term(t) for t in all_terms}
    return all_terms, norm_set


def fetch_top_new(limit: int = 20) -> List[str]:
    q = text("""
      SELECT
        term,
        COUNT(DISTINCT geo) AS geo_cnt,
        MAX(score) AS max_score,
        MAX(last_seen) AS last_seen
      FROM discovered_terms
      WHERE status='new'
      GROUP BY term
      ORDER BY geo_cnt DESC, max_score DESC, last_seen DESC
      LIMIT :limit;
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"limit": limit}).fetchall()

    # rows: (term, geo_cnt, max_score, last_seen)
    return [r[0] for r in rows]


def mark_approved(terms: List[str]) -> int:
    if not terms:
        return 0
    q = text("""
      UPDATE discovered_terms
      SET status='approved'
      WHERE status='new' AND term = ANY(:terms);
    """)
    with engine.begin() as conn:
        res = conn.execute(q, {"terms": terms})
    return res.rowcount or 0


def main():
    parser = argparse.ArgumentParser(
        description="Promote discovered_terms TOP N (status=new) into seeds.yaml without duplicates."
    )
    parser.add_argument("--seeds", default="app/seeds.yaml", help="Path to seeds.yaml")
    parser.add_argument("--limit", type=int, default=20, help="Top N discovered terms to consider")
    parser.add_argument("--group", default="discovered_auto", help="seed_groups key to append into")
    parser.add_argument("--approve", action="store_true", help="Also mark promoted terms as approved in DB")
    args = parser.parse_args()

    init_schema()

    cfg = load_seeds(args.seeds)
    cfg.setdefault("seed_groups", {})
    cfg["seed_groups"].setdefault(args.group, [])

    _, existing_norm = existing_seed_terms(cfg)

    # TOP N 가져오기
    top_terms = fetch_top_new(limit=args.limit)

    # TOP N 내부 중복 제거 + seeds 중복 제거
    promoted: List[str] = []
    seen_norm = set(existing_norm)

    for t in top_terms:
        nt = normalize_term(t)
        if nt in seen_norm:
            continue
        seen_norm.add(nt)
        promoted.append(t)

    if not promoted:
        print("No new unique terms to promote (all overlapped with existing seeds).")
        return

    # seeds.yaml에 추가
    cfg["seed_groups"][args.group].extend(promoted)
    dump_seeds(args.seeds, cfg)

    print(f"Promoted {len(promoted)} terms into seed_groups.{args.group} in {args.seeds}")
    for t in promoted:
        print(f" - {t}")

    # (선택) DB에서도 approved로 승급
    if args.approve:
        n = mark_approved(promoted)
        print(f"DB approved updated rows: {n}")


if __name__ == "__main__":
    main()
