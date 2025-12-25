# app/demote_seeds.py
from __future__ import annotations

import argparse
from typing import Dict, Any, List, Tuple, Set

import yaml
from sqlalchemy import text

from app.db import engine, init_schema


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_yaml(path: str, data: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


def norm(s: str) -> str:
    return " ".join(str(s).strip().lower().split())


# ------------------------------
# DB helpers
# ------------------------------
def trend_features_count(window_days: int) -> int:
    q = text("""
      SELECT COUNT(*)
      FROM trend_features
      WHERE as_of_date >= (CURRENT_DATE - (:days || ' days')::interval)
    """)
    with engine.begin() as conn:
        return int(conn.execute(q, {"days": int(window_days)}).scalar() or 0)


def get_active_terms_from_trend_features(window_days: int) -> Set[str]:
    """
    최근 N일 동안 trend_features에 WATCH+로 기록된 term 목록.
    (main.py가 WATCH 이상만 trend_features에 저장하므로, 등장=WATCH+)
    """
    q = text("""
      SELECT DISTINCT term
      FROM trend_features
      WHERE as_of_date >= (CURRENT_DATE - (:days || ' days')::interval)
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"days": int(window_days)}).fetchall()
    return {norm(r[0]) for r in rows if r and r[0] is not None}


def has_column(table: str, column: str) -> bool:
    q = text("""
      SELECT 1
      FROM information_schema.columns
      WHERE table_name = :t AND column_name = :c
      LIMIT 1;
    """)
    with engine.begin() as conn:
        return conn.execute(q, {"t": table, "c": column}).first() is not None


def get_protected_terms_from_discovered(grace_days: int) -> Set[str]:
    """
    Grace period 보호 term 세트.
    1) (있다면) discovered_terms.approved_at 최근 grace_days 이내
    2) discovered_terms.last_seen 최근 grace_days 이내  (fallback)
    - status는 approved/new 상관없이 보호(방금 promote/approve한 애들 보호 목적)
    """
    q_parts = []

    # approved_at이 있으면 그걸 우선 사용
    if has_column("discovered_terms", "approved_at"):
        q_parts.append("""
          SELECT DISTINCT term
          FROM discovered_terms
          WHERE approved_at IS NOT NULL
            AND approved_at >= (NOW() - (:days || ' days')::interval)
        """)

    # fallback: last_seen
    if has_column("discovered_terms", "last_seen"):
        q_parts.append("""
          SELECT DISTINCT term
          FROM discovered_terms
          WHERE last_seen IS NOT NULL
            AND last_seen >= (NOW() - (:days || ' days')::interval)
        """)

    if not q_parts:
        return set()

    q = text(" UNION ".join(q_parts))
    with engine.begin() as conn:
        rows = conn.execute(q, {"days": int(grace_days)}).fetchall()
    return {norm(r[0]) for r in rows if r and r[0] is not None}


def reject_in_db(terms: List[str]) -> int:
    """
    discovered_terms의 status를 rejected로 바꿈 (term 전체 geo에 적용)
    """
    if not terms:
        return 0
    q = text("""
      UPDATE discovered_terms
      SET status='rejected'
      WHERE term = ANY(:terms);
    """)
    with engine.begin() as conn:
        res = conn.execute(q, {"terms": terms})
    return res.rowcount or 0


# ------------------------------
# Main
# ------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Demote terms from seeds.yaml discovered_auto group (safe mode with grace + data gate)."
    )
    parser.add_argument("--seeds", default="app/seeds.yaml")
    parser.add_argument("--group", default="discovered_auto")

    # 성과 기반 강등
    parser.add_argument("--use-trend-features", action="store_true",
                        help="demote if term did NOT appear in trend_features within window-days")
    parser.add_argument("--window-days", type=int, default=14, help="7/14/30 등 강등 기준 기간")

    # Grace + 데이터 게이트
    parser.add_argument("--grace-days", type=int, default=7,
                        help="최근 grace-days 이내에 승인/발견 흔적이 있으면 강등 제외 (approved_at/last_seen 기반)")
    parser.add_argument("--min-tf-count", type=int, default=200,
                        help="최근 window-days 동안 trend_features 레코드가 이 수보다 적으면 강등을 스킵(초기/데이터부족 보호)")

    # 수동 강등
    parser.add_argument("--terms", default="", help="comma-separated explicit terms to demote (manual list)")

    # 실행 옵션
    parser.add_argument("--apply", action="store_true", help="actually write seeds.yaml (default: dry-run)")
    parser.add_argument("--reject", action="store_true", help="also set discovered_terms.status='rejected' in DB")

    args = parser.parse_args()

    init_schema()

    cfg = load_yaml(args.seeds)
    groups = cfg.get("seed_groups", {}) or {}
    arr = groups.get(args.group, []) or []
    if not arr:
        print(f"No terms in seed_groups.{args.group}")
        return

    arr_norm_set = {norm(x) for x in arr}

    # 수동 강등 목록 파싱
    manual_terms = [x.strip() for x in (args.terms or "").split(",") if x.strip()]
    manual_terms = [t for t in manual_terms if norm(t) in arr_norm_set]

    demote_auto: List[str] = []
    reasons: List[Tuple[str, str]] = []

    if args.use_trend_features:
        # ✅ 데이터 게이트: trend_features가 충분히 쌓이지 않았으면 강등 스킵
        tf_cnt = trend_features_count(args.window_days)
        if tf_cnt < args.min_tf_count:
            print(
                f"[SKIP] trend_features too small for last {args.window_days}d: "
                f"{tf_cnt} rows < min_tf_count({args.min_tf_count}).\n"
                f"→ This usually means monitoring hasn't run enough days yet. "
                f"Increase --window-days, lower --min-tf-count, or run app.main daily."
            )
            # 이 경우 수동 강등만 반영 가능하게 하려면 아래 주석 해제
            # demote_auto = []
        else:
            active = get_active_terms_from_trend_features(args.window_days)
            protected = get_protected_terms_from_discovered(args.grace_days)

            for t in arr:
                nt = norm(t)

                # ✅ grace 보호: 최근 승인/발견 흔적이 있는 애들은 제외
                if args.grace_days > 0 and nt in protected:
                    reasons.append((str(t), f"grace: recent approved/seen < {args.grace_days}d"))
                    continue

                # ✅ 성과 없음: 최근 window-days 동안 WATCH+ 기록이 없으면 강등
                if nt not in active:
                    demote_auto.append(str(t))
                    reasons.append((str(t), f"no WATCH+ in trend_features for {args.window_days}d"))

    else:
        print("[INFO] This script is currently focused on --use-trend-features mode.")
        print("       Run with --use-trend-features to demote based on WATCH+ inactivity.")
        # 그래도 수동 강등은 할 수 있게
        demote_auto = []

    # 최종 강등 리스트 = auto + manual (중복 제거, seeds에 실제 있는 것만)
    final: List[str] = []
    seen: Set[str] = set()

    for t in demote_auto + manual_terms:
        k = norm(t)
        if k in seen:
            continue
        if k not in arr_norm_set:
            continue
        seen.add(k)
        final.append(t)

    print(f"[DEMOTE PREVIEW] group={args.group}  candidates={len(arr)}  demote={len(final)}")
    for t in final[:50]:
        print(f" - {t}")
    if len(final) > 50:
        print(f" ... (+{len(final)-50} more)")

    if reasons:
        print("\n[WHY] sample reasons (first 30):")
        for t, r in reasons[:30]:
            print(f" - {t} ({r})")

    if not args.apply:
        print("\n(dry-run) Not writing seeds.yaml. Use --apply to persist.")
        if args.reject:
            print("(dry-run) Also not updating DB. Add --apply --reject to apply both.")
        return

    if not final:
        print("Nothing to apply.")
        return

    # seeds.yaml에서 제거
    final_norm = {norm(x) for x in final}
    new_arr = [x for x in arr if norm(x) not in final_norm]
    cfg.setdefault("seed_groups", {})
    cfg["seed_groups"][args.group] = new_arr
    save_yaml(args.seeds, cfg)
    print(f"\nApplied: removed {len(final)} terms from seed_groups.{args.group}")

    # DB에서 rejected 처리(선택)
    if args.reject:
        n = reject_in_db(final)
        print(f"DB rejected updated rows: {n}")


if __name__ == "__main__":
    main()
