# app/demote_seeds.py
from __future__ import annotations

import argparse
from typing import Dict, Any, List, Tuple, Set

import yaml
from sqlalchemy import text

from app.db import engine, init_schema


# --- 기본(문자열) 필터: 필요하면 유지/수정해서 사용 ---
DEFAULT_EXCLUDE = [
    # 로그에서 실제로 섞였던 오염/잡음 계열
    "studios", "hoodie", "scarf",
    "valve",
    "dog", "cat", "purina",
    "series", "order", "book", "novel", "throne of glass",
    "magnifying glass", "used for",
]

DEFAULT_INCLUDE = [
    # “뷰티 문맥” 최소 토큰 (없으면 강등 후보로 보기)
    "skin", "skincare", "serum", "toner", "mist", "cream", "moisturizer", "lotion",
    "cleanser", "face wash", "sunscreen", "spf", "mask", "foundation", "tint", "cushion",
    "acne", "hyperpigmentation", "melasma", "rosacea", "redness", "barrier",
    "ceramide", "panthenol", "azelaic", "tranexamic", "niacinamide", "vitamin c",
    "retinol", "peptide", "cica", "centella", "heartleaf", "pdrn", "pore",
]


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_yaml(path: str, data: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


def norm(s: str) -> str:
    return " ".join(str(s).strip().lower().split())


def parse_csv_arg(v: str, default: List[str]) -> List[str]:
    v = (v or "").strip()
    if not v:
        return default
    return [x.strip() for x in v.split(",") if x.strip()]


def pick_demote_terms_by_text_filters(
    terms: List[str],
    exclude: List[str],
    include: List[str],
) -> Tuple[List[str], List[Tuple[str, str]]]:
    """
    discovered_auto 목록에서 강등할 term 후보를 뽑는다.
    규칙:
      - exclude 토큰이 포함되면 강등
      - include 토큰이 하나도 없으면 강등(옵션으로 끌 수 있음)
    """
    demote: List[str] = []
    reasons: List[Tuple[str, str]] = []

    ex = [norm(x) for x in exclude]
    inc = [norm(x) for x in include]

    for t in terms:
        nt = norm(t)

        bad = next((x for x in ex if x in nt), None)
        if bad:
            demote.append(str(t))
            reasons.append((str(t), f"exclude match: {bad}"))
            continue

        if inc:
            ok = any(x in nt for x in inc)
            if not ok:
                demote.append(str(t))
                reasons.append((str(t), "no include token"))
                continue

    # 중복 제거(원본 순서 유지)
    seen: Set[str] = set()
    uniq: List[str] = []
    for t in demote:
        k = norm(t)
        if k in seen:
            continue
        seen.add(k)
        uniq.append(t)

    return uniq, reasons


def get_active_terms_from_trend_features(window_days: int) -> Set[str]:
    """
    최근 N일 동안 trend_features에 WATCH+로 기록된 term 목록.
    main.py가 compute_signal()이 None이 아닐 때만 trend_features에 저장하므로,
    trend_features에 한 번도 안 나오면 "WATCH 이상 못 찍음"으로 간주 가능.
    """
    q = text("""
      SELECT DISTINCT term
      FROM trend_features
      WHERE as_of_date >= (CURRENT_DATE - (:days || ' days')::interval)
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"days": int(window_days)}).fetchall()
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

def get_recently_approved_terms(grace_days: int) -> Set[str]:
    q = text("""
      SELECT DISTINCT term
      FROM discovered_terms
      WHERE status='approved'
        AND approved_at IS NOT NULL
        AND approved_at >= (NOW() - (:days || ' days')::interval);
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"days": int(grace_days)}).fetchall()
    return {norm(r[0]) for r in rows if r and r[0] is not None}



def main():
    parser = argparse.ArgumentParser(description="Demote terms from seeds.yaml discovered_auto group.")
    parser.add_argument("--seeds", default="app/seeds.yaml")
    parser.add_argument("--group", default="discovered_auto")
    parser.add_argument("--grace-days", type=int, default=7,
                    help="approved_at 기준으로 grace 기간(일). 이 기간 내 term은 강등 제외")
    parser.add_argument("--grace-only-approved", action="store_true",
                        help="approved_at이 없는 term은 grace 적용 없이 강등 대상으로 본다")


    # (모드 1) 문자열 필터 기반 강등 옵션
    parser.add_argument("--exclude", default="", help="comma-separated exclude tokens (override default)")
    parser.add_argument("--include", default="", help="comma-separated include tokens (override default)")
    parser.add_argument("--no-include-check", action="store_true", help="do not demote based on missing include tokens")

    # (모드 2) 성과 기반 강등 옵션: trend_features에 최근 N일 WATCH+ 기록이 없으면 강등
    parser.add_argument("--use-trend-features", action="store_true",
                        help="demote if term did NOT appear in trend_features within window-days")
    parser.add_argument("--window-days", type=int, default=14, help="7/14/30 등 강등 기준 기간")

    # 수동 강등 목록
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
        active = get_active_terms_from_trend_features(args.window_days)
        protected = get_recently_approved_terms(args.grace_days)

        for t in arr:
            nt = norm(t)

            # ✅ grace 기간 보호
            if nt in protected:
                reasons.append((str(t), f"grace: approved < {args.grace_days}d"))
                continue

            # (선택) approved_at이 없는 term은 보호 안 함(기본)
            # args.grace_only_approved가 True면 여기서 별도로 처리할 수도 있음

            if nt not in active:
                demote_auto.append(str(t))
                reasons.append((str(t), f"no WATCH+ in trend_features for {args.window_days}d"))


    else:
        exclude = parse_csv_arg(args.exclude, DEFAULT_EXCLUDE)
        include = [] if args.no_include_check else parse_csv_arg(args.include, DEFAULT_INCLUDE)
        demote_auto, reasons = pick_demote_terms_by_text_filters(arr, exclude=exclude, include=include)

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
        # 원본 arr에 있는 표기를 최대한 유지하고 싶으면 arr에서 찾아 가져오기
        # 여기서는 입력값 t 그대로 사용
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
