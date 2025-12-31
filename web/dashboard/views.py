import json
from datetime import date, timedelta
from django.db.models import Count, Max
from django.http import HttpResponse, HttpResponseBadRequest,JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.http import require_POST
from django.views.decorators.http import require_GET
from .models import TrendFeature, TrendSeries, DiscoveredTerm, Alert
from django.views.decorators.csrf import csrf_exempt

from datetime import date, timedelta
from django.db.models import Count, Max
ALERT_WINDOW_DAYS = 7
ALLOWED_SORTS = {
    "date": "as_of_date",
    "z": "z_score",
    "wow": "wow_change",
    "term": "term",
}

def severity_from_feature(*, z: float, has_alert: bool) -> str:
    if has_alert:
        return "EMERGING"
    if z >= 2.5:
        return "BREAKOUT"
    if z >= 2.0:
        return "RISING"
    if z >= 1.0:
        return "WATCH"
    return "NONE"


def trends(request):
    days = int(request.GET.get("days", "14"))
    start = date.today() - timedelta(days=days)

    geo = request.GET.get("geo", "")
    severity = request.GET.get("severity", "")
    q = request.GET.get("q", "").strip()

    sort = request.GET.get("sort", "date")
    direction = request.GET.get("dir", "desc")

    base = TrendFeature.objects.filter(as_of_date__gte=start)

    if geo:
        base = base.filter(geo=geo)
    if q:
        base = base.filter(term__icontains=q)

    # ✅ EMERGING: alerts 기반 필터
    alert_since = timezone.now() - timedelta(days=ALERT_WINDOW_DAYS)

    if severity == "EMERGING":
        # 최근 alerts에 찍힌 term+geo만 남김
        alert_pairs_qs = (
            Alert.objects
            .filter(severity="EMERGING", fired_at__gte=alert_since)
            .values("term", "geo")
            .distinct()
        )
        alert_pairs = {(a["term"], a["geo"]) for a in alert_pairs_qs}

        # TrendFeature에서 term/geo로 필터링 (ORM에서 (term,geo) IN 튜플이 어려워서 python filter)
        # → rows 뽑을 때만 거르는 방식으로 처리 (아래에서 반영)
    else:
        if severity == "WATCH":
            base = base.filter(z_score__gte=1.0, z_score__lt=2.0)
        elif severity == "RISING":
            base = base.filter(z_score__gte=2.0, z_score__lt=2.5)
        elif severity == "BREAKOUT":
            base = base.filter(z_score__gte=2.5)

    sort_col = ALLOWED_SORTS.get(sort, "as_of_date")
    prefix = "" if direction == "asc" else "-"
    order = f"{prefix}{sort_col}"

    # 먼저 넉넉히 가져온 뒤 (EMERGING 필터가 python filter라)
    rows = list(
        base.order_by(order)
            .values("as_of_date", "geo", "term", "z_score", "wow_change")[:800]
    )

    # ✅ term+geo 별 EMERGING 여부(최근 alerts) 맵 구성
    alert_rows = (
        Alert.objects
        .filter(fired_at__gte=alert_since)
        .values("term", "geo", "severity")
    )

    alert_set = {
        (a["term"], (a["geo"] or "").strip().upper())
        for a in alert_rows
        if a["severity"] == "EMERGING"
    }

    enriched = []
    for r in rows:
        r = dict(r)
        geo_r = (r["geo"] or "").strip().upper()
        term_r = r["term"]

        has_alert = (term_r, geo_r) in alert_set
        r["severity"] = severity_from_feature(
            z=float(r["z_score"]),
            has_alert=has_alert,
        )

        if severity in ("WATCH", "RISING", "BREAKOUT") and has_alert:
            continue

        # ✅ EMERGING 필터일 때만 여기서 걸러냄
        if severity == "EMERGING" and not has_alert:
            continue

        enriched.append(r)

        if len(enriched) >= 300:
            break

    ctx = {
        "days": days,
        "geo": geo,
        "severity": severity,
        "q": q,
        "sort": sort,
        "dir": direction,
        "rows": enriched,
    }

    if request.htmx:
        return render(request, "dashboard/_trends_table.html", ctx)
    return render(request, "dashboard/trends.html", ctx)



def dashboard(request):
    days = int(request.GET.get("days", "14"))
    start = date.today() - timedelta(days=days)

    qs = TrendFeature.objects.filter(as_of_date__gte=start)

    # trend_features에는 severity 컬럼이 없으므로 z_score 구간으로 계산
    counts = {
        "WATCH": qs.filter(z_score__gte=1.0, z_score__lt=2.0).count(),
        "RISING": qs.filter(z_score__gte=2.0, z_score__lt=2.5).count(),
        "BREAKOUT": qs.filter(z_score__gte=2.5).count(),
    }

    top_terms = (
        qs.filter(z_score__gte=1.0)
        .values("term")
        .annotate(cnt=Count("term"), last=Max("as_of_date"), max_z=Max("z_score"))
        .order_by("-max_z", "-cnt", "-last")[:10]
    )

    return render(request, "dashboard/dashboard.html", {
        "days": days,
        "counts": counts,
        "top_terms": top_terms,
    })

def term_detail(request, term: str):
    # 기본 90일
    start = date.today() - timedelta(days=90)

    # term에 실제 존재하는 geo 목록
    series_geos = (
        TrendSeries.objects
        .filter(term=term, date__gte=start)
        .values_list("geo", flat=True)
        .distinct()
    )
    geo_list = sorted({g.strip().upper() for g in series_geos if g and g.strip()})

    # ✅ 기본 geo는 ALL
    geo = (request.GET.get("geo") or "ALL").strip().upper()
    if geo != "ALL" and geo_list and geo not in geo_list:
        # 잘못된 geo가 넘어오면 ALL로
        geo = "ALL"

    # 단일 geo일 때만 labels/values가 의미 있음 (ALL이면 차트는 API로 그리니까 빈 리스트 OK)
    labels = []
    values = []

    if geo != "ALL":
        series = (
            TrendSeries.objects
            .filter(term=term, geo=geo, date__gte=start)
            .order_by("date")
            .values("date", "value")
        )
        labels = [s["date"].isoformat() for s in series]
        values = [float(s["value"]) for s in series]

    ALERT_WINDOW_DAYS = 7
    alert_since = timezone.now() - timedelta(days=ALERT_WINDOW_DAYS)

    alerts_qs = (
        Alert.objects
        .filter(
            term=term,
            fired_at__gte=alert_since,
        )
        .values("geo")
    )

    alert_geos = {a["geo"].strip().upper() for a in alerts_qs}

    events = []
    # ✅ Events 점용 feature rows (이게 빠져 있었음!)
    feats_events = list(
        TrendFeature.objects
        .filter(term=term, as_of_date__gte=start)
        .order_by("-as_of_date", "-z_score")
        .values("as_of_date", "geo", "z_score", "wow_change")[:500]
    )

    for f in feats_events:
        geo_f = (f["geo"] or "").strip().upper()
        has_alert = geo_f in alert_geos

        z = float(f["z_score"])
        sev = severity_from_feature(z=z, has_alert=has_alert)

        events.append({
            "date": f["as_of_date"].isoformat(),
            "geo": geo_f,
            "z": z,
            "wow": float(f["wow_change"]),
            "severity": sev,
        })

    return render(request, "dashboard/term_detail.html", {
        "term": term,
        "geo": geo,
        "geo_list": geo_list,
        "labels": labels,
        "values": values,
        "events": events,  # ✅ 차트 점용
        # Events 테이블은 htmx partial로 따로 로딩됨
    })

def discovery_inbox(request):
    # new 후보를 term 단위로 묶어서 보여주기
    rows = (
        DiscoveredTerm.objects.filter(status="new")
        .values("term")
        .annotate(
            geo_cnt=Count("geo", distinct=True),
            max_score=Max("score"),
            last_seen=Max("last_seen"),
        )
        .order_by("-geo_cnt", "-max_score", "-last_seen")[:200]
    )

    return render(request, "dashboard/discovery.html", {"rows": rows})

@require_GET
def api_term_series(request):
    term = request.GET.get("term", "").strip()
    geo = (request.GET.get("geo", "") or "").strip().upper()
    days = int(request.GET.get("days", "90"))

    if not term or not geo:
        return JsonResponse({"error": "term and geo are required"}, status=400)

    start = date.today() - timedelta(days=days)

    qs = (
        TrendSeries.objects
        .filter(term=term, geo=geo, date__gte=start)
        .order_by("date")
        .values("date", "value")
    )

    x = [r["date"].isoformat() for r in qs]
    y = [float(r["value"]) for r in qs]

    return JsonResponse({"term": term, "geo": geo, "days": days, "x": x, "y": y})

@require_GET
def api_term_series_all_geo(request):
    term = request.GET.get("term", "").strip()
    days = int(request.GET.get("days", "90"))

    if not term:
        return JsonResponse({"error": "term is required"}, status=400)

    start = date.today() - timedelta(days=days)

    qs = (
        TrendSeries.objects
        .filter(term=term, date__gte=start)
        .order_by("geo", "date")
        .values("geo", "date", "value")
    )

    m = {}
    for r in qs:
        g = (r["geo"] or "").strip().upper()
        if not g:
            continue
        m.setdefault(g, {"x": [], "y": []})
        m[g]["x"].append(r["date"].isoformat())
        m[g]["y"].append(float(r["value"]))

    traces = [{"geo": g, "x": v["x"], "y": v["y"]} for g, v in m.items()]
    traces.sort(key=lambda t: t["geo"])

    return JsonResponse({"term": term, "days": days, "traces": traces})


@require_POST
def discovery_approve(request):
    term = request.POST.get("term", "").strip()
    if not term:
        return HttpResponseBadRequest("missing term")

    now = timezone.now()
    # term 전체 geo row 승인 + approved_at 찍기
    DiscoveredTerm.objects.filter(term=term).update(
        status="approved",
        approved_at=now,
    )

    # HTMX면 해당 row만 새로 렌더링해서 교체
    if request.htmx:
        return HttpResponse("")  # 리스트에서 제거되게 클라이언트에서 처리
    return HttpResponse("ok")


@require_POST
def discovery_reject(request):
    term = request.POST.get("term", "").strip()
    if not term:
        return HttpResponseBadRequest("missing term")

    DiscoveredTerm.objects.filter(term=term).update(status="rejected")

    if request.htmx:
        return HttpResponse("")
    return HttpResponse("ok")

@require_GET
def events_table(request):
    term = request.GET.get("term", "").strip()
    geo = (request.GET.get("geo") or "ALL").strip().upper()
    days = int(request.GET.get("days", "90"))

    start = date.today() - timedelta(days=days)

    qs = TrendFeature.objects.filter(term=term, as_of_date__gte=start)
    if geo != "ALL":
        qs = qs.filter(geo=geo)

    feats = list(
        qs.order_by("-as_of_date", "-z_score")
          .values("as_of_date", "geo", "z_score", "wow_change")[:200]
    )

    alert_since = timezone.now() - timedelta(days=ALERT_WINDOW_DAYS)

    alert_rows = (
        Alert.objects
        .filter(term=term, fired_at__gte=alert_since)
        .values("geo")
    )

    alert_geos = {
        a["geo"].strip().upper()
        for a in alert_rows
        if a["geo"]
    }

    for f in feats:
        geo_f = (f["geo"] or "").strip().upper()
        has_alert = geo_f in alert_geos
        f["severity"] = severity_from_feature(
            z=float(f["z_score"]),
            has_alert=has_alert,
        )


    return render(request, "dashboard/_events_table.html", {
        "features": feats
    })



# 잠깐 쓸거
def _ai_analyze_term(term: str, geo: str) -> dict:
    t = term.lower()

    intent = []
    if "routine" in t:
        intent.append("routine")
    if any(k in t for k in ["serum", "toner", "cleanser", "mask", "cream", "sunscreen"]):
        intent.append("product_category")
    if any(k in t for k in ["acne", "barrier", "dark spots", "hyperpigmentation", "redness", "rosacea"]):
        intent.append("skin_concern")
    if any(k in t for k in ["korean", "k beauty", "k-beauty"]):
        intent.append("kbeauty_intent")

    # 간단 인사이트 템플릿
    angles = []
    if "night" in t and "routine" in t:
        angles = [
            "밤 루틴 = 회복/장벽/액티브 사용 니즈가 강함",
            "Step-by-step(3-step/5-step) 가이드를 상세페이지 상단에 배치",
            "성분 충돌(레티놀/산/비타민C) 안전 조합표 제공하면 전환에 도움",
        ]
    elif "before after" in t:
        angles = [
            "전/후 콘텐츠는 증거(리뷰/임상/사용기간/조명/각도) 신뢰가 핵심",
            "UGC(사용자 후기) 큐레이션 + 주의사항(개인차) 표기 권장",
        ]
    else:
        angles = [
            "검색어는 ‘방법/추천/후기’ 형태의 정보 탐색 의도가 강할 가능성",
            "루틴 카드/비교표/FAQ 형태가 성과 좋음",
        ]

    actions = {
        "content": [
            "TikTok/Shorts용 15초 ‘3-step 루틴’ 스크립트",
            "피부타입별(지성/건성/민감) 분기 카드",
        ],
        "commerce": [
            "상세페이지에 '사용 순서' 섹션 추가",
            "같이 쓰면 좋은 제품(번들) 제안",
        ],
        "risk_notes": [
            "자극 성분 포함 시 주의 문구/패치 테스트 안내",
        ]
    }

    return {
        "term": term,
        "geo": geo or "ALL",
        "intents": intent or ["general_info"],
        "summary": f"'{term}'는 {', '.join(intent) if intent else '일반 정보 탐색'} 의도가 강한 검색어로 보입니다.",
        "angles": angles,
        "actions": actions,
    }


@require_GET
def api_term_ai(request):
    term = (request.GET.get("term") or "").strip()
    geo = (request.GET.get("geo") or "ALL").strip().upper()
    if not term:
        return JsonResponse({"error": "term is required"}, status=400)

    data = _ai_analyze_term(term, geo)
    return JsonResponse(data)


@require_POST
def api_term_ai_slack(request):
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return HttpResponseBadRequest("invalid json")

    term = (payload.get("term") or "").strip()
    geo = (payload.get("geo") or "ALL").strip().upper()
    analysis = payload.get("analysis")

    if not term or not analysis:
        return HttpResponseBadRequest("term and analysis are required")

    # ✅ 메시지 구성
    text = (
        f"*AI Analysis*\n"
        f"- term: `{term}`\n"
        f"- geo: `{geo}`\n"
        f"- at: {timezone.now().isoformat()}\n\n"
        f"*Summary*\n{analysis.get('summary','')}\n\n"
        f"*Angles*\n" + "\n".join([f"- {a}" for a in analysis.get("angles", [])]) + "\n\n"
        f"*Actions*\n"
        f"- Content: " + ", ".join(analysis.get("actions", {}).get("content", [])) + "\n"
        f"- Commerce: " + ", ".join(analysis.get("actions", {}).get("commerce", [])) + "\n"
    )

    # ✅ 여기서 슬랙 전송: 너 app/slack_notifier에 맞춰 연결
    # 예시 1) webhook 보내는 함수가 있을 때
    from app.slack_notifier import send_daily_summary  # ❗ 실제 함수명에 맞게 교체

    # send_daily_summary(text) 같은 시그니처가 아니면,
    # slack_notifier.py의 실제 전송 함수로 바꿔줘.
    try:
        send_daily_summary(text)
    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)

    return JsonResponse({"ok": True})