from datetime import date, timedelta
from django.db.models import Count, Max
from django.http import HttpResponse, HttpResponseBadRequest,JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.http import require_POST
from django.views.decorators.http import require_GET
from .models import TrendFeature, TrendSeries, DiscoveredTerm

from datetime import date, timedelta
from django.db.models import Count, Max

ALLOWED_SORTS = {
    "date": "as_of_date",
    "z": "z_score",
    "wow": "wow_change",
    "term": "term",
}

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

    if severity == "WATCH":
        base = base.filter(z_score__gte=1.0, z_score__lt=2.0)
    elif severity == "RISING":
        base = base.filter(z_score__gte=2.0, z_score__lt=2.5)
    elif severity == "BREAKOUT":
        base = base.filter(z_score__gte=2.5)

    sort_col = ALLOWED_SORTS.get(sort, "as_of_date")
    prefix = "" if direction == "asc" else "-"
    order = f"{prefix}{sort_col}"

    rows = (
        base.order_by(order)
            .values("as_of_date", "geo", "term", "z_score", "wow_change")[:300]
    )

    def sev(z):
        if z >= 2.5:
            return "BREAKOUT"
        if z >= 2.0:
            return "RISING"
        if z >= 1.0:
            return "WATCH"
        return "NONE"

    enriched = []
    for r in rows:
        r = dict(r)
        r["severity"] = sev(float(r["z_score"]))
        enriched.append(r)

    ctx = {
        "days": days,
        "geo": geo,
        "severity": severity,
        "q": q,
        "sort": sort,
        "dir": direction,
        "rows": enriched,
    }


    # HTMX 요청이면 테이블만 반환
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
    start = date.today() - timedelta(days=90)

    # 1) geo 결정: ?geo=US 우선, 없으면 최근 feature에서 하나 기본값
    geo = (request.GET.get("geo") or "").strip()

    feats_qs = (
        TrendFeature.objects
        .filter(term=term, as_of_date__gte=start)
        .order_by("-as_of_date", "-z_score")
        .values("as_of_date", "geo", "z_score", "wow_change", "latest", "slope_7d")
    )

    geo_list_raw = (
        TrendFeature.objects
        .filter(term=term, as_of_date__gte=start)
        .values_list("geo", flat=True)
        .distinct()
    )

    # ✅ 공백 제거 + 대문자 통일 + 빈값 제거 + 중복 제거
    geo_set = {g.strip().upper() for g in geo_list_raw if g and g.strip()}
    geo_list = sorted(geo_set)


    events = []
    for f in list(feats_qs):
        events.append({
            "date": f["as_of_date"].isoformat(),
            "z": float(f["z_score"]),
            "wow": float(f["wow_change"]),
            "severity": "BREAKOUT" if f["z_score"] >= 2.5 else ("RISING" if f["z_score"] >= 2.0 else ("WATCH" if f["z_score"] >= 1.0 else "NONE")),
        })

    if not geo:
        first = feats_qs.first()
        geo = first["geo"] if first else ""   # feature도 없으면 빈 값(차트는 에러 표시)

    # 2) trend_series: geo까지 필터 (차트가 geo별로 정확히 뜸)
    series_qs = (
        TrendSeries.objects
        .filter(term=term, geo=geo, date__gte=start)
        .order_by("date")
        .values("date", "value")
    )

    labels = [s["date"].isoformat() for s in series_qs]
    values = [float(s["value"]) for s in series_qs]

    # 3) Events: 기본은 해당 geo만 보여주고 싶으면 geo 필터(추천)
    feats = feats_qs.filter(geo=geo)[:200]

    return render(request, "dashboard/term_detail.html", {
        "term": term,
        "geo": geo,
        "geo_list": geo_list, 
        "labels": labels,
        "values": values,
        "events": events,
        "features": list(feats),
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
    geo = request.GET.get("geo", "").strip()
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

    return JsonResponse({
        "term": term,
        "geo": geo,
        "days": days,
        "x": x,
        "y": y,
    })

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

    # geo별로 묶기
    m = {}
    for r in qs:
        g = r["geo"]
        m.setdefault(g, {"x": [], "y": []})
        m[g]["x"].append(r["date"].isoformat())
        m[g]["y"].append(float(r["value"]))

    traces = []
    for g, v in m.items():
        traces.append({"name": g, "x": v["x"], "y": v["y"]})

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
