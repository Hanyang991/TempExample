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
from .gemini_client import analyze_term
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
    if z >= 2.5:
        return "BREAKOUT"
    if z >= 2.0:
        return "RISING"
    if z >= 1.0:
        return "WATCH"
    if has_alert:
        return "EMERGING"
    return "NONE"


def trends(request):
    days = int(request.GET.get("days", "14"))
    start = date.today() - timedelta(days=days)

    geo = (request.GET.get("geo") or "").strip().upper()
    severity = (request.GET.get("severity") or "").strip().upper()
    q = (request.GET.get("q") or "").strip()

    sort = request.GET.get("sort", "date")
    direction = request.GET.get("dir", "desc")

    base = TrendFeature.objects.filter(as_of_date__gte=start)

    if geo:
        base = base.filter(geo=geo)

    if q:
        base = base.filter(term__icontains=q)

    #   이제 severity는 DB 컬럼 그대로 필터
    # severity가 비어있으면 전체
    if severity:
        base = base.filter(severity=severity)

    sort_col = ALLOWED_SORTS.get(sort, "as_of_date")
    prefix = "" if direction == "asc" else "-"
    order = f"{prefix}{sort_col}"

    rows = list(
        base.order_by(order)
            .values("as_of_date", "geo", "term", "z_score", "wow_change", "severity")[:300]
    )

    ctx = {
        "days": days,
        "geo": geo,
        "severity": severity,
        "q": q,
        "sort": sort,
        "dir": direction,
        "rows": rows,  #   이미 severity 포함
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

    #   기본 geo는 ALL
    geo = (request.GET.get("geo") or "ALL").strip().upper()
    if geo != "ALL" and geo_list and geo not in geo_list:
        geo = "ALL"

    # 단일 geo일 때만 labels/values가 의미 있음 (ALL이면 차트는 API로)
    labels: list[str] = []
    values: list[float] = []

    if geo != "ALL":
        series = (
            TrendSeries.objects
            .filter(term=term, geo=geo, date__gte=start)
            .order_by("date")
            .values("date", "value")
        )
        labels = [s["date"].isoformat() for s in series]
        values = [float(s["value"]) for s in series]

    #   Events 점용 feature rows: DB severity를 그대로 포함해서 가져오기
    feats_events = list(
        TrendFeature.objects
        .filter(term=term, as_of_date__gte=start)
        .order_by("-as_of_date", "-z_score")
        .values("as_of_date", "geo", "z_score", "wow_change", "severity")[:500]
    )

    #   events payload 구성 (severity 재계산 X)
    events = []
    for f in feats_events:
        events.append({
            "date": f["as_of_date"].isoformat(),
            "geo": (f["geo"] or "").strip().upper(),
            "z": float(f["z_score"]),
            "wow": float(f["wow_change"]),
            "severity": (f.get("severity") or "NONE").strip().upper(),
        })

    return render(request, "dashboard/term_detail.html", {
        "term": term,
        "geo": geo,
        "geo_list": geo_list,
        "labels": labels,
        "values": values,
        "events": events,  #   차트 점용 (DB severity)
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

def _get_latest_metrics(term: str, geo: str) -> dict:
    qs = TrendFeature.objects.filter(term=term)

    if geo != "ALL":
        qs = qs.filter(geo=geo)

    row = (
        qs.order_by("-as_of_date")
          .values("wow_change", "z_score", "slope_7d")
          .first()
    )

    if not row:
        return {}

    return {
        "wow": float(row["wow_change"]),
        "z": float(row["z_score"]),
        "slope": float(row["slope_7d"]),
    }


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

    #   DB severity 포함해서 그대로 가져오기
    feats = list(
        qs.order_by("-as_of_date", "-z_score")
          .values("as_of_date", "geo", "z_score", "wow_change", "severity")[:200]
    )

    # 템플릿 편의를 위해 정규화만 해줌 (재계산 X)
    for f in feats:
        f["geo"] = (f["geo"] or "").strip().upper()
        f["severity"] = (f.get("severity") or "NONE").strip().upper()

    return render(request, "dashboard/_events_table.html", {
        "features": feats
    })



@require_GET
def api_term_ai(request):
    term = (request.GET.get("term") or "").strip()
    geo = (request.GET.get("geo") or "ALL").strip().upper()
    print(term, geo)
    metrics = _get_latest_metrics(term=term, geo=geo)
    if not term:
        return JsonResponse({"error": "term is required"}, status=400)

    try:
        raw = analyze_term(term, geo, metrics)
        data = json.loads(raw)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

    return JsonResponse({
        "term": term,
        "geo": geo,
        "analysis": data,
    })


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

    # 메시지 구성
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

    #   여기서 슬랙 전송: 너 app/slack_notifier에 맞춰 연결
    # 예시 1) webhook 보내는 함수가 있을 때
    from app.slack_notifier import send_daily_summary  # ❗ 실제 함수명에 맞게 교체

    # send_daily_summary(text) 같은 시그니처가 아니면,
    # slack_notifier.py의 실제 전송 함수로 바꿔줘.
    try:
        send_daily_summary(text)
    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)

    return JsonResponse({"ok": True})