from django.urls import path
from . import views
urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("trends/", views.trends, name="trends"),
    path("term/<path:term>/", views.term_detail, name="term_detail"),
    path("discovery/", views.discovery_inbox, name="discovery_inbox"),
    path("discovery/approve/", views.discovery_approve, name="discovery_approve"),
    path("discovery/reject/", views.discovery_reject, name="discovery_reject"),

    path("api/term-series/", views.api_term_series, name="api_term_series"),
    path("api/term-series-all-geo/", views.api_term_series_all_geo, name="api_term_series_all_geo"),
    path("api/term-ai/", views.api_term_ai, name="api_term_ai"),
    path("api/term-ai-slack/", views.api_term_ai_slack, name="api_term_ai_slack"),
    path("api/term-has-today-event/", views.api_term_has_today_event, name="api_term_has_today_event"),

    # partials (htmx)
    path("events-table/", views.events_table, name="events_table"),

]
