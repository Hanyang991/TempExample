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

]
