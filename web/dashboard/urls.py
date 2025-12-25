from django.urls import path
from . import views

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("trends/", views.trends, name="trends"),
    path("term/<path:term>/", views.term_detail, name="term_detail"),
    path("discovery/", views.discovery_inbox, name="discovery_inbox"),
    path("discovery/approve/", views.discovery_approve, name="discovery_approve"),
    path("discovery/reject/", views.discovery_reject, name="discovery_reject"),
]
