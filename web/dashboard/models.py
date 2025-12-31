# web/dashboard/models.py
from django.db import models


class TrendSeries(models.Model):
    term = models.TextField()
    geo = models.TextField()
    date = models.DateField()
    value = models.FloatField()

    class Meta:
        db_table = "trend_series"
        managed = False


class DiscoveredTerm(models.Model):
    term = models.TextField(primary_key=False)
    geo = models.TextField(primary_key=False)

    source_term = models.TextField(null=True)
    kind = models.TextField()
    rank = models.IntegerField(null=True)
    score = models.FloatField(null=True)

    first_seen = models.DateTimeField()
    last_seen = models.DateTimeField()
    status = models.TextField()
    approved_at = models.DateTimeField(null=True)

    class Meta:
        db_table = "discovered_terms"
        managed = False
        unique_together = (("term", "geo"),)

class Alert(models.Model):
    term = models.TextField()
    geo = models.TextField()
    severity = models.TextField()
    fired_at = models.DateTimeField()
    slack_channel = models.TextField(null=True, blank=True)
    slack_ts = models.TextField(null=True, blank=True)
    status = models.TextField()
    cooldown_until = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "alerts"
        managed = False  # ✅ 마이그레이션 안 함


class TrendFeature(models.Model):
    term = models.TextField()
    geo = models.TextField()
    as_of_date = models.DateField()
    wow_change = models.FloatField()
    z_score = models.FloatField()
    slope_7d = models.FloatField()
    latest = models.FloatField()
    computed_at = models.DateTimeField(auto_now=True)

    # ✅ 추가
    severity = models.TextField(null=True, blank=True)

    class Meta:
        db_table = "trend_features"
        unique_together = ("term", "geo", "as_of_date")

