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


class TrendFeature(models.Model):
    term = models.TextField()
    geo = models.TextField()
    as_of_date = models.DateField()

    wow_change = models.FloatField()
    z_score = models.FloatField()
    slope_7d = models.FloatField()
    latest = models.FloatField()
    computed_at = models.DateTimeField()

    class Meta:
        db_table = "trend_features"
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
