from __future__ import annotations
from sqlalchemy import create_engine, text
from app.config import settings
from sqlalchemy import text

engine = create_engine(settings.postgres_dsn, pool_pre_ping=True)

def init_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS trend_series (
      term TEXT NOT NULL,
      geo  TEXT NOT NULL,
      date DATE NOT NULL,
      value DOUBLE PRECISION NOT NULL,
      source TEXT NOT NULL DEFAULT 'google_trends',
      collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (term, geo, date)
    );

    CREATE TABLE IF NOT EXISTS trend_features (
      term TEXT NOT NULL,
      geo  TEXT NOT NULL,
      as_of_date DATE NOT NULL,
      wow_change DOUBLE PRECISION NOT NULL,
      z_score DOUBLE PRECISION NOT NULL,
      slope_7d DOUBLE PRECISION NOT NULL,
      latest DOUBLE PRECISION NOT NULL,
      computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (term, geo, as_of_date)
    );

    CREATE TABLE IF NOT EXISTS alerts (
      id BIGSERIAL PRIMARY KEY,
      term TEXT NOT NULL,
      geo  TEXT NOT NULL,
      severity TEXT NOT NULL,
      fired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      slack_channel TEXT,
      slack_ts TEXT,
      status TEXT NOT NULL DEFAULT 'new',
      cooldown_until TIMESTAMPTZ
    );

    CREATE INDEX IF NOT EXISTS idx_alerts_lookup
      ON alerts(term, geo, severity, fired_at DESC);

    CREATE INDEX IF NOT EXISTS idx_series_term_geo_date
      ON trend_series(term, geo, date DESC);

    CREATE INDEX IF NOT EXISTS idx_features_term_geo_date
      ON trend_features(term, geo, as_of_date DESC);

    CREATE TABLE IF NOT EXISTS discovered_terms (
      term TEXT NOT NULL,
      geo  TEXT NOT NULL,
      source_term TEXT,
      kind TEXT NOT NULL DEFAULT 'related_queries',  -- related_queries / related_topics
      rank INT,
      score DOUBLE PRECISION,
      first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_seen  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      status TEXT NOT NULL DEFAULT 'new',            -- new / approved / rejected
      PRIMARY KEY (term, geo)
    );

    CREATE INDEX IF NOT EXISTS idx_discovered_status
      ON discovered_terms(status);

    CREATE INDEX IF NOT EXISTS idx_discovered_geo
      ON discovered_terms(geo);

    CREATE INDEX IF NOT EXISTS idx_discovered_last_seen
      ON discovered_terms(last_seen DESC);
    
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS hourly_snapshots (
          id BIGSERIAL PRIMARY KEY,
          snapshot_at TIMESTAMPTZ NOT NULL,
          geo_count INT NOT NULL,
          term_count INT NOT NULL,
          timeframe TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE (snapshot_at)
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS hourly_snapshot_features (
          snapshot_id BIGINT NOT NULL REFERENCES hourly_snapshots(id) ON DELETE CASCADE,
          term TEXT NOT NULL,
          geo TEXT NOT NULL,
          wow_change DOUBLE PRECISION NOT NULL,
          z_score DOUBLE PRECISION NOT NULL,
          slope_7d DOUBLE PRECISION NOT NULL,
          latest DOUBLE PRECISION NOT NULL,
          severity TEXT NOT NULL,
          PRIMARY KEY (snapshot_id, term, geo)
        );
        """))

        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_hsf_term_geo
          ON hourly_snapshot_features(term, geo);
        """))
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_hsf_severity
          ON hourly_snapshot_features(severity);
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS daily_rollups (
          report_date DATE PRIMARY KEY,
          payload_json JSONB NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))
