ALTER TABLE crawl_runs ADD COLUMN area_country TEXT NOT NULL DEFAULT 'TR';
ALTER TABLE listings_current ADD COLUMN area_country TEXT NOT NULL DEFAULT 'TR';
ALTER TABLE system_logs ADD COLUMN area_country TEXT;
ALTER TABLE deal_feedback ADD COLUMN area_country TEXT NOT NULL DEFAULT 'TR';

CREATE INDEX IF NOT EXISTS idx_crawl_runs_country_area_started
  ON crawl_runs(area_country, area_city, area_district, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_listings_current_country_area_active
  ON listings_current(area_country, area_city, area_district, is_active);

CREATE INDEX IF NOT EXISTS idx_system_logs_country_area
  ON system_logs(area_country, area_city, area_district, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_deal_feedback_country_area_created
  ON deal_feedback(area_country, area_city, area_district, created_at DESC);
