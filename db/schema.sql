-- Canonical schema for both local SQLite and Cloudflare D1.
-- Kept in sync with db/migrations/0001_init.sql.

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS crawl_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_tag TEXT NOT NULL UNIQUE,
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL,
  area_city TEXT NOT NULL,
  area_district TEXT NOT NULL,
  raw_count INTEGER NOT NULL,
  unique_count INTEGER NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS source_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  source TEXT NOT NULL,
  status TEXT NOT NULL,
  blocked INTEGER NOT NULL DEFAULT 0,
  observed_total INTEGER,
  crawled_listings INTEGER NOT NULL DEFAULT 0,
  url TEXT NOT NULL,
  notes TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (run_id) REFERENCES crawl_runs(id) ON DELETE CASCADE,
  UNIQUE(run_id, source)
);

CREATE TABLE IF NOT EXISTS listings_current (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  listing_key TEXT NOT NULL,
  listing_id TEXT,
  url TEXT NOT NULL,
  title TEXT,
  address TEXT,
  neighborhood TEXT,
  room_count TEXT,
  building_age TEXT,
  floor_info TEXT,
  price_tl INTEGER,
  gross_sqm REAL,
  net_sqm REAL,
  avg_price_for_sale INTEGER,
  endeksa_min_price INTEGER,
  endeksa_max_price INTEGER,
  area_city TEXT NOT NULL,
  area_district TEXT NOT NULL,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  last_seen_run_id INTEGER,
  last_crawled_at TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  FOREIGN KEY (last_seen_run_id) REFERENCES crawl_runs(id) ON DELETE SET NULL,
  UNIQUE(source, listing_key)
);

CREATE INDEX IF NOT EXISTS idx_listings_current_source ON listings_current(source);
CREATE INDEX IF NOT EXISTS idx_listings_current_area_active ON listings_current(area_city, area_district, is_active);
CREATE INDEX IF NOT EXISTS idx_listings_current_last_seen ON listings_current(last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_listings_current_price_tl ON listings_current(price_tl);
CREATE INDEX IF NOT EXISTS idx_listings_current_room_neighborhood ON listings_current(room_count, neighborhood);

CREATE TABLE IF NOT EXISTS listing_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  source TEXT NOT NULL,
  listing_key TEXT NOT NULL,
  listing_id TEXT,
  url TEXT NOT NULL,
  title TEXT,
  address TEXT,
  neighborhood TEXT,
  room_count TEXT,
  building_age TEXT,
  floor_info TEXT,
  price_tl INTEGER,
  gross_sqm REAL,
  net_sqm REAL,
  avg_price_for_sale INTEGER,
  endeksa_min_price INTEGER,
  endeksa_max_price INTEGER,
  crawled_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES crawl_runs(id) ON DELETE CASCADE,
  UNIQUE(run_id, source, listing_key)
);

CREATE INDEX IF NOT EXISTS idx_listing_snapshots_run ON listing_snapshots(run_id);
CREATE INDEX IF NOT EXISTS idx_listing_snapshots_source ON listing_snapshots(source);

CREATE TABLE IF NOT EXISTS system_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  level TEXT NOT NULL,
  event_type TEXT NOT NULL,
  run_tag TEXT,
  run_id INTEGER,
  area_city TEXT,
  area_district TEXT,
  source TEXT,
  message TEXT NOT NULL,
  metadata_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_system_logs_created_at ON system_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_system_logs_event_type ON system_logs(event_type);
CREATE INDEX IF NOT EXISTS idx_system_logs_area ON system_logs(area_city, area_district, created_at DESC);
