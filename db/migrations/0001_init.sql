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

CREATE TABLE IF NOT EXISTS listing_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  source TEXT NOT NULL,
  listing_key TEXT NOT NULL,
  listing_id TEXT,
  url TEXT NOT NULL,
  title TEXT,
  address TEXT,
  crawled_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES crawl_runs(id) ON DELETE CASCADE,
  UNIQUE(run_id, source, listing_key)
);

CREATE INDEX IF NOT EXISTS idx_listing_snapshots_run ON listing_snapshots(run_id);
CREATE INDEX IF NOT EXISTS idx_listing_snapshots_source ON listing_snapshots(source);
