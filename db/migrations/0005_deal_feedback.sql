CREATE TABLE IF NOT EXISTS deal_feedback (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  area_city TEXT NOT NULL,
  area_district TEXT NOT NULL,
  source TEXT NOT NULL,
  listing_key TEXT NOT NULL,
  listing_id TEXT,
  feedback TEXT NOT NULL CHECK(feedback IN ('good', 'bad')),
  note TEXT
);

CREATE INDEX IF NOT EXISTS idx_deal_feedback_area_created
  ON deal_feedback(area_city, area_district, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_deal_feedback_listing
  ON deal_feedback(source, listing_key, created_at DESC);
