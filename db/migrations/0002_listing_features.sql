ALTER TABLE listings_current ADD COLUMN neighborhood TEXT;
ALTER TABLE listings_current ADD COLUMN room_count TEXT;
ALTER TABLE listings_current ADD COLUMN building_age TEXT;
ALTER TABLE listings_current ADD COLUMN floor_info TEXT;
ALTER TABLE listings_current ADD COLUMN price_tl INTEGER;
ALTER TABLE listings_current ADD COLUMN gross_sqm REAL;
ALTER TABLE listings_current ADD COLUMN net_sqm REAL;
ALTER TABLE listings_current ADD COLUMN avg_price_for_sale INTEGER;
ALTER TABLE listings_current ADD COLUMN endeksa_min_price INTEGER;
ALTER TABLE listings_current ADD COLUMN endeksa_max_price INTEGER;

ALTER TABLE listing_snapshots ADD COLUMN neighborhood TEXT;
ALTER TABLE listing_snapshots ADD COLUMN room_count TEXT;
ALTER TABLE listing_snapshots ADD COLUMN building_age TEXT;
ALTER TABLE listing_snapshots ADD COLUMN floor_info TEXT;
ALTER TABLE listing_snapshots ADD COLUMN price_tl INTEGER;
ALTER TABLE listing_snapshots ADD COLUMN gross_sqm REAL;
ALTER TABLE listing_snapshots ADD COLUMN net_sqm REAL;
ALTER TABLE listing_snapshots ADD COLUMN avg_price_for_sale INTEGER;
ALTER TABLE listing_snapshots ADD COLUMN endeksa_min_price INTEGER;
ALTER TABLE listing_snapshots ADD COLUMN endeksa_max_price INTEGER;

CREATE INDEX IF NOT EXISTS idx_listings_current_price_tl ON listings_current(price_tl);
CREATE INDEX IF NOT EXISTS idx_listings_current_room_neighborhood ON listings_current(room_count, neighborhood);
