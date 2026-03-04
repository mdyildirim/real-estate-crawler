ALTER TABLE listings_current ADD COLUMN deed_status TEXT;
ALTER TABLE listings_current ADD COLUMN credit_suitability TEXT;
ALTER TABLE listings_current ADD COLUMN in_site TEXT;
ALTER TABLE listings_current ADD COLUMN usage_status TEXT;

ALTER TABLE listing_snapshots ADD COLUMN deed_status TEXT;
ALTER TABLE listing_snapshots ADD COLUMN credit_suitability TEXT;
ALTER TABLE listing_snapshots ADD COLUMN in_site TEXT;
ALTER TABLE listing_snapshots ADD COLUMN usage_status TEXT;
