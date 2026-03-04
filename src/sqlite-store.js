"use strict";

const fs = require("fs");
const path = require("path");

let BetterSqlite3 = null;
try {
  BetterSqlite3 = require("better-sqlite3");
} catch (error) {
  throw new Error(
    "better-sqlite3 is required for SQLite persistence. Run `npm install` before crawling."
  );
}

const SCHEMA_PATH = path.join(__dirname, "..", "db", "schema.sql");

function persistRunToSqlite({ dbPath, runTag, payload }) {
  const dbDir = path.dirname(dbPath);
  fs.mkdirSync(dbDir, { recursive: true });
  const db = new BetterSqlite3(dbPath);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  // Upgrade older local DB files before schema index creation runs.
  ensureFeatureColumns(db);
  db.exec(fs.readFileSync(SCHEMA_PATH, "utf8"));
  ensureFeatureColumns(db);

  const nowIso = new Date().toISOString();

  const insertRunStmt = db.prepare(`
    INSERT INTO crawl_runs (
      run_tag, started_at, finished_at, area_city, area_district, raw_count, unique_count
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  const insertSourceRunStmt = db.prepare(`
    INSERT INTO source_runs (
      run_id, source, status, blocked, observed_total, crawled_listings, url, notes
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(run_id, source) DO UPDATE SET
      status = excluded.status,
      blocked = excluded.blocked,
      observed_total = excluded.observed_total,
      crawled_listings = excluded.crawled_listings,
      url = excluded.url,
      notes = excluded.notes
  `);

  const upsertListingCurrentStmt = db.prepare(`
    INSERT INTO listings_current (
      source, listing_key, listing_id, url, title, address,
      neighborhood, room_count, building_age, floor_info, price_tl, gross_sqm, net_sqm,
      avg_price_for_sale, endeksa_min_price, endeksa_max_price,
      area_city, area_district, first_seen_at, last_seen_at, last_seen_run_id, last_crawled_at, is_active
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
    ON CONFLICT(source, listing_key) DO UPDATE SET
      listing_id = excluded.listing_id,
      url = excluded.url,
      title = excluded.title,
      address = excluded.address,
      neighborhood = excluded.neighborhood,
      room_count = excluded.room_count,
      building_age = excluded.building_age,
      floor_info = excluded.floor_info,
      price_tl = excluded.price_tl,
      gross_sqm = excluded.gross_sqm,
      net_sqm = excluded.net_sqm,
      avg_price_for_sale = excluded.avg_price_for_sale,
      endeksa_min_price = excluded.endeksa_min_price,
      endeksa_max_price = excluded.endeksa_max_price,
      last_seen_at = excluded.last_seen_at,
      last_seen_run_id = excluded.last_seen_run_id,
      last_crawled_at = excluded.last_crawled_at,
      is_active = 1
  `);

  const insertSnapshotStmt = db.prepare(`
    INSERT INTO listing_snapshots (
      run_id, source, listing_key, listing_id, url, title, address,
      neighborhood, room_count, building_age, floor_info, price_tl, gross_sqm, net_sqm,
      avg_price_for_sale, endeksa_min_price, endeksa_max_price, crawled_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(run_id, source, listing_key) DO UPDATE SET
      listing_id = excluded.listing_id,
      url = excluded.url,
      title = excluded.title,
      address = excluded.address,
      neighborhood = excluded.neighborhood,
      room_count = excluded.room_count,
      building_age = excluded.building_age,
      floor_info = excluded.floor_info,
      price_tl = excluded.price_tl,
      gross_sqm = excluded.gross_sqm,
      net_sqm = excluded.net_sqm,
      avg_price_for_sale = excluded.avg_price_for_sale,
      endeksa_min_price = excluded.endeksa_min_price,
      endeksa_max_price = excluded.endeksa_max_price,
      crawled_at = excluded.crawled_at
  `);

  const deactivateSourceStmt = db.prepare(`
    UPDATE listings_current
    SET is_active = 0
    WHERE area_city = ?
      AND area_district = ?
      AND source = ?
      AND last_seen_run_id <> ?
  `);

  const tx = db.transaction(() => {
    const summary = payload.summary || {};
    const area = summary.area || {};
    const results = payload.results || [];
    const listings = payload.listings || [];
    const sourceSummaries = summary.sourceSummaries || [];

    const runInsert = insertRunStmt.run(
      runTag,
      summary.startedAt || nowIso,
      summary.finishedAt || nowIso,
      area.city || "Istanbul",
      area.district || "Atasehir",
      Number(summary.totals?.crawledRaw || 0),
      Number(summary.totals?.crawledUnique || 0)
    );
    const runId = Number(runInsert.lastInsertRowid);

    const resultNotesBySource = new Map(
      results.map((r) => [r.source, Array.isArray(r.notes) ? r.notes.join(" | ") : ""])
    );

    for (const s of sourceSummaries) {
      insertSourceRunStmt.run(
        runId,
        s.source,
        s.status || "unknown",
        s.blocked ? 1 : 0,
        s.observedTotal == null ? null : Number(s.observedTotal),
        Number(s.crawledListings || 0),
        s.url || "",
        resultNotesBySource.get(s.source) || ""
      );
    }

    for (const listing of listings) {
      const listingKey = listing.listingKey || listing.listingId || listing.url;
      const crawledAt = listing.crawledAt || summary.finishedAt || nowIso;
      upsertListingCurrentStmt.run(
        listing.source || "",
        listingKey,
        listing.listingId || "",
        listing.url || "",
        listing.title || "",
        listing.address || "",
        listing.neighborhood || "",
        listing.roomCount || "",
        listing.buildingAge || "",
        listing.floorInfo || "",
        toNullableNumber(listing.priceTl),
        toNullableNumber(listing.grossSqm),
        toNullableNumber(listing.netSqm),
        toNullableNumber(listing.avgPriceForSale),
        toNullableNumber(listing.endeksaMinPrice),
        toNullableNumber(listing.endeksaMaxPrice),
        area.city || "Istanbul",
        area.district || "Atasehir",
        crawledAt,
        crawledAt,
        runId,
        crawledAt
      );
      insertSnapshotStmt.run(
        runId,
        listing.source || "",
        listingKey,
        listing.listingId || "",
        listing.url || "",
        listing.title || "",
        listing.address || "",
        listing.neighborhood || "",
        listing.roomCount || "",
        listing.buildingAge || "",
        listing.floorInfo || "",
        toNullableNumber(listing.priceTl),
        toNullableNumber(listing.grossSqm),
        toNullableNumber(listing.netSqm),
        toNullableNumber(listing.avgPriceForSale),
        toNullableNumber(listing.endeksaMinPrice),
        toNullableNumber(listing.endeksaMaxPrice),
        crawledAt
      );
    }

    const successfulSources = sourceSummaries
      .filter((s) => s.status === "ok")
      .map((s) => s.source);
    for (const source of successfulSources) {
      deactivateSourceStmt.run(area.city || "Istanbul", area.district || "Atasehir", source, runId);
    }

    return {
      runId,
      storedListings: listings.length
    };
  });

  const out = tx();
  db.close();
  return out;
}

function toNullableNumber(value) {
  if (value == null || value === "") {
    return null;
  }
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function ensureFeatureColumns(db) {
  const alterStatements = [
    "ALTER TABLE listings_current ADD COLUMN neighborhood TEXT",
    "ALTER TABLE listings_current ADD COLUMN room_count TEXT",
    "ALTER TABLE listings_current ADD COLUMN building_age TEXT",
    "ALTER TABLE listings_current ADD COLUMN floor_info TEXT",
    "ALTER TABLE listings_current ADD COLUMN price_tl INTEGER",
    "ALTER TABLE listings_current ADD COLUMN gross_sqm REAL",
    "ALTER TABLE listings_current ADD COLUMN net_sqm REAL",
    "ALTER TABLE listings_current ADD COLUMN avg_price_for_sale INTEGER",
    "ALTER TABLE listings_current ADD COLUMN endeksa_min_price INTEGER",
    "ALTER TABLE listings_current ADD COLUMN endeksa_max_price INTEGER",
    "ALTER TABLE listing_snapshots ADD COLUMN neighborhood TEXT",
    "ALTER TABLE listing_snapshots ADD COLUMN room_count TEXT",
    "ALTER TABLE listing_snapshots ADD COLUMN building_age TEXT",
    "ALTER TABLE listing_snapshots ADD COLUMN floor_info TEXT",
    "ALTER TABLE listing_snapshots ADD COLUMN price_tl INTEGER",
    "ALTER TABLE listing_snapshots ADD COLUMN gross_sqm REAL",
    "ALTER TABLE listing_snapshots ADD COLUMN net_sqm REAL",
    "ALTER TABLE listing_snapshots ADD COLUMN avg_price_for_sale INTEGER",
    "ALTER TABLE listing_snapshots ADD COLUMN endeksa_min_price INTEGER",
    "ALTER TABLE listing_snapshots ADD COLUMN endeksa_max_price INTEGER",
    "CREATE INDEX IF NOT EXISTS idx_listings_current_price_tl ON listings_current(price_tl)",
    "CREATE INDEX IF NOT EXISTS idx_listings_current_room_neighborhood ON listings_current(room_count, neighborhood)"
  ];

  for (const sql of alterStatements) {
    try {
      db.exec(sql);
    } catch (error) {
      if (!/duplicate column name|already exists|no such table/i.test(String(error.message || error))) {
        throw error;
      }
    }
  }
}

module.exports = {
  persistRunToSqlite
};
