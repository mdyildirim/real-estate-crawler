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
  db.exec(fs.readFileSync(SCHEMA_PATH, "utf8"));

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
      area_city, area_district, first_seen_at, last_seen_at, last_seen_run_id, last_crawled_at, is_active
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
    ON CONFLICT(source, listing_key) DO UPDATE SET
      listing_id = excluded.listing_id,
      url = excluded.url,
      title = excluded.title,
      address = excluded.address,
      last_seen_at = excluded.last_seen_at,
      last_seen_run_id = excluded.last_seen_run_id,
      last_crawled_at = excluded.last_crawled_at,
      is_active = 1
  `);

  const insertSnapshotStmt = db.prepare(`
    INSERT INTO listing_snapshots (
      run_id, source, listing_key, listing_id, url, title, address, crawled_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(run_id, source, listing_key) DO UPDATE SET
      listing_id = excluded.listing_id,
      url = excluded.url,
      title = excluded.title,
      address = excluded.address,
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

module.exports = {
  persistRunToSqlite
};
