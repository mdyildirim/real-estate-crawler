function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store"
    }
  });
}

function normalizeSourceSummary(row) {
  return {
    source: row?.source || "",
    status: row?.status || "unknown",
    blocked: row?.blocked ? 1 : 0,
    observedTotal: row?.observedTotal == null ? null : Number(row.observedTotal),
    crawledListings: Number(row?.crawledListings || 0),
    url: row?.url || ""
  };
}

function normalizeListing(row) {
  const listingId = row?.listingId ? String(row.listingId) : "";
  const listingKey = row?.listingKey || listingId || row?.url || "";
  return {
    source: row?.source || "",
    listingKey,
    listingId,
    url: row?.url || "",
    title: row?.title || "",
    address: row?.address || "",
    neighborhood: row?.neighborhood || "",
    roomCount: row?.roomCount || "",
    buildingAge: row?.buildingAge || "",
    floorInfo: row?.floorInfo || "",
    priceTl: toNullableNumber(row?.priceTl),
    grossSqm: toNullableNumber(row?.grossSqm),
    netSqm: toNullableNumber(row?.netSqm),
    avgPriceForSale: toNullableNumber(row?.avgPriceForSale),
    endeksaMinPrice: toNullableNumber(row?.endeksaMinPrice),
    endeksaMaxPrice: toNullableNumber(row?.endeksaMaxPrice),
    crawledAt: row?.crawledAt || new Date().toISOString()
  };
}

function toNullableNumber(value) {
  if (value == null || value === "") {
    return null;
  }
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

async function parseJsonBody(request) {
  try {
    return await request.json();
  } catch {
    return null;
  }
}

async function upsertRun(DB, summary, runTag) {
  const startedAt = summary?.startedAt || new Date().toISOString();
  const finishedAt = summary?.finishedAt || startedAt;
  const areaCity = summary?.area?.city || "Istanbul";
  const areaDistrict = summary?.area?.district || "Atasehir";
  const rawCount = Number(summary?.totals?.crawledRaw || 0);
  const uniqueCount = Number(summary?.totals?.crawledUnique || 0);

  await DB.prepare(
    `
      INSERT OR IGNORE INTO crawl_runs (
        run_tag, started_at, finished_at, area_city, area_district, raw_count, unique_count
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    `
  )
    .bind(runTag, startedAt, finishedAt, areaCity, areaDistrict, rawCount, uniqueCount)
    .run();

  await DB.prepare(
    `
      UPDATE crawl_runs
      SET started_at = ?, finished_at = ?, area_city = ?, area_district = ?, raw_count = ?, unique_count = ?
      WHERE run_tag = ?
    `
  )
    .bind(startedAt, finishedAt, areaCity, areaDistrict, rawCount, uniqueCount, runTag)
    .run();

  const row = await DB.prepare(`SELECT id FROM crawl_runs WHERE run_tag = ?`).bind(runTag).first();
  return {
    runId: row?.id,
    areaCity,
    areaDistrict
  };
}

function normalizeError(error) {
  if (!error) {
    return "Unknown error";
  }
  if (typeof error === "string") {
    return error;
  }
  return error.message || String(error);
}

async function runInBatches(DB, statements, size = 80) {
  if (!Array.isArray(statements) || statements.length === 0) {
    return;
  }
  const chunkSize = Math.max(1, Math.min(200, size));
  for (let i = 0; i < statements.length; i += chunkSize) {
    const chunk = statements.slice(i, i + chunkSize);
    await DB.batch(chunk);
  }
}

export async function onRequestPost(context) {
  const DB = context.env?.DB;
  if (!DB) {
    return json({ ok: false, error: "D1 binding `DB` is missing." }, 500);
  }

  const expectedToken = context.env?.INGEST_API_TOKEN;
  if (expectedToken) {
    const got = context.request.headers.get("authorization") || "";
    if (got !== `Bearer ${expectedToken}`) {
      return json({ ok: false, error: "Unauthorized" }, 401);
    }
  }

  const payload = await parseJsonBody(context.request);
  if (!payload || typeof payload !== "object") {
    return json({ ok: false, error: "Invalid JSON body" }, 400);
  }

  const summary = payload.summary || {};
  const runTag = summary.runTag || `manual-${Date.now()}`;
  const sourceSummaries = Array.isArray(summary.sourceSummaries)
    ? summary.sourceSummaries.map(normalizeSourceSummary)
    : [];
  const resultsBySource = new Map(
    (Array.isArray(payload.results) ? payload.results : []).map((r) => [
      r.source,
      Array.isArray(r.notes) ? r.notes.join(" | ") : ""
    ])
  );
  const uniqueListings = Array.isArray(payload.listings) ? payload.listings.map(normalizeListing) : [];

  try {
    const runInfo = await upsertRun(DB, summary, runTag);
    if (!runInfo.runId) {
      return json({ ok: false, error: "Failed to persist run metadata." }, 500);
    }

    const sourceRunStatements = sourceSummaries.map((row) =>
      DB.prepare(
        `
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
        `
      ).bind(
        runInfo.runId,
        row.source,
        row.status,
        row.blocked,
        row.observedTotal,
        row.crawledListings,
        row.url,
        resultsBySource.get(row.source) || ""
      )
    );
    await runInBatches(DB, sourceRunStatements, 40);

    const upsertCurrentStatements = uniqueListings.map((listing) =>
      DB.prepare(
        `
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
        `
      ).bind(
        listing.source,
        listing.listingKey,
        listing.listingId,
        listing.url,
        listing.title,
        listing.address,
        listing.neighborhood,
        listing.roomCount,
        listing.buildingAge,
        listing.floorInfo,
        listing.priceTl,
        listing.grossSqm,
        listing.netSqm,
        listing.avgPriceForSale,
        listing.endeksaMinPrice,
        listing.endeksaMaxPrice,
        runInfo.areaCity,
        runInfo.areaDistrict,
        listing.crawledAt,
        listing.crawledAt,
        runInfo.runId,
        listing.crawledAt
      )
    );
    await runInBatches(DB, upsertCurrentStatements, 80);

    const snapshotStatements = uniqueListings.map((listing) =>
      DB.prepare(
        `
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
        `
      ).bind(
        runInfo.runId,
        listing.source,
        listing.listingKey,
        listing.listingId,
        listing.url,
        listing.title,
        listing.address,
        listing.neighborhood,
        listing.roomCount,
        listing.buildingAge,
        listing.floorInfo,
        listing.priceTl,
        listing.grossSqm,
        listing.netSqm,
        listing.avgPriceForSale,
        listing.endeksaMinPrice,
        listing.endeksaMaxPrice,
        listing.crawledAt
      )
    );
    await runInBatches(DB, snapshotStatements, 80);

    const successfulSources = sourceSummaries.filter((r) => r.status === "ok").map((r) => r.source);
    const staleStatements = successfulSources.map((source) =>
      DB.prepare(
        `
          UPDATE listings_current
          SET is_active = 0
          WHERE area_city = ?
            AND area_district = ?
            AND source = ?
            AND last_seen_run_id <> ?
        `
      ).bind(runInfo.areaCity, runInfo.areaDistrict, source, runInfo.runId)
    );
    await runInBatches(DB, staleStatements, 40);

    return json({
      ok: true,
      runTag,
      runId: runInfo.runId,
      receivedSourceSummaries: sourceSummaries.length,
      receivedListings: uniqueListings.length
    });
  } catch (error) {
    return json(
      {
        ok: false,
        error: "Ingest failed.",
        details: normalizeError(error)
      },
      500
    );
  }
}

export async function onRequestGet(context) {
  return json({
    ok: true,
    endpoint: "/api/ingest",
    method: "POST",
    auth: context.env?.INGEST_API_TOKEN ? "Bearer token required" : "No token configured"
  });
}
