function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store"
    }
  });
}

function toInt(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return fallback;
  }
  return Math.floor(n);
}

function inClausePlaceholders(count) {
  return new Array(count).fill("?").join(", ");
}

function canonicalAreaName(value, fallback) {
  const raw = String(value || fallback || "")
    .replace(/\s+/g, " ")
    .trim();
  if (!raw) {
    return String(fallback || "");
  }
  const ascii = raw
    .toLocaleLowerCase("tr-TR")
    .replace(/ç/g, "c")
    .replace(/ğ/g, "g")
    .replace(/ı/g, "i")
    .replace(/ö/g, "o")
    .replace(/ş/g, "s")
    .replace(/ü/g, "u")
    .replace(/[^a-z0-9 ]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!ascii) {
    return String(fallback || "");
  }
  return ascii
    .split(" ")
    .map((part) => (part ? part[0].toUpperCase() + part.slice(1) : ""))
    .join(" ");
}

export async function onRequestGet(context) {
  const DB = context.env?.DB;
  if (!DB) {
    return json({ ok: false, error: "D1 binding `DB` is missing." }, 500);
  }

  const url = new URL(context.request.url);
  const limit = Math.max(1, Math.min(100, toInt(url.searchParams.get("limit"), 20)));
  const areaCity = canonicalAreaName(url.searchParams.get("city"), "Istanbul");
  const areaDistrict = canonicalAreaName(url.searchParams.get("district"), "Atasehir");

  const runsRes = await DB.prepare(
    `
      SELECT
        id,
        run_tag AS runTag,
        started_at AS startedAt,
        finished_at AS finishedAt,
        area_city AS areaCity,
        area_district AS areaDistrict,
        raw_count AS rawCount,
        unique_count AS uniqueCount,
        created_at AS createdAt
      FROM crawl_runs
      WHERE area_city = ? AND area_district = ?
      ORDER BY id DESC
      LIMIT ?
    `
  )
    .bind(areaCity, areaDistrict, limit)
    .all();

  const runs = runsRes.results || [];
  if (runs.length === 0) {
    return json({ ok: true, area: { city: areaCity, district: areaDistrict }, runs: [] });
  }

  const runIds = runs.map((r) => r.id);
  const statsSql = `
    SELECT
      run_id AS runId,
      source,
      status,
      blocked,
      observed_total AS observedTotal,
      crawled_listings AS crawledListings,
      url,
      notes
    FROM source_runs
    WHERE run_id IN (${inClausePlaceholders(runIds.length)})
    ORDER BY run_id DESC, source
  `;
  const statsRes = await DB.prepare(statsSql)
    .bind(...runIds)
    .all();

  const statsByRun = new Map();
  for (const row of statsRes.results || []) {
    const bucket = statsByRun.get(row.runId) || [];
    bucket.push({
      source: row.source,
      status: row.status,
      blocked: Boolean(row.blocked),
      observedTotal: row.observedTotal,
      crawledListings: row.crawledListings,
      url: row.url,
      notes: row.notes || ""
    });
    statsByRun.set(row.runId, bucket);
  }

  return json({
    ok: true,
    area: { city: areaCity, district: areaDistrict },
    runs: runs.map((r) => ({
      ...r,
      sourceSummaries: statsByRun.get(r.id) || []
    }))
  });
}
