function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store"
    }
  });
}

const AREA_DEFAULTS = {
  TR: { city: "Istanbul", district: "Atasehir" },
  ES: { city: "Madrid", district: "Madrid Capital" }
};

function canonicalCountryCode(value, fallback = "TR") {
  const raw = String(value || fallback || "TR")
    .trim()
    .toUpperCase();
  return Object.prototype.hasOwnProperty.call(AREA_DEFAULTS, raw) ? raw : fallback;
}

function areaDefaultsForCountry(country) {
  return AREA_DEFAULTS[canonicalCountryCode(country)] || AREA_DEFAULTS.TR;
}

function canonicalAreaName(value, fallback) {
  const raw = String(value || fallback || "")
    .replace(/\s+/g, " ")
    .trim();
  if (!raw) {
    return String(fallback || "");
  }
  const ascii = raw
    .replace(/İ/g, "I")
    .replace(/ı/g, "i")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
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

async function parseJsonBody(request) {
  try {
    return await request.json();
  } catch {
    return null;
  }
}

function normalizeFeedbackValue(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (["good", "up", "positive", "1", "true", "real"].includes(normalized)) {
    return "good";
  }
  if (["bad", "down", "negative", "0", "false", "weak"].includes(normalized)) {
    return "bad";
  }
  return null;
}

function toInt(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.floor(n) : fallback;
}

async function feedbackCounts(DB, areaCountry, areaCity, areaDistrict, source, listingKey) {
  const row = await DB.prepare(
    `
      SELECT
        SUM(CASE WHEN feedback = 'good' THEN 1 ELSE 0 END) AS goodCount,
        SUM(CASE WHEN feedback = 'bad' THEN 1 ELSE 0 END) AS badCount
      FROM deal_feedback
      WHERE area_country = ?
        AND area_city = ?
        AND area_district = ?
        AND source = ?
        AND listing_key = ?
    `
  )
    .bind(areaCountry, areaCity, areaDistrict, source, listingKey)
    .first();

  const goodCount = Number(row?.goodCount || 0);
  const badCount = Number(row?.badCount || 0);
  return {
    goodCount,
    badCount,
    totalCount: goodCount + badCount,
    netScore: goodCount - badCount
  };
}

export async function onRequestPost(context) {
  const DB = context.env?.DB;
  if (!DB) {
    return json({ ok: false, error: "D1 binding `DB` is missing." }, 500);
  }

  const body = await parseJsonBody(context.request);
  if (!body || typeof body !== "object") {
    return json({ ok: false, error: "Invalid JSON body." }, 400);
  }

  const areaCountry = canonicalCountryCode(body.country, "TR");
  const defaults = areaDefaultsForCountry(areaCountry);
  const areaCity = canonicalAreaName(body.city, defaults.city);
  const areaDistrict = canonicalAreaName(body.district, defaults.district);
  const source = String(body.source || "").trim();
  const listingKey = String(body.listingKey || "").trim();
  const listingId = body.listingId == null ? null : String(body.listingId);
  const feedback = normalizeFeedbackValue(body.feedback);
  const noteRaw = body.note == null ? "" : String(body.note).trim();
  const note = noteRaw ? noteRaw.slice(0, 500) : null;

  if (!source || !listingKey || !feedback) {
    return json(
      {
        ok: false,
        error: "Required fields: source, listingKey, feedback ('good' or 'bad')."
      },
      400
    );
  }

  try {
    await DB.prepare(
      `
        INSERT INTO deal_feedback (
          area_country, area_city, area_district, source, listing_key, listing_id, feedback, note
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `
    )
      .bind(areaCountry, areaCity, areaDistrict, source, listingKey, listingId, feedback, note)
      .run();
  } catch (error) {
    return json(
      {
        ok: false,
        error: "Failed to store feedback. Migration may be missing.",
        details: error?.message || String(error)
      },
      500
    );
  }

  const counts = await feedbackCounts(DB, areaCountry, areaCity, areaDistrict, source, listingKey);
  return json({
    ok: true,
    area: { country: areaCountry, city: areaCity, district: areaDistrict },
    listing: { source, listingKey, listingId },
    recorded: feedback,
    feedback: counts
  });
}

export async function onRequestGet(context) {
  const DB = context.env?.DB;
  if (!DB) {
    return json({ ok: false, error: "D1 binding `DB` is missing." }, 500);
  }

  const url = new URL(context.request.url);
  const areaCountry = canonicalCountryCode(url.searchParams.get("country"), "TR");
  const defaults = areaDefaultsForCountry(areaCountry);
  const areaCity = canonicalAreaName(url.searchParams.get("city"), defaults.city);
  const areaDistrict = canonicalAreaName(url.searchParams.get("district"), defaults.district);
  const source = String(url.searchParams.get("source") || "").trim();
  const listingKey = String(url.searchParams.get("listingKey") || "").trim();
  const limit = Math.max(1, Math.min(100, toInt(url.searchParams.get("limit"), 20)));

  if (source && listingKey) {
    const counts = await feedbackCounts(DB, areaCountry, areaCity, areaDistrict, source, listingKey);
    return json({
      ok: true,
      area: { country: areaCountry, city: areaCity, district: areaDistrict },
      listing: { source, listingKey },
      feedback: counts
    });
  }

  try {
    const rowsRes = await DB.prepare(
      `
        SELECT
          created_at AS createdAt,
          source,
          listing_key AS listingKey,
          listing_id AS listingId,
          feedback,
          note
        FROM deal_feedback
        WHERE area_country = ?
          AND area_city = ?
          AND area_district = ?
        ORDER BY id DESC
        LIMIT ?
      `
    )
      .bind(areaCountry, areaCity, areaDistrict, limit)
      .all();

    return json({
      ok: true,
      area: { country: areaCountry, city: areaCity, district: areaDistrict },
      count: (rowsRes.results || []).length,
      rows: rowsRes.results || []
    });
  } catch (error) {
    return json(
      {
        ok: false,
        error: "Feedback table is not available. Run DB migrations.",
        details: error?.message || String(error)
      },
      500
    );
  }
}
