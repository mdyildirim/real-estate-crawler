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

function canonicalAreaName(value, fallback = "") {
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

function safeParseJson(value) {
  if (!value) {
    return null;
  }
  try {
    return JSON.parse(value);
  } catch {
    return { raw: String(value) };
  }
}

export async function onRequestGet(context) {
  const DB = context.env?.DB;
  if (!DB) {
    return json({ ok: false, error: "D1 binding `DB` is missing." }, 500);
  }

  const url = new URL(context.request.url);
  const limit = Math.max(1, Math.min(200, toInt(url.searchParams.get("limit"), 50)));
  const level = (url.searchParams.get("level") || "").trim();
  const eventType = (url.searchParams.get("event") || "").trim();
  const runTag = (url.searchParams.get("run_tag") || "").trim();

  const areaCityRaw = (url.searchParams.get("city") || "").trim();
  const areaDistrictRaw = (url.searchParams.get("district") || "").trim();
  const areaCity = areaCityRaw ? canonicalAreaName(areaCityRaw) : "";
  const areaDistrict = areaDistrictRaw ? canonicalAreaName(areaDistrictRaw) : "";

  const where = [];
  const binds = [];

  if (level) {
    where.push("level = ?");
    binds.push(level);
  }
  if (eventType) {
    where.push("event_type = ?");
    binds.push(eventType);
  }
  if (runTag) {
    where.push("run_tag = ?");
    binds.push(runTag);
  }
  if (areaCity) {
    where.push("area_city = ?");
    binds.push(areaCity);
  }
  if (areaDistrict) {
    where.push("area_district = ?");
    binds.push(areaDistrict);
  }

  const sql = `
    SELECT
      id,
      created_at AS createdAt,
      level,
      event_type AS eventType,
      run_tag AS runTag,
      run_id AS runId,
      area_city AS areaCity,
      area_district AS areaDistrict,
      source,
      message,
      metadata_json AS metadataJson
    FROM system_logs
    ${where.length > 0 ? `WHERE ${where.join(" AND ")}` : ""}
    ORDER BY id DESC
    LIMIT ?
  `;

  const result = await DB.prepare(sql)
    .bind(...binds, limit)
    .all();

  return json({
    ok: true,
    filters: {
      limit,
      level: level || null,
      eventType: eventType || null,
      runTag: runTag || null,
      areaCity: areaCity || null,
      areaDistrict: areaDistrict || null
    },
    logs: (result.results || []).map((row) => ({
      ...row,
      metadata: safeParseJson(row.metadataJson)
    }))
  });
}
