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

function normalizeBoolFlag(value, fallback = true) {
  if (value == null) {
    return fallback;
  }
  return !["0", "false", "no"].includes(String(value).toLowerCase());
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

export async function onRequestGet(context) {
  const DB = context.env?.DB;
  if (!DB) {
    return json({ ok: false, error: "D1 binding `DB` is missing." }, 500);
  }

  const url = new URL(context.request.url);
  const limit = Math.max(1, Math.min(200, toInt(url.searchParams.get("limit"), 50)));
  const offset = Math.max(0, toInt(url.searchParams.get("offset"), 0));
  const source = (url.searchParams.get("source") || "").trim();
  const activeOnly = normalizeBoolFlag(url.searchParams.get("active"), true);
  const areaCountry = canonicalCountryCode(url.searchParams.get("country"), "TR");
  const defaults = areaDefaultsForCountry(areaCountry);
  const areaCity = canonicalAreaName(url.searchParams.get("city"), defaults.city);
  const areaDistrict = canonicalAreaName(url.searchParams.get("district"), defaults.district);

  const where = ["area_country = ?", "area_city = ?", "area_district = ?"];
  const binds = [areaCountry, areaCity, areaDistrict];

  if (source) {
    where.push("source = ?");
    binds.push(source);
  }
  if (activeOnly) {
    where.push("is_active = 1");
  }

  const sql = `
    SELECT
      source,
      listing_key AS listingKey,
      listing_id AS listingId,
      url,
      title,
      address,
      neighborhood,
      room_count AS roomCount,
      building_age AS buildingAge,
      floor_info AS floorInfo,
      deed_status AS deedStatus,
      credit_suitability AS creditSuitability,
      in_site AS inSite,
      usage_status AS usageStatus,
      price_tl AS priceTl,
      gross_sqm AS grossSqm,
      net_sqm AS netSqm,
      avg_price_for_sale AS avgPriceForSale,
      endeksa_min_price AS endeksaMinPrice,
      endeksa_max_price AS endeksaMaxPrice,
      area_country AS areaCountry,
      area_city AS areaCity,
      area_district AS areaDistrict,
      first_seen_at AS firstSeenAt,
      last_seen_at AS lastSeenAt,
      last_crawled_at AS lastCrawledAt,
      is_active AS isActive
    FROM listings_current
    WHERE ${where.join(" AND ")}
    ORDER BY datetime(last_seen_at) DESC
    LIMIT ?
    OFFSET ?
  `;

  const result = await DB.prepare(sql)
    .bind(...binds, limit, offset)
    .all();

  return json({
    ok: true,
    area: { country: areaCountry, city: areaCity, district: areaDistrict },
    source: source || null,
    activeOnly,
    limit,
    offset,
    count: result.results.length,
    listings: result.results
  });
}
