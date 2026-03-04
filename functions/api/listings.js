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
  const areaCity = (url.searchParams.get("city") || "Istanbul").trim();
  const areaDistrict = (url.searchParams.get("district") || "Atasehir").trim();

  const where = ["area_city = ?", "area_district = ?"];
  const binds = [areaCity, areaDistrict];

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
      price_tl AS priceTl,
      gross_sqm AS grossSqm,
      net_sqm AS netSqm,
      avg_price_for_sale AS avgPriceForSale,
      endeksa_min_price AS endeksaMinPrice,
      endeksa_max_price AS endeksaMaxPrice,
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
    area: { city: areaCity, district: areaDistrict },
    source: source || null,
    activeOnly,
    limit,
    offset,
    count: result.results.length,
    listings: result.results
  });
}
