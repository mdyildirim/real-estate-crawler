import {
  withPrecomputed,
  buildBacktestReport,
  canonicalCountryCode,
  canonicalAreaName,
  toInt,
  toBool
} from "./deals.js";

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store"
    }
  });
}

function toFloat(value, fallback) {
  if (value == null || value === "") {
    return fallback;
  }
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

const AREA_DEFAULTS = {
  TR: { city: "Istanbul", district: "Atasehir" },
  ES: { city: "Madrid", district: "Madrid Capital" }
};

export async function onRequestGet(context) {
  const DB = context.env?.DB;
  if (!DB) {
    return json({ ok: false, error: "D1 binding `DB` is missing." }, 500);
  }

  const url = new URL(context.request.url);
  const areaCountry = canonicalCountryCode(url.searchParams.get("country"), "TR");
  const defaults = AREA_DEFAULTS[areaCountry] || AREA_DEFAULTS.TR;
  const areaCity = canonicalAreaName(url.searchParams.get("city"), defaults.city);
  const areaDistrict = canonicalAreaName(url.searchParams.get("district"), defaults.district);
  const folds = Math.max(3, Math.min(10, toInt(url.searchParams.get("folds"), 5)));
  const minComps = Math.max(3, Math.min(30, toInt(url.searchParams.get("min_comps"), 6)));
  const strictSameNeighborhood = toBool(url.searchParams.get("strict_same_neighborhood"), true);
  const minSameNeighborhoodComps = Math.max(1, Math.min(10, toInt(url.searchParams.get("min_same_neighborhood_comps"), 3)));
  const modelLambda = Math.max(0.01, Math.min(1000, toFloat(url.searchParams.get("model_lambda"), 8)));

  const rowsRes = await DB.prepare(
    `
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
        last_seen_at AS lastSeenAt
      FROM listings_current
      WHERE area_country = ?
        AND area_city = ?
        AND area_district = ?
        AND is_active = 1
    `
  )
    .bind(areaCountry, areaCity, areaDistrict)
    .all();

  const rows = (rowsRes.results || []).map((r) => ({
    ...r,
    priceTl: toFloat(r.priceTl, null),
    grossSqm: toFloat(r.grossSqm, null),
    netSqm: toFloat(r.netSqm, null),
    avgPriceForSale: toFloat(r.avgPriceForSale, null),
    endeksaMinPrice: toFloat(r.endeksaMinPrice, null),
    endeksaMaxPrice: toFloat(r.endeksaMaxPrice, null)
  }));

  const preRows = rows
    .map(withPrecomputed)
    .filter((row) => Number.isFinite(row._pricePerSqm) && row._pricePerSqm > 0);
  const report = buildBacktestReport(preRows, {
    folds,
    minComps,
    strictSameNeighborhood,
    minSameNeighborhoodComps,
    modelLambda
  });

  return json({
    ok: Boolean(report?.ok),
    area: { country: areaCountry, city: areaCity, district: areaDistrict },
    params: {
      folds,
      minComps,
      strictSameNeighborhood,
      minSameNeighborhoodComps,
      modelLambda
    },
    report
  });
}
