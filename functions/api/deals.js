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
  if (value == null || value === "") {
    return fallback;
  }
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return fallback;
  }
  return Math.floor(n);
}

function toFloat(value, fallback) {
  if (value == null || value === "") {
    return fallback;
  }
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return fallback;
  }
  return n;
}

function toBool(value, fallback) {
  if (value == null) {
    return fallback;
  }
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "evet"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "hayir", "hayır"].includes(normalized)) {
    return false;
  }
  return fallback;
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
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

function bucketLabel(bucket) {
  if (bucket === "neighborhood+room") {
    return "Mahalle + oda sayısı";
  }
  if (bucket === "neighborhood+multifactor") {
    return "Mahalle + çok faktörlü benzerlik";
  }
  if (bucket === "district+room") {
    return "İlçe + oda sayısı";
  }
  if (bucket === "district+multifactor") {
    return "İlçe + çok faktörlü benzerlik";
  }
  return "İlçe + m² benzerliği";
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

function normalizeForMatch(text) {
  return String(text || "")
    .trim()
    .replace(/İ/g, "I")
    .replace(/ı/g, "i")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function parseRoomStats(roomText) {
  const normalized = normalizeForMatch(roomText || "");
  if (!normalized) {
    return { bedrooms: null, livingRooms: null, total: null };
  }
  const spanish = normalized.match(/([0-9]+(?:[.,][0-9]+)?)\s*(?:hab|habitaciones?|dormitorios?)/);
  if (spanish) {
    const total = Number(spanish[1].replace(",", "."));
    return { bedrooms: null, livingRooms: null, total: Number.isFinite(total) ? total : null };
  }
  const plus = normalized.match(/([0-9]+(?:[.,][0-9]+)?)\s*\+\s*([0-9]+(?:[.,][0-9]+)?)/);
  if (plus) {
    const bedrooms = Number(plus[1].replace(",", "."));
    const livingRooms = Number(plus[2].replace(",", "."));
    return {
      bedrooms: Number.isFinite(bedrooms) ? bedrooms : null,
      livingRooms: Number.isFinite(livingRooms) ? livingRooms : null,
      total: Number.isFinite(bedrooms) && Number.isFinite(livingRooms) ? bedrooms + livingRooms : null
    };
  }
  const single = normalized.match(/([0-9]+(?:[.,][0-9]+)?)/);
  if (single) {
    const total = Number(single[1].replace(",", "."));
    return { bedrooms: null, livingRooms: null, total: Number.isFinite(total) ? total : null };
  }
  return { bedrooms: null, livingRooms: null, total: null };
}

function parseBuildingAgeYears(ageText) {
  const normalized = normalizeForMatch(ageText || "");
  if (!normalized) {
    return null;
  }
  if (
    normalized.includes("sifir") ||
    normalized.includes("yeni bina") ||
    normalized.includes("obra nueva") ||
    normalized.includes("a estrenar") ||
    normalized === "0" ||
    normalized === "0 yas"
  ) {
    return 0;
  }
  const range = normalized.match(/([0-9]{1,2})\s*-\s*([0-9]{1,2})/);
  if (range) {
    const min = Number(range[1]);
    const max = Number(range[2]);
    if (Number.isFinite(min) && Number.isFinite(max)) {
      return (min + max) / 2;
    }
  }
  const plus = normalized.match(/([0-9]{1,2})\s*(ve\s*uzeri|uzeri|ustu|ustunde)/);
  if (plus) {
    const start = Number(plus[1]);
    if (Number.isFinite(start)) {
      return start + 5;
    }
  }
  const plusEs = normalized.match(/([0-9]{1,2})\s*(anos|anos o mas|o mas|o más)/);
  if (plusEs) {
    const start = Number(plusEs[1]);
    if (Number.isFinite(start)) {
      return start + 5;
    }
  }
  const single = normalized.match(/([0-9]{1,2})/);
  if (single) {
    const age = Number(single[1]);
    return Number.isFinite(age) ? age : null;
  }
  return null;
}

function parseFloorCategory(floorText) {
  const normalized = normalizeForMatch(floorText || "");
  if (!normalized) {
    return "unknown";
  }
  if (normalized.includes("bodrum")) {
    return "basement";
  }
  if (normalized.includes("zemin") || normalized.includes("giris") || normalized.includes("bahce")) {
    return "ground";
  }
  if (normalized.includes("planta baja") || normalized.includes("bajo") || normalized.includes("entresuelo")) {
    return "ground";
  }
  if (normalized.includes("cati") || normalized.includes("teras") || normalized.includes("en ust") || normalized.includes("son kat")) {
    return "top";
  }
  if (normalized.includes("atico") || normalized.includes("ultima planta")) {
    return "top";
  }
  if (normalized.includes("ara kat")) {
    return "middle";
  }
  const numericFloor = normalized.match(/([0-9]{1,2})\s*\.?\s*kat|([0-9]{1,2})kat/);
  const numericFloorEs = normalized.match(/([0-9]{1,2})\s*(?:planta|piso)/);
  const levelEs = Number(numericFloorEs ? numericFloorEs[1] : NaN);
  if (Number.isFinite(levelEs)) {
    if (levelEs <= 0) {
      return "ground";
    }
    return "middle";
  }
  if (numericFloor) {
    const level = Number(numericFloor[1] || numericFloor[2]);
    if (Number.isFinite(level) && level <= 0) {
      return "ground";
    }
    if (Number.isFinite(level) && level >= 1) {
      return "middle";
    }
  }
  return "unknown";
}

function floorDesirabilityScore(category) {
  if (category === "middle") {
    return 1;
  }
  if (category === "top") {
    return 0.66;
  }
  if (category === "ground") {
    return 0.55;
  }
  if (category === "basement") {
    return 0.3;
  }
  return 0.6;
}

function floorSimilarity(targetCategory, compCategory) {
  if (!targetCategory || !compCategory) {
    return 0.5;
  }
  if (targetCategory === compCategory) {
    return 1;
  }
  if (targetCategory === "unknown" || compCategory === "unknown") {
    return 0.6;
  }
  if (
    (targetCategory === "middle" && compCategory === "top") ||
    (targetCategory === "top" && compCategory === "middle")
  ) {
    return 0.7;
  }
  if (
    (targetCategory === "middle" && compCategory === "ground") ||
    (targetCategory === "ground" && compCategory === "middle")
  ) {
    return 0.62;
  }
  if (
    (targetCategory === "top" && compCategory === "ground") ||
    (targetCategory === "ground" && compCategory === "top")
  ) {
    return 0.5;
  }
  return 0.35;
}

function parseAssetClass(titleText) {
  const normalized = normalizeForMatch(titleText || "").replace(/-/g, " ");
  if (!normalized) {
    return "unknown";
  }
  if (normalized.includes("prefabrik")) {
    return "prefabrik";
  }
  if (normalized.includes("residence")) {
    return "residence";
  }
  if (normalized.includes("villa")) {
    return "villa";
  }
  if (normalized.includes("chalet")) {
    return "villa";
  }
  if (normalized.includes("mustakil")) {
    return "mustakil";
  }
  if (normalized.includes("casa")) {
    return "mustakil";
  }
  if (normalized.includes("daire") || normalized.includes("apartman")) {
    return "daire";
  }
  if (
    normalized.includes("piso") ||
    normalized.includes("apartamento") ||
    normalized.includes("apartament") ||
    normalized.includes("estudio") ||
    normalized.includes("atico") ||
    normalized.includes("duplex")
  ) {
    return "daire";
  }
  if (normalized.includes("ofis") || normalized.includes("isyeri")) {
    return "ofis";
  }
  if (normalized.includes("oficina") || normalized.includes("local")) {
    return "ofis";
  }
  return "unknown";
}

function isApartmentLikeAsset(assetClass) {
  return assetClass === "daire" || assetClass === "residence";
}

function requiresStrictAssetMatch(assetClass) {
  return assetClass === "prefabrik" || assetClass === "villa" || assetClass === "mustakil" || assetClass === "ofis";
}

function assetClassSimilarity(targetClass, compClass) {
  if (!targetClass || !compClass) {
    return 0.55;
  }
  if (targetClass === compClass) {
    return 1;
  }
  if (targetClass === "unknown" || compClass === "unknown") {
    return 0.62;
  }
  if (isApartmentLikeAsset(targetClass) && isApartmentLikeAsset(compClass)) {
    return 0.72;
  }
  return 0.25;
}

function parseDeedCategory(deedText) {
  const normalized = normalizeForMatch(deedText || "").replace(/-/g, " ");
  if (!normalized) {
    return "unknown";
  }
  if (normalized.includes("hisseli")) {
    return "hisseli";
  }
  if (normalized.includes("mustakil")) {
    return "mustakil";
  }
  if (normalized.includes("kat mulkiyeti")) {
    return "kat_mulkiyeti";
  }
  if (normalized.includes("kat irtifaki")) {
    return "kat_irtifaki";
  }
  if (normalized.includes("devre mulk")) {
    return "devre_mulk";
  }
  return "other";
}

function deedScore(category) {
  if (category === "mustakil") {
    return 1;
  }
  if (category === "kat_mulkiyeti") {
    return 0.85;
  }
  if (category === "kat_irtifaki") {
    return 0.72;
  }
  if (category === "devre_mulk") {
    return 0.45;
  }
  if (category === "hisseli") {
    return 0.2;
  }
  if (category === "other") {
    return 0.58;
  }
  return 0.55;
}

function deedSimilarity(targetCategory, compCategory) {
  if (!targetCategory || !compCategory) {
    return 0.55;
  }
  if (targetCategory === compCategory) {
    return 1;
  }
  if (targetCategory === "unknown" || compCategory === "unknown") {
    return 0.62;
  }
  if (targetCategory === "hisseli" || compCategory === "hisseli") {
    return 0.32;
  }
  if (
    (targetCategory === "mustakil" && compCategory === "kat_mulkiyeti") ||
    (targetCategory === "kat_mulkiyeti" && compCategory === "mustakil")
  ) {
    return 0.62;
  }
  return 0.5;
}

function parseCreditCategory(text) {
  const normalized = normalizeForMatch(text || "").replace(/-/g, " ");
  if (!normalized) {
    return "unknown";
  }
  if (normalized.includes("uygun degil") || normalized.includes("uygun değil") || normalized === "hayir") {
    return "not_suitable";
  }
  if (normalized.includes("krediye uygun") || normalized === "uygun" || normalized === "evet") {
    return "suitable";
  }
  return "unknown";
}

function creditScore(category) {
  if (category === "suitable") {
    return 1;
  }
  if (category === "not_suitable") {
    return 0.2;
  }
  return 0.55;
}

function creditSimilarity(targetCategory, compCategory) {
  if (!targetCategory || !compCategory) {
    return 0.55;
  }
  if (targetCategory === compCategory) {
    return 1;
  }
  if (targetCategory === "unknown" || compCategory === "unknown") {
    return 0.62;
  }
  return 0.35;
}

function parseSiteCategory(text) {
  const normalized = normalizeForMatch(text || "").replace(/-/g, " ");
  if (!normalized) {
    return "unknown";
  }
  if (normalized === "evet" || normalized.includes("site icerisinde evet")) {
    return "yes";
  }
  if (normalized === "hayir" || normalized.includes("site icerisinde hayir")) {
    return "no";
  }
  return "unknown";
}

function siteScore(category) {
  if (category === "yes") {
    return 1;
  }
  if (category === "no") {
    return 0.45;
  }
  return 0.58;
}

function siteSimilarity(targetCategory, compCategory) {
  if (!targetCategory || !compCategory) {
    return 0.55;
  }
  if (targetCategory === compCategory) {
    return 1;
  }
  if (targetCategory === "unknown" || compCategory === "unknown") {
    return 0.62;
  }
  return 0.45;
}

function parseUsageCategory(text) {
  const normalized = normalizeForMatch(text || "").replace(/-/g, " ");
  if (!normalized) {
    return "unknown";
  }
  if (normalized.includes("bos")) {
    return "empty";
  }
  if (normalized.includes("kiraci")) {
    return "tenant";
  }
  if (normalized.includes("mulk sahibi")) {
    return "owner";
  }
  return "other";
}

function usageScore(category) {
  if (category === "empty") {
    return 1;
  }
  if (category === "owner") {
    return 0.64;
  }
  if (category === "tenant") {
    return 0.45;
  }
  if (category === "other") {
    return 0.56;
  }
  return 0.58;
}

function usageSimilarity(targetCategory, compCategory) {
  if (!targetCategory || !compCategory) {
    return 0.55;
  }
  if (targetCategory === compCategory) {
    return 1;
  }
  if (targetCategory === "unknown" || compCategory === "unknown") {
    return 0.62;
  }
  if (targetCategory === "empty" || compCategory === "empty") {
    return 0.5;
  }
  return 0.65;
}

function sizeDesirabilityScore(sqm) {
  if (!Number.isFinite(sqm) || sqm <= 0) {
    return 0.5;
  }
  return clamp((sqm - 45) / 110, 0, 1);
}

function newnessScore(ageYears) {
  if (!Number.isFinite(ageYears) || ageYears < 0) {
    return 0.5;
  }
  return clamp(1 - ageYears / 30, 0.1, 1);
}

function ageSimilarity(targetAgeYears, compAgeYears) {
  if (!Number.isFinite(targetAgeYears) || !Number.isFinite(compAgeYears)) {
    return 0.55;
  }
  return clamp(1 - Math.abs(targetAgeYears - compAgeYears) / 25, 0.1, 1);
}

function roomSimilarity(target, comp) {
  if (target._normRoomCount && target._normRoomCount === comp._normRoomCount) {
    return 1;
  }
  const t = target._roomTotal;
  const c = comp._roomTotal;
  if (Number.isFinite(t) && Number.isFinite(c)) {
    const diff = Math.abs(t - c);
    if (diff === 0) {
      return 0.85;
    }
    if (diff <= 1) {
      return 0.66;
    }
    if (diff <= 2) {
      return 0.45;
    }
    return 0.25;
  }
  return 0.45;
}

function neighborhoodSimilarity(target, comp) {
  if (target._normNeighborhood && comp._normNeighborhood && target._normNeighborhood === comp._normNeighborhood) {
    return 1;
  }
  if (target._normNeighborhood && comp._normNeighborhood) {
    return 0.45;
  }
  return 0.35;
}

function sqmSimilarity(targetSqm, compSqm) {
  if (!Number.isFinite(targetSqm) || !Number.isFinite(compSqm) || targetSqm <= 0 || compSqm <= 0) {
    return 0;
  }
  const ratio = Math.min(targetSqm, compSqm) / Math.max(targetSqm, compSqm);
  return clamp((ratio - 0.55) / 0.45, 0, 1);
}

function weightedMedian(rows) {
  if (!rows.length) {
    return null;
  }
  const sorted = [...rows].sort((a, b) => a.value - b.value);
  const totalWeight = sorted.reduce((sum, row) => sum + row.weight, 0);
  if (!(totalWeight > 0)) {
    return null;
  }
  let cumulative = 0;
  for (const row of sorted) {
    cumulative += row.weight;
    if (cumulative >= totalWeight / 2) {
      return row.value;
    }
  }
  return sorted[sorted.length - 1].value;
}

function median(values) {
  if (values.length === 0) {
    return null;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }
  return sorted[mid];
}

function medianAbsDeviation(values, center) {
  if (values.length === 0 || center == null) {
    return null;
  }
  const abs = values.map((x) => Math.abs(x - center));
  return median(abs);
}

function computePsmBounds(rows) {
  const psmValues = rows.map((row) => row._pricePerSqm).filter((value) => Number.isFinite(value) && value > 0);
  if (psmValues.length < 16) {
    return { low: 0, high: Number.POSITIVE_INFINITY };
  }
  const center = median(psmValues);
  const mad = medianAbsDeviation(psmValues, center);
  if (!Number.isFinite(center) || center <= 0) {
    return { low: 0, high: Number.POSITIVE_INFINITY };
  }

  const ratioLow = center * 0.35;
  const ratioHigh = center * 2.8;
  if (!Number.isFinite(mad) || mad <= 0) {
    return { low: Math.max(1000, ratioLow), high: ratioHigh };
  }

  const robustLow = center - 4 * mad;
  const robustHigh = center + 4 * mad;
  const low = Math.max(1000, Math.max(ratioLow, robustLow));
  const high = Math.max(low * 1.2, Math.min(ratioHigh, robustHigh));
  return { low, high };
}

function isApartmentLike(row) {
  return row._assetClass === "daire" || row._assetClass === "residence";
}

function isOutlierListing(row, bounds) {
  if (!Number.isFinite(row._pricePerSqm) || !Number.isFinite(row._effectiveSqm) || row._effectiveSqm <= 0) {
    return true;
  }
  if (row._effectiveSqm < 20) {
    return true;
  }
  if (isApartmentLike(row) && row._effectiveSqm < 35) {
    return true;
  }
  if (isApartmentLike(row) && row._effectiveSqm > 350) {
    return true;
  }
  if (Number.isFinite(bounds.low) && row._pricePerSqm < bounds.low) {
    return true;
  }
  if (Number.isFinite(bounds.high) && row._pricePerSqm > bounds.high) {
    return true;
  }
  return false;
}

function dedupeFingerprint(row) {
  if (!row._normNeighborhood || !row._normRoomCount || !Number.isFinite(row._effectiveSqm) || !Number.isFinite(row._pricePerSqm)) {
    return row.listingKey;
  }
  const sqmBucket = Math.round(row._effectiveSqm / 3);
  const ageBucket = Number.isFinite(row._buildingAgeYears) ? Math.round(row._buildingAgeYears / 5) : "na";
  const psmBucket = Math.round(row._pricePerSqm / 2000);
  return [
    row._normNeighborhood,
    row._normRoomCount,
    row._assetClass || "unknown",
    row._floorCategory || "unknown",
    row._deedCategory || "unknown",
    sqmBucket,
    ageBucket,
    psmBucket
  ].join("|");
}

function dedupeListings(rows) {
  const clusters = new Map();
  for (const row of rows) {
    const key = dedupeFingerprint(row);
    if (!clusters.has(key)) {
      clusters.set(key, []);
    }
    clusters.get(key).push(row);
  }

  const deduped = [];
  for (const clusterRows of clusters.values()) {
    if (clusterRows.length === 1) {
      deduped.push({ ...clusterRows[0], _dedupeClusterSize: 1 });
      continue;
    }
    const sorted = [...clusterRows].sort((a, b) => a._pricePerSqm - b._pricePerSqm);
    const chosen = sorted[Math.floor(sorted.length / 2)];
    deduped.push({ ...chosen, _dedupeClusterSize: clusterRows.length });
  }
  return deduped;
}

function buildScoringUniverse(rows) {
  const bounds = computePsmBounds(rows);
  const filtered = rows.filter((row) => !isOutlierListing(row, bounds));
  const deduped = dedupeListings(filtered);
  return {
    rows: deduped,
    bounds,
    filteredOut: rows.length - filtered.length,
    dedupedOut: filtered.length - deduped.length
  };
}

const DAY_MS = 24 * 60 * 60 * 1000;

function makeListingCompositeKey(source, listingKey) {
  return `${String(source || "")}::${String(listingKey || "")}`;
}

function listingCompositeKey(row) {
  return makeListingCompositeKey(row?.source, row?.listingKey);
}

function toEpochMs(value) {
  if (!value) {
    return null;
  }
  const ms = Date.parse(String(value));
  return Number.isFinite(ms) ? ms : null;
}

function computeHistoryStats(entries) {
  const points = entries
    .map((row) => ({
      priceTl: Number(row?.priceTl),
      crawledAt: row?.crawledAt || null,
      ms: toEpochMs(row?.crawledAt)
    }))
    .filter((row) => Number.isFinite(row.priceTl) && row.priceTl > 0 && Number.isFinite(row.ms))
    .sort((a, b) => a.ms - b.ms);

  if (!points.length) {
    return null;
  }

  const prices = points.map((row) => row.priceTl);
  const firstPrice = prices[0];
  const latestPrice = prices[prices.length - 1];
  const lowestPrice = Math.min(...prices);
  const highestPrice = Math.max(...prices);
  const firstMs = points[0].ms;
  const latestMs = points[points.length - 1].ms;
  const daysCovered = Math.max(0, (latestMs - firstMs) / DAY_MS);

  let priceDropCount = 0;
  let lastDropAt = null;
  for (let i = 1; i < points.length; i += 1) {
    const prev = points[i - 1].priceTl;
    const next = points[i].priceTl;
    if (next < prev * 0.995) {
      priceDropCount += 1;
      lastDropAt = points[i].crawledAt;
    }
  }

  function changePctInWindow(days) {
    const cutoffMs = latestMs - days * DAY_MS;
    const baseline = points.find((row) => row.ms >= cutoffMs);
    if (!baseline || !(baseline.priceTl > 0)) {
      return null;
    }
    return (latestPrice - baseline.priceTl) / baseline.priceTl;
  }

  const meanPrice = prices.reduce((sum, n) => sum + n, 0) / prices.length;
  const variance =
    prices.reduce((sum, n) => sum + Math.pow(n - meanPrice, 2), 0) / Math.max(1, prices.length);
  const priceVolatility = meanPrice > 0 ? Math.sqrt(variance) / meanPrice : null;
  const changeFromFirstPct = firstPrice > 0 ? (latestPrice - firstPrice) / firstPrice : null;
  const historyConfidence = clamp(
    0.55 * clamp(points.length / 8, 0, 1) + 0.45 * clamp(daysCovered / 60, 0, 1),
    0,
    1
  );

  return {
    points: points.length,
    firstPrice,
    latestPrice,
    lowestPrice,
    highestPrice,
    firstSeenAt: points[0].crawledAt,
    latestSeenAt: points[points.length - 1].crawledAt,
    daysCovered,
    changeFromFirstPct,
    recent7dChangePct: changePctInWindow(7),
    recent30dChangePct: changePctInWindow(30),
    priceDropCount,
    lastDropAt,
    priceVolatility,
    historyConfidence
  };
}

async function loadHistoryStatsMap(DB, areaCountry, areaCity, areaDistrict, lookbackDays) {
  const map = new Map();
  const days = Math.max(30, Math.min(365, toInt(lookbackDays, 120)));
  const rawWindow = `-${days} days`;
  try {
    const rowsRes = await DB.prepare(
      `
        SELECT
          s.source,
          s.listing_key AS listingKey,
          s.price_tl AS priceTl,
          s.crawled_at AS crawledAt
        FROM listing_snapshots s
        JOIN listings_current lc
          ON lc.source = s.source
         AND lc.listing_key = s.listing_key
        JOIN crawl_runs r
          ON r.id = s.run_id
        WHERE lc.area_country = ?
          AND lc.area_city = ?
          AND lc.area_district = ?
          AND lc.is_active = 1
          AND r.area_country = ?
          AND r.area_city = ?
          AND r.area_district = ?
          AND s.price_tl IS NOT NULL
          AND s.price_tl > 0
          AND datetime(s.crawled_at) >= datetime('now', ?)
        ORDER BY s.source ASC, s.listing_key ASC, datetime(s.crawled_at) ASC
      `
    )
      .bind(areaCountry, areaCity, areaDistrict, areaCountry, areaCity, areaDistrict, rawWindow)
      .all();

    const grouped = new Map();
    for (const row of rowsRes.results || []) {
      const key = makeListingCompositeKey(row.source, row.listingKey);
      if (!grouped.has(key)) {
        grouped.set(key, []);
      }
      grouped.get(key).push({
        priceTl: Number(row.priceTl),
        crawledAt: row.crawledAt
      });
    }

    for (const [key, entries] of grouped.entries()) {
      const stats = computeHistoryStats(entries);
      if (stats) {
        map.set(key, stats);
      }
    }

    return {
      map,
      lookbackDays: days,
      snapshotRows: (rowsRes.results || []).length
    };
  } catch (error) {
    console.log("Failed to load history stats:", error?.message || String(error));
    return {
      map,
      lookbackDays: days,
      snapshotRows: 0
    };
  }
}

async function loadFeedbackSummaryMap(DB, areaCountry, areaCity, areaDistrict) {
  const map = new Map();
  try {
    const rowsRes = await DB.prepare(
      `
        SELECT
          source,
          listing_key AS listingKey,
          SUM(CASE WHEN feedback = 'good' THEN 1 ELSE 0 END) AS goodCount,
          SUM(CASE WHEN feedback = 'bad' THEN 1 ELSE 0 END) AS badCount
        FROM deal_feedback
        WHERE area_country = ?
          AND area_city = ?
          AND area_district = ?
        GROUP BY source, listing_key
      `
    )
      .bind(areaCountry, areaCity, areaDistrict)
      .all();

    let totalVotes = 0;
    for (const row of rowsRes.results || []) {
      const goodCount = Number(row.goodCount || 0);
      const badCount = Number(row.badCount || 0);
      const totalCount = goodCount + badCount;
      totalVotes += totalCount;
      map.set(makeListingCompositeKey(row.source, row.listingKey), {
        goodCount,
        badCount,
        totalCount,
        netScore: goodCount - badCount
      });
    }

    return { map, totalVotes };
  } catch (error) {
    // If the table is not migrated yet, continue without feedback signal.
    console.log("Failed to load feedback summary:", error?.message || String(error));
    return { map, totalVotes: 0 };
  }
}

function safeLog(value) {
  return Math.log(Math.max(1, Number(value) || 0));
}

function mean(values) {
  if (!values.length) {
    return null;
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function hashString(text) {
  const str = String(text || "");
  let hash = 2166136261;
  for (let i = 0; i < str.length; i += 1) {
    hash ^= str.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function buildSmoothedMeanMap(rows, keyFn, globalMean, smoothing = 8) {
  const stats = new Map();
  for (const row of rows) {
    if (!Number.isFinite(row._pricePerSqm) || row._pricePerSqm <= 0) {
      continue;
    }
    const key = keyFn(row);
    if (!key) {
      continue;
    }
    if (!stats.has(key)) {
      stats.set(key, { count: 0, sum: 0 });
    }
    const entry = stats.get(key);
    entry.count += 1;
    entry.sum += safeLog(row._pricePerSqm);
  }

  const out = new Map();
  for (const [key, entry] of stats.entries()) {
    const smoothed =
      (entry.sum + smoothing * globalMean) /
      Math.max(1, entry.count + smoothing);
    out.set(key, smoothed);
  }
  return out;
}

function buildEncoder(rows) {
  const yLogs = rows
    .map((row) => safeLog(row._pricePerSqm))
    .filter((value) => Number.isFinite(value));
  const globalMean = Number.isFinite(mean(yLogs)) ? mean(yLogs) : safeLog(60000);

  return {
    globalMean,
    neighborhoodMean: buildSmoothedMeanMap(rows, (row) => row._normNeighborhood, globalMean),
    roomMean: buildSmoothedMeanMap(rows, (row) => row._normRoomCount, globalMean),
    assetMean: buildSmoothedMeanMap(rows, (row) => row._assetClass, globalMean),
    floorMean: buildSmoothedMeanMap(rows, (row) => row._floorCategory, globalMean),
    deedMean: buildSmoothedMeanMap(rows, (row) => row._deedCategory, globalMean),
    creditMean: buildSmoothedMeanMap(rows, (row) => row._creditCategory, globalMean),
    siteMean: buildSmoothedMeanMap(rows, (row) => row._siteCategory, globalMean),
    usageMean: buildSmoothedMeanMap(rows, (row) => row._usageCategory, globalMean)
  };
}

function encodedDelta(map, key, globalMean) {
  if (!key || !map.has(key)) {
    return 0;
  }
  return map.get(key) - globalMean;
}

function featureVector(row, encoder) {
  const globalMean = encoder?.globalMean ?? safeLog(60000);
  const logSqm = safeLog(row._effectiveSqm);
  const roomTotalNorm = Number.isFinite(row._roomTotal) ? clamp(row._roomTotal / 8, 0, 1.5) : 0.5;
  const ageNorm = Number.isFinite(row._buildingAgeYears) ? clamp(row._buildingAgeYears / 40, 0, 1.5) : 0.55;

  return [
    1,
    logSqm,
    roomTotalNorm,
    ageNorm,
    row._floorScore ?? 0.6,
    row._tapuScore ?? 0.55,
    row._krediScore ?? 0.55,
    row._siteScore ?? 0.58,
    row._usageScore ?? 0.58,
    encodedDelta(encoder?.neighborhoodMean, row._normNeighborhood, globalMean),
    encodedDelta(encoder?.roomMean, row._normRoomCount, globalMean),
    encodedDelta(encoder?.assetMean, row._assetClass, globalMean),
    encodedDelta(encoder?.floorMean, row._floorCategory, globalMean),
    encodedDelta(encoder?.deedMean, row._deedCategory, globalMean),
    encodedDelta(encoder?.creditMean, row._creditCategory, globalMean),
    encodedDelta(encoder?.siteMean, row._siteCategory, globalMean),
    encodedDelta(encoder?.usageMean, row._usageCategory, globalMean)
  ];
}

function solveLinearSystem(matrix, vector) {
  const n = matrix.length;
  const a = matrix.map((row) => row.slice());
  const b = vector.slice();

  for (let i = 0; i < n; i += 1) {
    let pivot = i;
    for (let r = i + 1; r < n; r += 1) {
      if (Math.abs(a[r][i]) > Math.abs(a[pivot][i])) {
        pivot = r;
      }
    }
    if (Math.abs(a[pivot][i]) < 1e-10) {
      return null;
    }
    if (pivot !== i) {
      [a[i], a[pivot]] = [a[pivot], a[i]];
      [b[i], b[pivot]] = [b[pivot], b[i]];
    }

    const diag = a[i][i];
    for (let c = i; c < n; c += 1) {
      a[i][c] /= diag;
    }
    b[i] /= diag;

    for (let r = 0; r < n; r += 1) {
      if (r === i) {
        continue;
      }
      const factor = a[r][i];
      if (Math.abs(factor) < 1e-12) {
        continue;
      }
      for (let c = i; c < n; c += 1) {
        a[r][c] -= factor * a[i][c];
      }
      b[r] -= factor * b[i];
    }
  }
  return b;
}

function trainRidgeModel(rows, opts = {}) {
  const trainRows = rows.filter((row) => Number.isFinite(row._pricePerSqm) && row._pricePerSqm > 0);
  if (trainRows.length < 30) {
    return null;
  }

  const lambda = clamp(toFloat(opts.lambda, 8), 0.01, 1000);
  const encoder = buildEncoder(trainRows);
  const sampleVector = featureVector(trainRows[0], encoder);
  const p = sampleVector.length;
  const xtx = Array.from({ length: p }, () => new Array(p).fill(0));
  const xty = new Array(p).fill(0);

  for (const row of trainRows) {
    const x = featureVector(row, encoder);
    const y = safeLog(row._pricePerSqm);
    for (let i = 0; i < p; i += 1) {
      xty[i] += x[i] * y;
      for (let j = i; j < p; j += 1) {
        xtx[i][j] += x[i] * x[j];
      }
    }
  }

  for (let i = 0; i < p; i += 1) {
    for (let j = 0; j < i; j += 1) {
      xtx[i][j] = xtx[j][i];
    }
    const ridgePenalty = i === 0 ? lambda * 0.12 : lambda;
    xtx[i][i] += ridgePenalty;
  }

  const beta = solveLinearSystem(xtx, xty);
  if (!beta) {
    return null;
  }

  const trainApe = [];
  for (const row of trainRows) {
    const predicted = predictPsm({ encoder, beta }, row);
    const actual = row._pricePerSqm;
    if (Number.isFinite(predicted) && predicted > 0 && Number.isFinite(actual) && actual > 0) {
      trainApe.push(Math.abs(predicted - actual) / actual);
    }
  }

  return {
    encoder,
    beta,
    lambda,
    trainRows: trainRows.length,
    trainMAPE: Number.isFinite(mean(trainApe)) ? mean(trainApe) : null
  };
}

function predictLogPsm(model, row) {
  if (!model?.encoder || !Array.isArray(model?.beta)) {
    return null;
  }
  const x = featureVector(row, model.encoder);
  if (x.length !== model.beta.length) {
    return null;
  }
  let y = 0;
  for (let i = 0; i < x.length; i += 1) {
    y += x[i] * model.beta[i];
  }
  return Number.isFinite(y) ? y : null;
}

function predictPsm(model, row) {
  const y = predictLogPsm(model, row);
  if (!Number.isFinite(y)) {
    return null;
  }
  const psm = Math.exp(y);
  if (!Number.isFinite(psm) || psm <= 0) {
    return null;
  }
  return psm;
}

function computeComparableMedianPsm(comps, minComps) {
  const compAdjustedPsmRows = comps
    .map((x) => ({
      value: x._adjustedPricePerSqm,
      weight: 0.12 + Math.pow(x._similarity, 2) * 1.9
    }))
    .filter((x) => Number.isFinite(x.value) && Number.isFinite(x.weight) && x.weight > 0);
  if (compAdjustedPsmRows.length < minComps) {
    return null;
  }
  return weightedMedian(compAdjustedPsmRows);
}

function blendFairPsm(compMedianPsm, modelPredictedPsm, comparableCount) {
  const hasComp = Number.isFinite(compMedianPsm) && compMedianPsm > 0;
  const hasModel = Number.isFinite(modelPredictedPsm) && modelPredictedPsm > 0;
  if (!hasComp && !hasModel) {
    return { fairPsm: null, compWeight: null };
  }
  if (hasComp && !hasModel) {
    return { fairPsm: compMedianPsm, compWeight: 1 };
  }
  if (!hasComp && hasModel) {
    return { fairPsm: modelPredictedPsm, compWeight: 0 };
  }

  const clampedModel = clamp(modelPredictedPsm, compMedianPsm * 0.65, compMedianPsm * 1.45);
  const compWeight = clamp(0.55 + Math.min(Math.max(0, comparableCount), 25) / 100, 0.55, 0.8);
  const fairPsm = compMedianPsm * compWeight + clampedModel * (1 - compWeight);
  return { fairPsm, compWeight };
}

function portalBaselinePsm(row, trainRows) {
  const rows = trainRows.filter((x) => Number.isFinite(x._pricePerSqm) && x._pricePerSqm > 0);
  if (!rows.length) {
    return null;
  }
  const sameNeighborhoodRoom = rows.filter(
    (x) =>
      row._normNeighborhood &&
      row._normRoomCount &&
      x._normNeighborhood === row._normNeighborhood &&
      x._normRoomCount === row._normRoomCount
  );
  if (sameNeighborhoodRoom.length >= 4) {
    return median(sameNeighborhoodRoom.map((x) => x._pricePerSqm));
  }

  const sameRoom = rows.filter((x) => row._normRoomCount && x._normRoomCount === row._normRoomCount);
  if (sameRoom.length >= 8) {
    return median(sameRoom.map((x) => x._pricePerSqm));
  }

  const sameNeighborhood = rows.filter(
    (x) => row._normNeighborhood && x._normNeighborhood === row._normNeighborhood
  );
  if (sameNeighborhood.length >= 8) {
    return median(sameNeighborhood.map((x) => x._pricePerSqm));
  }
  return median(rows.map((x) => x._pricePerSqm));
}

function meanAbsoluteError(entries) {
  if (!entries.length) {
    return null;
  }
  return mean(entries.map((x) => Math.abs(x.pred - x.actual)));
}

function meanAbsolutePercentageError(entries) {
  if (!entries.length) {
    return null;
  }
  return mean(
    entries
      .filter((x) => Number.isFinite(x.actual) && x.actual > 0)
      .map((x) => Math.abs(x.pred - x.actual) / x.actual)
  );
}

function precisionAtK(records, scoreField, k, truthField = "truthDeal") {
  if (!records.length || k <= 0) {
    return null;
  }
  const sorted = [...records].sort((a, b) => b[scoreField] - a[scoreField]);
  const top = sorted.slice(0, Math.min(k, sorted.length));
  if (!top.length) {
    return null;
  }
  const positives = top.filter((row) => row[truthField]).length;
  return positives / top.length;
}

function meanTruthAtK(records, scoreField, k, truthField = "truthDiscount") {
  if (!records.length || k <= 0) {
    return null;
  }
  const sorted = [...records].sort((a, b) => b[scoreField] - a[scoreField]);
  const top = sorted.slice(0, Math.min(k, sorted.length));
  if (!top.length) {
    return null;
  }
  const values = top.map((row) => row[truthField]).filter((v) => Number.isFinite(v));
  return values.length ? mean(values) : null;
}

function dcgAtK(rows, k, relevanceField = "truthDiscount") {
  const top = rows.slice(0, Math.min(k, rows.length));
  let score = 0;
  for (let i = 0; i < top.length; i += 1) {
    const rel = Math.max(0, Number(top[i][relevanceField]) || 0);
    score += rel / Math.log2(i + 2);
  }
  return score;
}

function ndcgAtK(records, scoreField, k, relevanceField = "truthDiscount") {
  if (!records.length || k <= 0) {
    return null;
  }
  const predicted = [...records].sort((a, b) => b[scoreField] - a[scoreField]);
  const ideal = [...records].sort((a, b) => (b[relevanceField] || 0) - (a[relevanceField] || 0));
  const dcg = dcgAtK(predicted, k, relevanceField);
  const idcg = dcgAtK(ideal, k, relevanceField);
  if (!(idcg > 0)) {
    return null;
  }
  return dcg / idcg;
}

export function buildBacktestReport(preRows, opts = {}) {
  const folds = Math.max(3, Math.min(10, toInt(opts.folds, 5)));
  const minComps = Math.max(3, Math.min(30, toInt(opts.minComps, 6)));
  const strictSameNeighborhood = toBool(opts.strictSameNeighborhood, true);
  const minSameNeighborhoodComps = Math.max(1, Math.min(10, toInt(opts.minSameNeighborhoodComps, 3)));
  const universe = buildScoringUniverse(preRows);
  const rows = universe.rows;

  if (rows.length < 80) {
    return {
      ok: false,
      reason: "Not enough listings for backtest.",
      requiredListings: 80,
      availableListings: rows.length
    };
  }

  const modelErrors = [];
  const portalErrors = [];
  const rankingRows = [];
  let trainedFoldCount = 0;

  for (let fold = 0; fold < folds; fold += 1) {
    const trainRows = [];
    const testRows = [];
    for (const row of rows) {
      const bucket = hashString(row.listingKey || row.url || "") % folds;
      if (bucket === fold) {
        testRows.push(row);
      } else {
        trainRows.push(row);
      }
    }
    if (trainRows.length < 40 || testRows.length < 10) {
      continue;
    }

    const model = trainRidgeModel(trainRows, { lambda: toFloat(opts.modelLambda, 8) });
    if (!model) {
      continue;
    }
    trainedFoldCount += 1;

    for (const row of testRows) {
      const actual = row._pricePerSqm;
      if (!Number.isFinite(actual) || actual <= 0) {
        continue;
      }

      const picked = pickComparables(row, trainRows, {
        minComps,
        strictSameNeighborhood,
        minSameNeighborhoodComps
      });
      const comps = picked.comps || [];
      const sameNeighborhoodCount = picked.sameNeighborhoodComparableCount || 0;
      const comparableMedian = computeComparableMedianPsm(comps, minComps);
      const comparableMedianValid =
        Number.isFinite(comparableMedian) &&
        (!strictSameNeighborhood || !row._normNeighborhood || sameNeighborhoodCount >= minSameNeighborhoodComps);

      const modelPredPsm = predictPsm(model, row);
      const blended = blendFairPsm(comparableMedianValid ? comparableMedian : null, modelPredPsm, comps.length);
      if (Number.isFinite(blended.fairPsm) && blended.fairPsm > 0) {
        modelErrors.push({ pred: blended.fairPsm, actual });
      }

      const portalPredPsm = portalBaselinePsm(row, trainRows);
      if (Number.isFinite(portalPredPsm) && portalPredPsm > 0) {
        portalErrors.push({ pred: portalPredPsm, actual });
      }

      if (comparableMedianValid && comparableMedian > 0) {
        const truthDiscount = (comparableMedian - actual) / comparableMedian;
        if (Number.isFinite(blended.fairPsm) && blended.fairPsm > 0) {
          rankingRows.push({
            truthDeal: truthDiscount >= 0.15,
            truthDiscount,
            modelScore: (blended.fairPsm - actual) / blended.fairPsm,
            portalScore: -actual
          });
        }
      }
    }
  }

  const k10 = 10;
  const k20 = 20;
  const modelPrecisionAt10 = precisionAtK(rankingRows, "modelScore", k10);
  const portalPrecisionAt10 = precisionAtK(rankingRows, "portalScore", k10);
  const modelPrecisionAt20 = precisionAtK(rankingRows, "modelScore", k20);
  const portalPrecisionAt20 = precisionAtK(rankingRows, "portalScore", k20);
  const modelMeanDiscountAt20 = meanTruthAtK(rankingRows, "modelScore", k20);
  const portalMeanDiscountAt20 = meanTruthAtK(rankingRows, "portalScore", k20);
  const modelNdcgAt20 = ndcgAtK(rankingRows, "modelScore", k20);
  const portalNdcgAt20 = ndcgAtK(rankingRows, "portalScore", k20);

  return {
    ok: true,
    folds,
    trainedFoldCount,
    sample: {
      scoringRows: rows.length,
      outliersFiltered: universe.filteredOut,
      dedupedRows: universe.dedupedOut,
      evaluatedPredictions: modelErrors.length
    },
    metrics: {
      model: {
        maePsm: meanAbsoluteError(modelErrors),
        mapePsm: meanAbsolutePercentageError(modelErrors),
        precisionAt10: modelPrecisionAt10,
        precisionAt20: modelPrecisionAt20,
        meanTruthDiscountAt20: modelMeanDiscountAt20,
        ndcgAt20: modelNdcgAt20
      },
      portalBaseline: {
        maePsm: meanAbsoluteError(portalErrors),
        mapePsm: meanAbsolutePercentageError(portalErrors),
        precisionAt10: portalPrecisionAt10,
        precisionAt20: portalPrecisionAt20,
        meanTruthDiscountAt20: portalMeanDiscountAt20,
        ndcgAt20: portalNdcgAt20
      }
    },
    lift: {
      precisionAt10:
        Number.isFinite(modelPrecisionAt10) && Number.isFinite(portalPrecisionAt10) && portalPrecisionAt10 > 0
          ? modelPrecisionAt10 / portalPrecisionAt10
          : null,
      precisionAt20:
        Number.isFinite(modelPrecisionAt20) && Number.isFinite(portalPrecisionAt20) && portalPrecisionAt20 > 0
          ? modelPrecisionAt20 / portalPrecisionAt20
          : null,
      meanTruthDiscountAt20:
        Number.isFinite(modelMeanDiscountAt20) && Number.isFinite(portalMeanDiscountAt20) && portalMeanDiscountAt20 !== 0
          ? modelMeanDiscountAt20 / portalMeanDiscountAt20
          : null,
      ndcgAt20:
        Number.isFinite(modelNdcgAt20) && Number.isFinite(portalNdcgAt20) && portalNdcgAt20 > 0
          ? modelNdcgAt20 / portalNdcgAt20
          : null
    },
    config: {
      minComps,
      strictSameNeighborhood,
      minSameNeighborhoodComps
    }
  };
}

function effectiveSqm(row) {
  if (Number.isFinite(row.netSqm) && row.netSqm > 0) {
    return row.netSqm;
  }
  if (Number.isFinite(row.grossSqm) && row.grossSqm > 0) {
    return row.grossSqm;
  }
  return null;
}

function pricePerSqm(row) {
  const sqm = effectiveSqm(row);
  if (!Number.isFinite(row.priceTl) || row.priceTl <= 0 || sqm == null || sqm <= 0) {
    return null;
  }
  return row.priceTl / sqm;
}

function withPrecomputed(row) {
  const sqm = effectiveSqm(row);
  const psm = pricePerSqm(row);
  const roomStats = parseRoomStats(row.roomCount || "");
  const buildingAgeYears = parseBuildingAgeYears(row.buildingAge || "");
  const floorCategory = parseFloorCategory(row.floorInfo || "");
  const assetClass = parseAssetClass(row.title || "");
  const deedCategory = parseDeedCategory(row.deedStatus || "");
  const creditCategory = parseCreditCategory(row.creditSuitability || "");
  const siteCategory = parseSiteCategory(row.inSite || "");
  const usageCategory = parseUsageCategory(row.usageStatus || "");
  const floorScore = floorDesirabilityScore(floorCategory);
  const sizeScore = sizeDesirabilityScore(sqm);
  const freshnessScore = newnessScore(buildingAgeYears);
  const tapuScore = deedScore(deedCategory);
  const krediScore = creditScore(creditCategory);
  const siteScoreValue = siteScore(siteCategory);
  const usageScoreValue = usageScore(usageCategory);
  const qualityScore = clamp(
    0.2 * sizeScore +
      0.16 * freshnessScore +
      0.12 * floorScore +
      0.2 * tapuScore +
      0.14 * krediScore +
      0.08 * siteScoreValue +
      0.1 * usageScoreValue,
    0,
    1
  );

  return {
    ...row,
    _normNeighborhood: normalizeForMatch(row.neighborhood || ""),
    _normRoomCount: normalizeForMatch(row.roomCount || ""),
    _effectiveSqm: sqm,
    _pricePerSqm: psm,
    _roomTotal: roomStats.total,
    _buildingAgeYears: buildingAgeYears,
    _floorCategory: floorCategory,
    _assetClass: assetClass,
    _deedCategory: deedCategory,
    _creditCategory: creditCategory,
    _siteCategory: siteCategory,
    _usageCategory: usageCategory,
    _floorScore: floorScore,
    _sizeScore: sizeScore,
    _freshnessScore: freshnessScore,
    _tapuScore: tapuScore,
    _krediScore: krediScore,
    _siteScore: siteScoreValue,
    _usageScore: usageScoreValue,
    _qualityScore: qualityScore
  };
}

function pickComparables(target, rows, opts) {
  const minComps = Math.max(3, Number(opts?.minComps) || 6);
  const strictSameNeighborhood = Boolean(opts?.strictSameNeighborhood);
  const minSameNeighborhoodComps = Math.max(1, Number(opts?.minSameNeighborhoodComps) || 3);
  const candidates = rows.filter((x) => x.listingKey !== target.listingKey && Number.isFinite(x._pricePerSqm));
  const scored = [];
  const strictAssetMatch = requiresStrictAssetMatch(target._assetClass);

  for (const comp of candidates) {
    if (strictAssetMatch && comp._assetClass !== target._assetClass) {
      continue;
    }

    if (
      target._assetClass !== "unknown" &&
      comp._assetClass !== "unknown" &&
      target._assetClass !== comp._assetClass &&
      !(isApartmentLikeAsset(target._assetClass) && isApartmentLikeAsset(comp._assetClass))
    ) {
      continue;
    }

    const roomSim = roomSimilarity(target, comp);
    const neighborhoodSim = neighborhoodSimilarity(target, comp);
    const sqmSim = sqmSimilarity(target._effectiveSqm, comp._effectiveSqm);
    const ageSim = ageSimilarity(target._buildingAgeYears, comp._buildingAgeYears);
    const floorSim = floorSimilarity(target._floorCategory, comp._floorCategory);
    const assetSim = assetClassSimilarity(target._assetClass, comp._assetClass);
    const deedSim = deedSimilarity(target._deedCategory, comp._deedCategory);
    const creditSim = creditSimilarity(target._creditCategory, comp._creditCategory);
    const siteSim = siteSimilarity(target._siteCategory, comp._siteCategory);
    const usageSim = usageSimilarity(target._usageCategory, comp._usageCategory);

    if (Number.isFinite(target._effectiveSqm) && Number.isFinite(comp._effectiveSqm) && target._effectiveSqm > 0 && comp._effectiveSqm > 0) {
      const sqmRatio = Math.min(target._effectiveSqm, comp._effectiveSqm) / Math.max(target._effectiveSqm, comp._effectiveSqm);
      if (sqmRatio < 0.55) {
        continue;
      }
    }

    const similarityScore = clamp(
      0.2 * sqmSim +
        0.2 * roomSim +
        0.2 * neighborhoodSim +
        0.1 * ageSim +
        0.07 * floorSim +
        0.07 * assetSim +
        0.07 * deedSim +
        0.04 * creditSim +
        0.03 * siteSim +
        0.02 * usageSim,
      0,
      1
    );

    if (similarityScore < 0.28) {
      continue;
    }

    const qualityGap = target._qualityScore - comp._qualityScore;
    const adjustedPricePerSqm = comp._pricePerSqm * (1 + qualityGap * 0.18);

    scored.push({
      ...comp,
      _similarity: similarityScore,
      _roomSim: roomSim,
      _neighborhoodSim: neighborhoodSim,
      _sqmSim: sqmSim,
      _ageSim: ageSim,
      _floorSim: floorSim,
      _assetSim: assetSim,
      _deedSim: deedSim,
      _creditSim: creditSim,
      _siteSim: siteSim,
      _usageSim: usageSim,
      _adjustedPricePerSqm: adjustedPricePerSqm
    });
  }

  scored.sort((a, b) => b._similarity - a._similarity);
  const sameNeighborhood = scored.filter(
    (row) =>
      target._normNeighborhood &&
      row._normNeighborhood &&
      row._normNeighborhood === target._normNeighborhood
  );
  const minNeighborhoodComps = Math.max(minComps, 4);
  const neighborhoodFocused = sameNeighborhood.length >= minNeighborhoodComps;
  const strictNeighborhoodGate = strictSameNeighborhood && Boolean(target._normNeighborhood);
  const picked = strictNeighborhoodGate
    ? sameNeighborhood.slice(0, Math.min(80, sameNeighborhood.length))
    : neighborhoodFocused
      ? sameNeighborhood.slice(0, Math.min(80, sameNeighborhood.length))
      : scored.slice(0, Math.min(80, scored.length));

  let neighborhoodRoomMatches = 0;
  let roomMatches = 0;
  for (const row of picked) {
    const sameRoom = row._normRoomCount && row._normRoomCount === target._normRoomCount;
    const sameNeighborhood = row._normNeighborhood && row._normNeighborhood === target._normNeighborhood;
    if (sameRoom) {
      roomMatches += 1;
    }
    if (sameRoom && sameNeighborhood) {
      neighborhoodRoomMatches += 1;
    }
  }

  const pickedCount = picked.length || 1;
  const neighborhoodRoomRatio = neighborhoodRoomMatches / pickedCount;
  const roomRatio = roomMatches / pickedCount;

  let bucket = neighborhoodFocused || strictNeighborhoodGate ? "neighborhood+multifactor" : "district+multifactor";
  if (strictNeighborhoodGate && sameNeighborhood.length < minSameNeighborhoodComps) {
    bucket = "district+sqm";
  }
  if (neighborhoodFocused && roomRatio >= 0.5) {
    bucket = "neighborhood+room";
  } else if (!neighborhoodFocused && !strictNeighborhoodGate && neighborhoodRoomRatio >= 0.5) {
    bucket = "neighborhood+room";
  } else if (!neighborhoodFocused && !strictNeighborhoodGate && roomRatio >= 0.5) {
    bucket = "district+room";
  } else if (picked.length < minComps) {
    bucket = "district+sqm";
  }

  return {
    bucket,
    comps: picked,
    neighborhoodFocused,
    sameNeighborhoodComparableCount: sameNeighborhood.length,
    strictNeighborhoodGate
  };
}

function scoreDeal(target, rows, opts) {
  if (!Number.isFinite(target._pricePerSqm)) {
    return null;
  }

  const picked = pickComparables(target, rows, opts);
  const comps = picked.comps || [];
  if (comps.length < opts.minComps) {
    return null;
  }
  if (
    opts.strictSameNeighborhood &&
    target._normNeighborhood &&
    (picked.sameNeighborhoodComparableCount || 0) < opts.minSameNeighborhoodComps
  ) {
    return null;
  }
  const compMedian = computeComparableMedianPsm(comps, opts.minComps);
  if (!Number.isFinite(compMedian) || compMedian <= 0) {
    return null;
  }
  const modelPredictedPsm = opts.model ? predictPsm(opts.model, target) : null;
  const blendedFair = blendFairPsm(compMedian, modelPredictedPsm, comps.length);
  const fairPsm = blendedFair.fairPsm;
  if (!Number.isFinite(fairPsm) || fairPsm <= 0) {
    return null;
  }

  const compPsmForDispersion = comps
    .map((x) => x._adjustedPricePerSqm)
    .filter((x) => Number.isFinite(x) && x > 0);
  const dispersionMad = medianAbsDeviation(compPsmForDispersion, compMedian);
  const dispersionRatio = dispersionMad == null ? 1 : clamp(dispersionMad / compMedian, 0, 1);
  const avgSimilarity =
    comps.reduce((sum, row) => sum + (Number.isFinite(row._similarity) ? row._similarity : 0), 0) / Math.max(1, comps.length);

  let fairPrice = fairPsm * target._effectiveSqm;
  if (Number.isFinite(target.avgPriceForSale) && target.avgPriceForSale > 0) {
    fairPrice = fairPrice * 0.82 + target.avgPriceForSale * 0.18;
  }

  if (!Number.isFinite(fairPrice) || fairPrice <= 0) {
    return null;
  }

  const discountPct = (fairPrice - target.priceTl) / fairPrice;
  const confidence = clamp(
    (Math.min(comps.length, 40) / 40) * clamp(1 - dispersionRatio, 0.18, 1) * clamp(avgSimilarity, 0.3, 1),
    0,
    1
  );

  const historyStats = opts.historyByKey?.get(listingCompositeKey(target)) || null;
  const nowMs = Date.now();
  const firstSeenMs = toEpochMs(target.firstSeenAt || historyStats?.firstSeenAt || target.lastSeenAt);
  const daysOnMarket = Number.isFinite(firstSeenMs) ? Math.max(0, (nowMs - firstSeenMs) / DAY_MS) : null;
  const historyPoints = Number(historyStats?.points || 0);
  const historyDropCount = Number(historyStats?.priceDropCount || 0);
  const historyChangeFromFirstPct = Number.isFinite(historyStats?.changeFromFirstPct)
    ? historyStats.changeFromFirstPct
    : null;
  const historyRecent7dChangePct = Number.isFinite(historyStats?.recent7dChangePct)
    ? historyStats.recent7dChangePct
    : null;
  const historyRecent30dChangePct = Number.isFinite(historyStats?.recent30dChangePct)
    ? historyStats.recent30dChangePct
    : null;
  const historyVolatility = Number.isFinite(historyStats?.priceVolatility)
    ? historyStats.priceVolatility
    : null;
  const historyConfidence = Number.isFinite(historyStats?.historyConfidence)
    ? historyStats.historyConfidence
    : clamp(historyPoints / 6, 0, 1);

  const livabilityQuality = clamp(target._qualityScore, 0, 1);
  const qualityMultiplier = 0.78 + livabilityQuality * 0.44;

  const ageNorm = Number.isFinite(target._buildingAgeYears) ? clamp(target._buildingAgeYears / 30, 0, 1.5) : 0.55;
  const usageRenovationPenalty =
    target._usageCategory === "empty"
      ? 0.08
      : target._usageCategory === "owner"
        ? 0.18
        : target._usageCategory === "tenant"
          ? 0.34
          : 0.24;
  const floorRenovationPenalty =
    target._floorCategory === "middle"
      ? 0.05
      : target._floorCategory === "top"
        ? 0.13
        : target._floorCategory === "ground"
          ? 0.16
          : target._floorCategory === "basement"
            ? 0.28
            : 0.12;
  const renovationUnitCost = 1200 + ageNorm * 2800 + usageRenovationPenalty * 1400 + floorRenovationPenalty * 900;
  const renovationAssetFactor =
    target._assetClass === "ofis" ? 0.65 : target._assetClass === "prefabrik" ? 0.55 : 1;
  const renovationCostTl =
    Number.isFinite(target._effectiveSqm) && target._effectiveSqm > 0
      ? renovationUnitCost * target._effectiveSqm * renovationAssetFactor
      : 0;
  const transactionCostTl = Number.isFinite(target.priceTl) && target.priceTl > 0 ? target.priceTl * 0.055 : 0;
  const expectedNetGainTl = fairPrice - target.priceTl - transactionCostTl - renovationCostTl;
  const netYieldPct =
    Number.isFinite(target.priceTl) && target.priceTl > 0 ? expectedNetGainTl / target.priceTl : null;
  if (!Number.isFinite(netYieldPct) || expectedNetGainTl <= 0) {
    return null;
  }

  const valuationRisk = clamp(
    (1 - confidence) * 0.65 + dispersionRatio * 0.35 + (1 - clamp(avgSimilarity, 0, 1)) * 0.3,
    0,
    1
  );
  const legalRisk = clamp((1 - target._tapuScore) * 0.65 + (1 - target._krediScore) * 0.35, 0, 1);
  const liquidityRisk = clamp(
    (1 - target._usageScore) * 0.45 + (1 - target._siteScore) * 0.25 + (1 - target._floorScore) * 0.3,
    0,
    1
  );
  const staleRisk = Number.isFinite(daysOnMarket) ? clamp((daysOnMarket - 40) / 140, 0, 1) : 0.25;
  const noDropRisk = Number.isFinite(daysOnMarket) && daysOnMarket > 45 && historyDropCount === 0 ? 0.28 : 0;
  const recentIncreaseRisk =
    Number.isFinite(historyRecent30dChangePct) && historyRecent30dChangePct > 0
      ? clamp(historyRecent30dChangePct * 2, 0, 0.35)
      : 0;
  const volatilityRisk = Number.isFinite(historyVolatility) ? clamp(historyVolatility / 0.22, 0, 1) * 0.25 : 0.1;
  const historyCoverageRisk = historyPoints >= 5 ? 0.08 : historyPoints >= 2 ? 0.2 : 0.35;
  const historyRisk = clamp(
    historyCoverageRisk + staleRisk * 0.35 + noDropRisk + recentIncreaseRisk + volatilityRisk,
    0,
    1
  );

  const totalRiskScore = clamp(
    0.36 * valuationRisk + 0.24 * legalRisk + 0.17 * liquidityRisk + 0.23 * historyRisk,
    0,
    1
  );
  const riskPenaltyMultiplier = 1 + totalRiskScore * 1.35;
  const riskAdjustedYieldPct = netYieldPct / riskPenaltyMultiplier;
  let score = riskAdjustedYieldPct * (0.72 + confidence * 0.56) * qualityMultiplier;

  const feedbackStats = opts.feedbackByKey?.get(listingCompositeKey(target)) || {
    goodCount: 0,
    badCount: 0,
    totalCount: 0,
    netScore: 0
  };
  const crowdTilt =
    feedbackStats.totalCount >= 3 ? clamp(feedbackStats.netScore / feedbackStats.totalCount, -1, 1) : 0;
  score *= 1 + crowdTilt * 0.12;

  const pricePerSqmGapPct = (fairPsm - target._pricePerSqm) / fairPsm;

  let endeksaMidPrice = null;
  if (Number.isFinite(target.endeksaMinPrice) && Number.isFinite(target.endeksaMaxPrice)) {
    endeksaMidPrice = (target.endeksaMinPrice + target.endeksaMaxPrice) / 2;
  } else if (Number.isFinite(target.avgPriceForSale)) {
    endeksaMidPrice = target.avgPriceForSale;
  }
  const discountVsEndeksa = endeksaMidPrice && endeksaMidPrice > 0 ? (endeksaMidPrice - target.priceTl) / endeksaMidPrice : null;
  if (Number.isFinite(discountVsEndeksa) && discountVsEndeksa > 0) {
    score *= 1 + clamp(discountVsEndeksa, 0, 0.2) * 0.35;
  }
  const sameNeighborhoodComparables = comps.filter(
    (row) =>
      target._normNeighborhood &&
      row._normNeighborhood &&
      row._normNeighborhood === target._normNeighborhood
  );
  const fallbackComparables = comps.filter(
    (row) =>
      !(
        target._normNeighborhood &&
        row._normNeighborhood &&
        row._normNeighborhood === target._normNeighborhood
      )
  );
  const topComparableRows = [...sameNeighborhoodComparables, ...fallbackComparables].slice(0, 3);
  const outsideNeighborhoodComparableCount = Math.max(0, comps.length - sameNeighborhoodComparables.length);
  const topComparables = topComparableRows.map((row) => ({
    source: row.source,
    listingKey: row.listingKey,
    listingId: row.listingId,
    url: row.url,
    title: row.title,
    neighborhood: row.neighborhood,
    roomCount: row.roomCount,
    buildingAge: row.buildingAge,
    floorInfo: row.floorInfo,
    deedStatus: row.deedStatus || null,
    creditSuitability: row.creditSuitability || null,
    inSite: row.inSite || null,
    usageStatus: row.usageStatus || null,
    priceTl: row.priceTl,
    effectiveSqm: row._effectiveSqm,
    pricePerSqm: row._pricePerSqm,
    similarity: row._similarity,
    adjustedPricePerSqm: row._adjustedPricePerSqm,
    sameNeighborhood:
      Boolean(target._normNeighborhood) &&
      Boolean(row._normNeighborhood) &&
      row._normNeighborhood === target._normNeighborhood
  }));

  return {
    source: target.source,
    listingKey: target.listingKey,
    listingId: target.listingId,
    url: target.url,
    title: target.title,
    address: target.address,
    neighborhood: target.neighborhood,
    roomCount: target.roomCount,
    buildingAge: target.buildingAge,
    floorInfo: target.floorInfo,
    deedStatus: target.deedStatus,
    creditSuitability: target.creditSuitability,
    inSite: target.inSite,
    usageStatus: target.usageStatus,
    priceTl: target.priceTl,
    effectiveSqm: target._effectiveSqm,
    pricePerSqm: target._pricePerSqm,
    fairPriceEstimate: fairPrice,
    fairPricePerSqm: fairPsm,
    discountPct,
    discountAmountTl: fairPrice - target.priceTl,
    transactionCostEstimateTl: transactionCostTl,
    renovationCostEstimateTl: renovationCostTl,
    expectedNetGainTl,
    netYieldPct,
    riskAdjustedYieldPct,
    riskScore: totalRiskScore,
    daysOnMarket,
    historyPriceDropCount: historyDropCount,
    confidence,
    score,
    comparableCount: comps.length,
    comparableBucket: picked.bucket,
    dispersionRatio,
    averageComparableSimilarity: avgSimilarity,
    qualityMultiplier,
    avgPriceForSale: target.avgPriceForSale || null,
    endeksaMinPrice: target.endeksaMinPrice || null,
    endeksaMaxPrice: target.endeksaMaxPrice || null,
    discountVsEndeksa,
    feedback: {
      goodCount: feedbackStats.goodCount,
      badCount: feedbackStats.badCount,
      totalCount: feedbackStats.totalCount,
      netScore: feedbackStats.netScore
    },
    topComparables,
    lastSeenAt: target.lastSeenAt,
    reasoning: {
      method: picked.bucket,
      methodLabel: bucketLabel(picked.bucket),
      comparableCount: comps.length,
      minSameNeighborhoodComps: opts.minSameNeighborhoodComps,
      strictSameNeighborhoodApplied: Boolean(opts.strictSameNeighborhood),
      strictNeighborhoodGate: Boolean(picked.strictNeighborhoodGate),
      neighborhoodFocused: Boolean(picked.neighborhoodFocused),
      sameNeighborhoodComparableCount: picked.sameNeighborhoodComparableCount || 0,
      outsideNeighborhoodComparableCount,
      effectiveSqm: target._effectiveSqm,
      usedSqmType: Number.isFinite(target.netSqm) && target.netSqm > 0 ? "net" : "gross",
      roomCount: target.roomCount || null,
      neighborhood: target.neighborhood || null,
      buildingAgeRaw: target.buildingAge || null,
      buildingAgeYears: target._buildingAgeYears,
      floorRaw: target.floorInfo || null,
      floorCategory: target._floorCategory,
      deedStatusRaw: target.deedStatus || null,
      deedCategory: target._deedCategory,
      creditSuitabilityRaw: target.creditSuitability || null,
      creditCategory: target._creditCategory,
      inSiteRaw: target.inSite || null,
      siteCategory: target._siteCategory,
      usageStatusRaw: target.usageStatus || null,
      usageCategory: target._usageCategory,
      sizeScore: target._sizeScore,
      freshnessScore: target._freshnessScore,
      floorScore: target._floorScore,
      tapuScore: target._tapuScore,
      krediScore: target._krediScore,
      siteScore: target._siteScore,
      usageScore: target._usageScore,
      livabilityQuality,
      listingPricePerSqm: target._pricePerSqm,
      modelPredictedPricePerSqm: modelPredictedPsm,
      fairPricePerSqm: fairPsm,
      fairPriceCompWeight: blendedFair.compWeight,
      comparableMedianPricePerSqm: compMedian,
      pricePerSqmGapPct,
      fairPriceEstimate: fairPrice,
      discountAmountTl: fairPrice - target.priceTl,
      discountPct,
      transactionCostTl,
      renovationCostTl,
      expectedNetGainTl,
      netYieldPct,
      riskAdjustedYieldPct,
      valuationRisk,
      legalRisk,
      liquidityRisk,
      historyRisk,
      totalRiskScore,
      daysOnMarket,
      historyPoints,
      historyDropCount,
      historyChangeFromFirstPct,
      historyRecent7dChangePct,
      historyRecent30dChangePct,
      historyVolatility,
      historyConfidence,
      confidence,
      dispersionRatio,
      averageComparableSimilarity: avgSimilarity,
      qualityMultiplier,
      blendedWithAvgPriceForSale: Number.isFinite(target.avgPriceForSale) && target.avgPriceForSale > 0,
      avgPriceForSale: target.avgPriceForSale || null,
      endeksaMidPrice,
      discountVsEndeksa,
      crowdFeedbackGood: feedbackStats.goodCount,
      crowdFeedbackBad: feedbackStats.badCount,
      crowdFeedbackNet: feedbackStats.netScore,
      crowdFeedbackTotal: feedbackStats.totalCount,
      crowdTilt,
      topComparables
    }
  };
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
  const limit = Math.max(1, Math.min(100, toInt(url.searchParams.get("limit"), 25)));
  const minDiscount = clamp(toFloat(url.searchParams.get("min_discount"), 0.12), -0.5, 0.9);
  const minConfidence = clamp(toFloat(url.searchParams.get("min_confidence"), 0.35), 0, 1);
  const minComps = Math.max(3, Math.min(50, toInt(url.searchParams.get("min_comps"), 6)));
  const strictSameNeighborhood = toBool(url.searchParams.get("strict_same_neighborhood"), true);
  const minSameNeighborhoodComps = Math.max(1, Math.min(20, toInt(url.searchParams.get("min_same_neighborhood_comps"), 3)));
  const useModel = toBool(url.searchParams.get("use_model"), true);
  const modelLambda = clamp(toFloat(url.searchParams.get("model_lambda"), 8), 0.01, 1000);
  const historyLookbackDays = Math.max(30, Math.min(365, toInt(url.searchParams.get("history_lookback_days"), 120)));

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
        first_seen_at AS firstSeenAt,
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
  const pre = rows.map(withPrecomputed);
  const valid = pre.filter((x) => Number.isFinite(x._pricePerSqm));
  const universe = buildScoringUniverse(valid);
  const scoringRows = universe.rows;
  const model = useModel ? trainRidgeModel(scoringRows, { lambda: modelLambda }) : null;
  const historyInfo = await loadHistoryStatsMap(DB, areaCountry, areaCity, areaDistrict, historyLookbackDays);
  const feedbackInfo = await loadFeedbackSummaryMap(DB, areaCountry, areaCity, areaDistrict);

  const scored = [];
  for (const listing of scoringRows) {
    const deal = scoreDeal(listing, scoringRows, {
      minComps,
      strictSameNeighborhood,
      minSameNeighborhoodComps,
      model,
      historyByKey: historyInfo.map,
      feedbackByKey: feedbackInfo.map
    });
    if (!deal) {
      continue;
    }
    if (deal.discountPct < minDiscount) {
      continue;
    }
    if (deal.confidence < minConfidence) {
      continue;
    }
    scored.push(deal);
  }

  scored.sort((a, b) => b.score - a.score);

  return json({
    ok: true,
    area: { country: areaCountry, city: areaCity, district: areaDistrict },
    model: {
      version: "v4-hybrid-ml",
      factors: [
        "m2 benzerliği",
        "oda benzerliği",
        "mahalle benzerliği",
        "makine öğrenmesi fiyat tahmini",
        "bina yaşı",
        "kat tercihi",
        "tapu durumu",
        "krediye uygunluk",
        "site içerisinde",
        "kullanım durumu",
        "mahalle sıkılığı",
        "outlier temizliği",
        "dedup kümesi",
        "emsal benzerlik güveni",
        "Endeksa karşılaştırması",
        "ilan geçmişi (fiyat düşüşü/tempo)",
        "masraf tahmini (işlem + yenileme)",
        "risk dengeli net getiri",
        "saha geri bildirimi"
      ]
    },
    params: {
      limit,
      minDiscount,
      minConfidence,
      minComps,
      strictSameNeighborhood,
      minSameNeighborhoodComps,
      useModel,
      modelLambda,
      historyLookbackDays
    },
    totals: {
      activeListings: rows.length,
      listingsWithPriceAndSqm: valid.length,
      listingsAfterOutlierFilter: valid.length - universe.filteredOut,
      listingsAfterDedup: scoringRows.length,
      filteredOutliers: universe.filteredOut,
      dedupedListings: universe.dedupedOut,
      historySnapshotRows: historyInfo.snapshotRows,
      listingsWithHistory: historyInfo.map.size,
      feedbackVotes: feedbackInfo.totalVotes,
      modelTrainingRows: model?.trainRows ?? 0,
      modelTrainMape: model?.trainMAPE ?? null,
      dealsMatched: scored.length
    },
    deals: scored.slice(0, limit)
  });
}

export {
  withPrecomputed,
  canonicalCountryCode,
  canonicalAreaName,
  toInt,
  toBool
};
