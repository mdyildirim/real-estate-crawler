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

function toFloat(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return fallback;
  }
  return n;
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function bucketLabel(bucket) {
  if (bucket === "neighborhood+room") {
    return "Mahalle + oda sayısı";
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

function normalizeForMatch(text) {
  return String(text || "")
    .trim()
    .toLocaleLowerCase("tr-TR")
    .replace(/ç/g, "c")
    .replace(/ğ/g, "g")
    .replace(/ı/g, "i")
    .replace(/ö/g, "o")
    .replace(/ş/g, "s")
    .replace(/ü/g, "u")
    .replace(/\s+/g, " ");
}

function parseRoomStats(roomText) {
  const normalized = normalizeForMatch(roomText || "");
  if (!normalized) {
    return { bedrooms: null, livingRooms: null, total: null };
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
  if (normalized.includes("cati") || normalized.includes("teras") || normalized.includes("en ust") || normalized.includes("son kat")) {
    return "top";
  }
  if (normalized.includes("ara kat")) {
    return "middle";
  }
  const numericFloor = normalized.match(/([0-9]{1,2})\s*\.?\s*kat|([0-9]{1,2})kat/);
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

function pickComparables(target, rows, minComps) {
  const candidates = rows.filter((x) => x.listingKey !== target.listingKey && Number.isFinite(x._pricePerSqm));
  const scored = [];

  for (const comp of candidates) {
    const roomSim = roomSimilarity(target, comp);
    const neighborhoodSim = neighborhoodSimilarity(target, comp);
    const sqmSim = sqmSimilarity(target._effectiveSqm, comp._effectiveSqm);
    const ageSim = ageSimilarity(target._buildingAgeYears, comp._buildingAgeYears);
    const floorSim = floorSimilarity(target._floorCategory, comp._floorCategory);
    const deedSim = deedSimilarity(target._deedCategory, comp._deedCategory);
    const creditSim = creditSimilarity(target._creditCategory, comp._creditCategory);
    const siteSim = siteSimilarity(target._siteCategory, comp._siteCategory);
    const usageSim = usageSimilarity(target._usageCategory, comp._usageCategory);
    const similarityScore = clamp(
      0.26 * sqmSim +
        0.2 * roomSim +
        0.15 * neighborhoodSim +
        0.11 * ageSim +
        0.08 * floorSim +
        0.08 * deedSim +
        0.05 * creditSim +
        0.04 * siteSim +
        0.03 * usageSim,
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
      _deedSim: deedSim,
      _creditSim: creditSim,
      _siteSim: siteSim,
      _usageSim: usageSim,
      _adjustedPricePerSqm: adjustedPricePerSqm
    });
  }

  scored.sort((a, b) => b._similarity - a._similarity);
  const picked = scored.slice(0, Math.min(80, scored.length));

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

  let bucket = "district+multifactor";
  if (neighborhoodRoomRatio >= 0.5) {
    bucket = "neighborhood+room";
  } else if (roomRatio >= 0.5) {
    bucket = "district+room";
  } else if (picked.length < minComps) {
    bucket = "district+sqm";
  }

  return { bucket, comps: picked };
}

function scoreDeal(target, rows, opts) {
  if (!Number.isFinite(target._pricePerSqm)) {
    return null;
  }

  const picked = pickComparables(target, rows, opts.minComps);
  const comps = picked.comps || [];
  if (comps.length < opts.minComps) {
    return null;
  }

  const compAdjustedPsmRows = comps
    .map((x) => ({
      value: x._adjustedPricePerSqm,
      weight: 0.12 + Math.pow(x._similarity, 2) * 1.9
    }))
    .filter((x) => Number.isFinite(x.value) && Number.isFinite(x.weight) && x.weight > 0);
  if (compAdjustedPsmRows.length < opts.minComps) {
    return null;
  }

  const compMedian = weightedMedian(compAdjustedPsmRows);
  if (!Number.isFinite(compMedian) || compMedian <= 0) {
    return null;
  }

  const compPsmForDispersion = compAdjustedPsmRows.map((x) => x.value);
  const dispersionMad = medianAbsDeviation(compPsmForDispersion, compMedian);
  const dispersionRatio = dispersionMad == null ? 1 : clamp(dispersionMad / compMedian, 0, 1);
  const avgSimilarity =
    comps.reduce((sum, row) => sum + (Number.isFinite(row._similarity) ? row._similarity : 0), 0) / Math.max(1, comps.length);

  let fairPrice = compMedian * target._effectiveSqm;
  if (Number.isFinite(target.avgPriceForSale) && target.avgPriceForSale > 0) {
    fairPrice = fairPrice * 0.7 + target.avgPriceForSale * 0.3;
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
  const livabilityQuality = clamp(target._qualityScore, 0, 1);
  const qualityMultiplier = 0.78 + livabilityQuality * 0.44;
  let score = discountPct * confidence * qualityMultiplier;
  const pricePerSqmGapPct = (compMedian - target._pricePerSqm) / compMedian;

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
    fairPricePerSqm: compMedian,
    discountPct,
    discountAmountTl: fairPrice - target.priceTl,
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
    lastSeenAt: target.lastSeenAt,
    reasoning: {
      method: picked.bucket,
      methodLabel: bucketLabel(picked.bucket),
      comparableCount: comps.length,
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
      comparableMedianPricePerSqm: compMedian,
      pricePerSqmGapPct,
      fairPriceEstimate: fairPrice,
      discountAmountTl: fairPrice - target.priceTl,
      discountPct,
      confidence,
      dispersionRatio,
      averageComparableSimilarity: avgSimilarity,
      qualityMultiplier,
      blendedWithAvgPriceForSale: Number.isFinite(target.avgPriceForSale) && target.avgPriceForSale > 0,
      avgPriceForSale: target.avgPriceForSale || null,
      endeksaMidPrice,
      discountVsEndeksa
    }
  };
}

export async function onRequestGet(context) {
  const DB = context.env?.DB;
  if (!DB) {
    return json({ ok: false, error: "D1 binding `DB` is missing." }, 500);
  }

  const url = new URL(context.request.url);
  const areaCity = canonicalAreaName(url.searchParams.get("city"), "Istanbul");
  const areaDistrict = canonicalAreaName(url.searchParams.get("district"), "Atasehir");
  const limit = Math.max(1, Math.min(100, toInt(url.searchParams.get("limit"), 25)));
  const minDiscount = clamp(toFloat(url.searchParams.get("min_discount"), 0.12), -0.5, 0.9);
  const minConfidence = clamp(toFloat(url.searchParams.get("min_confidence"), 0.35), 0, 1);
  const minComps = Math.max(3, Math.min(50, toInt(url.searchParams.get("min_comps"), 6)));

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
      WHERE area_city = ?
        AND area_district = ?
        AND is_active = 1
    `
  )
    .bind(areaCity, areaDistrict)
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

  const scored = [];
  for (const listing of valid) {
    const deal = scoreDeal(listing, valid, { minComps });
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
    area: { city: areaCity, district: areaDistrict },
    model: {
      version: "v3-legal-usage-signals",
      factors: [
        "m2 benzerliği",
        "oda benzerliği",
        "mahalle benzerliği",
        "bina yaşı",
        "kat tercihi",
        "tapu durumu",
        "krediye uygunluk",
        "site içerisinde",
        "kullanım durumu",
        "emsal benzerlik güveni",
        "Endeksa karşılaştırması"
      ]
    },
    params: { limit, minDiscount, minConfidence, minComps },
    totals: {
      activeListings: rows.length,
      listingsWithPriceAndSqm: valid.length,
      dealsMatched: scored.length
    },
    deals: scored.slice(0, limit)
  });
}
