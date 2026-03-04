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
  return {
    ...row,
    _normNeighborhood: normalizeForMatch(row.neighborhood || ""),
    _normRoomCount: normalizeForMatch(row.roomCount || ""),
    _effectiveSqm: sqm,
    _pricePerSqm: psm
  };
}

function sqmComparable(a, b, minRatio, maxRatio) {
  if (!Number.isFinite(a) || !Number.isFinite(b) || a <= 0 || b <= 0) {
    return false;
  }
  const ratio = a >= b ? a / b : b / a;
  return ratio <= maxRatio && ratio >= minRatio;
}

function pickComparables(target, rows, minComps) {
  const candidates = rows.filter((x) => x.listingKey !== target.listingKey && Number.isFinite(x._pricePerSqm));
  const sameNeighborhoodRoom = candidates.filter(
    (x) =>
      x._normNeighborhood &&
      x._normNeighborhood === target._normNeighborhood &&
      x._normRoomCount &&
      x._normRoomCount === target._normRoomCount &&
      sqmComparable(target._effectiveSqm, x._effectiveSqm, 1, 1.35)
  );
  if (sameNeighborhoodRoom.length >= minComps) {
    return { bucket: "neighborhood+room", comps: sameNeighborhoodRoom };
  }

  const sameDistrictRoom = candidates.filter(
    (x) =>
      x._normRoomCount &&
      x._normRoomCount === target._normRoomCount &&
      sqmComparable(target._effectiveSqm, x._effectiveSqm, 1, 1.4)
  );
  if (sameDistrictRoom.length >= minComps) {
    return { bucket: "district+room", comps: sameDistrictRoom };
  }

  const sameDistrictAnyRoom = candidates.filter((x) => sqmComparable(target._effectiveSqm, x._effectiveSqm, 1, 1.45));
  return { bucket: "district+sqm", comps: sameDistrictAnyRoom };
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

  const compPsm = comps.map((x) => x._pricePerSqm).filter((x) => Number.isFinite(x));
  if (compPsm.length < opts.minComps) {
    return null;
  }

  const compMedian = median(compPsm);
  if (!Number.isFinite(compMedian) || compMedian <= 0) {
    return null;
  }

  const dispersionMad = medianAbsDeviation(compPsm, compMedian);
  const dispersionRatio = dispersionMad == null ? 1 : clamp(dispersionMad / compMedian, 0, 1);

  let fairPrice = compMedian * target._effectiveSqm;
  if (Number.isFinite(target.avgPriceForSale) && target.avgPriceForSale > 0) {
    fairPrice = fairPrice * 0.7 + target.avgPriceForSale * 0.3;
  }

  if (!Number.isFinite(fairPrice) || fairPrice <= 0) {
    return null;
  }

  const discountPct = (fairPrice - target.priceTl) / fairPrice;
  const confidence = clamp((comps.length / 20) * clamp(1 - dispersionRatio, 0.15, 1), 0, 1);
  const score = discountPct * confidence;
  const pricePerSqmGapPct = (compMedian - target._pricePerSqm) / compMedian;

  let endeksaMidPrice = null;
  if (Number.isFinite(target.endeksaMinPrice) && Number.isFinite(target.endeksaMaxPrice)) {
    endeksaMidPrice = (target.endeksaMinPrice + target.endeksaMaxPrice) / 2;
  } else if (Number.isFinite(target.avgPriceForSale)) {
    endeksaMidPrice = target.avgPriceForSale;
  }
  const discountVsEndeksa = endeksaMidPrice && endeksaMidPrice > 0 ? (endeksaMidPrice - target.priceTl) / endeksaMidPrice : null;

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
      listingPricePerSqm: target._pricePerSqm,
      comparableMedianPricePerSqm: compMedian,
      pricePerSqmGapPct,
      fairPriceEstimate: fairPrice,
      discountAmountTl: fairPrice - target.priceTl,
      discountPct,
      confidence,
      dispersionRatio,
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
    params: { limit, minDiscount, minConfidence, minComps },
    totals: {
      activeListings: rows.length,
      listingsWithPriceAndSqm: valid.length,
      dealsMatched: scored.length
    },
    deals: scored.slice(0, limit)
  });
}
