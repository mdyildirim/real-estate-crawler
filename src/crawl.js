#!/usr/bin/env node
"use strict";

const fs = require("fs/promises");
const path = require("path");
const { execFile } = require("child_process");
const { promisify } = require("util");
const { persistRunToSqlite } = require("./sqlite-store");

const DEFAULT_TIMEOUT_MS = 30000;
const DEFAULT_CONCURRENCY = 4;
const DEFAULT_OUTPUT_DIR = "output";
const DEFAULT_SQLITE_PATH = "data/real-estate.sqlite";
const execFileAsync = promisify(execFile);
const BROWSER_UA =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

const SOURCE_DEFS = {
  sahibinden: {
    source: "sahibinden",
    urlForArea: (area) =>
      `https://www.sahibinden.com/satilik-daire/${toTurkishSlug(area.city)}-${toTurkishSlug(area.district)}`,
    crawl: crawlCloudflareProneSource
  },
  hepsiemlak: {
    source: "hepsiemlak",
    urlForArea: (area) => `https://www.hepsiemlak.com/${toTurkishSlug(area.city)}-${toTurkishSlug(area.district)}-satilik`,
    crawl: crawlCloudflareProneSource
  },
  emlakjet: {
    source: "emlakjet",
    urlForArea: (area) =>
      `https://www.emlakjet.com/satilik-konut/${toTurkishSlug(area.city)}-${toTurkishSlug(area.district)}`,
    crawl: crawlEmlakjet
  },
  atasehirsatilik: {
    source: "atasehirsatilik",
    urlForArea: (area) =>
      isAtasehirArea(area) ? "https://atasehirsatilik.com/sitemap.php" : null,
    crawl: crawlAtasehirSatilik
  },
  turyap_251316: {
    source: "turyap_251316",
    urlForArea: (area) =>
      isAtasehirArea(area) ? "https://www.turyap.com.tr/Portfoyler.aspx?SirketID=251316" : null,
    crawl: crawlTuryapOffice251316
  }
};

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const startedAt = new Date().toISOString();
  const runTag = formatRunTag(new Date());
  const selectedSourceKeys = resolveSources(opts.sources);

  const results = [];
  for (const key of selectedSourceKeys) {
    const baseDef = SOURCE_DEFS[key];
    const resolvedUrl = typeof baseDef.urlForArea === "function" ? baseDef.urlForArea(opts.area) : baseDef.url;
    if (!resolvedUrl) {
      results.push({
        source: baseDef.source,
        url: "",
        status: "skipped",
        blocked: false,
        observedTotal: null,
        fetchedAt: new Date().toISOString(),
        notes: [`Source is not configured for ${opts.area.city}/${opts.area.district}.`],
        listings: []
      });
      continue;
    }
    const def = { ...baseDef, url: resolvedUrl };
    try {
      const result = await def.crawl({
        ...opts,
        def
      });
      results.push(result);
    } catch (error) {
      results.push({
        source: def.source,
        url: def.url,
        status: "error",
        blocked: false,
        observedTotal: null,
        fetchedAt: new Date().toISOString(),
        notes: [normalizeError(error)],
        listings: []
      });
    }
  }

  const allListings = results.flatMap((r) => r.listings || []);
  const uniqueGlobal = uniqueBy(allListings, (x) => `${x.source}:${x.listingKey || x.listingId || x.url}`);

  const summary = {
    runTag,
    startedAt,
    finishedAt: new Date().toISOString(),
    area: {
      city: opts.area.city,
      district: opts.area.district
    },
    sourceSummaries: results.map((r) => ({
      source: r.source,
      status: r.status,
      blocked: Boolean(r.blocked),
      observedTotal: r.observedTotal ?? null,
      crawledListings: (r.listings || []).length,
      url: r.url
    })),
    totals: {
      crawledRaw: allListings.length,
      crawledUnique: uniqueGlobal.length
    }
  };

  const payload = {
    summary,
    results,
    listings: uniqueGlobal
  };

  if (opts.writeOutput) {
    const runDir = path.join(opts.outputDir, `run-${runTag}`);
    await fs.mkdir(runDir, { recursive: true });
    await fs.writeFile(path.join(runDir, "results.json"), JSON.stringify(payload, null, 2), "utf8");
    await fs.writeFile(path.join(runDir, "summary.json"), JSON.stringify(summary, null, 2), "utf8");
    await fs.writeFile(path.join(runDir, "listings.csv"), toCsv(uniqueGlobal), "utf8");

    await fs.mkdir(opts.outputDir, { recursive: true });
    await fs.writeFile(path.join(opts.outputDir, "latest-summary.json"), JSON.stringify(summary, null, 2), "utf8");
    await fs.writeFile(path.join(opts.outputDir, "latest-results.json"), JSON.stringify(payload, null, 2), "utf8");
    await fs.writeFile(path.join(opts.outputDir, "latest-listings.csv"), toCsv(uniqueGlobal), "utf8");

    summary.outputRunDir = runDir;
  }

  if (opts.writeSqlite) {
    const sqliteResult = persistRunToSqlite({
      dbPath: opts.sqlitePath,
      runTag,
      payload
    });
    summary.sqlite = {
      dbPath: opts.sqlitePath,
      runId: sqliteResult.runId,
      storedListings: sqliteResult.storedListings
    };

    if (opts.writeOutput) {
      await fs.writeFile(path.join(opts.outputDir, "latest-summary.json"), JSON.stringify(summary, null, 2), "utf8");
    }
  }

  printHumanSummary(summary);
}

function parseArgs(argv) {
  const opts = {
    area: {
      city: "Istanbul",
      district: "Atasehir"
    },
    sources: "all",
    timeoutMs: DEFAULT_TIMEOUT_MS,
    concurrency: DEFAULT_CONCURRENCY,
    emlakjetDetailMax: 350,
    outputDir: DEFAULT_OUTPUT_DIR,
    writeOutput: true,
    writeSqlite: true,
    sqlitePath: DEFAULT_SQLITE_PATH
  };

  for (const arg of argv) {
    if (arg === "--no-write") {
      opts.writeOutput = false;
    } else if (arg === "--no-sqlite") {
      opts.writeSqlite = false;
    } else if (arg.startsWith("--sources=")) {
      opts.sources = arg.slice("--sources=".length);
    } else if (arg.startsWith("--city=")) {
      opts.area.city = normalizeAreaName(arg.slice("--city=".length) || "Istanbul");
    } else if (arg.startsWith("--district=")) {
      opts.area.district = normalizeAreaName(arg.slice("--district=".length) || "Atasehir");
    } else if (arg.startsWith("--timeout-ms=")) {
      opts.timeoutMs = toPositiveInt(arg.slice("--timeout-ms=".length), DEFAULT_TIMEOUT_MS);
    } else if (arg.startsWith("--concurrency=")) {
      opts.concurrency = toPositiveInt(arg.slice("--concurrency=".length), DEFAULT_CONCURRENCY);
    } else if (arg.startsWith("--emlakjet-detail-max=")) {
      opts.emlakjetDetailMax = toPositiveInt(arg.slice("--emlakjet-detail-max=".length), 350);
    } else if (arg.startsWith("--output-dir=")) {
      opts.outputDir = arg.slice("--output-dir=".length) || DEFAULT_OUTPUT_DIR;
    } else if (arg.startsWith("--sqlite-path=")) {
      opts.sqlitePath = arg.slice("--sqlite-path=".length) || DEFAULT_SQLITE_PATH;
    }
  }

  if (!opts.area.city) {
    opts.area.city = "Istanbul";
  }
  if (!opts.area.district) {
    opts.area.district = "Atasehir";
  }

  return opts;
}

function resolveSources(rawSources) {
  if (!rawSources || rawSources === "all") {
    return Object.keys(SOURCE_DEFS);
  }

  const requested = rawSources
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
  const valid = requested.filter((x) => Object.prototype.hasOwnProperty.call(SOURCE_DEFS, x));
  if (valid.length === 0) {
    return Object.keys(SOURCE_DEFS);
  }
  return valid;
}

async function crawlCloudflareProneSource(ctx) {
  const { def } = ctx;
  const fetchedAt = new Date().toISOString();
  const text = await fetchText(def.url, ctx.timeoutMs);

  if (isCloudflareBlocked(text)) {
    return {
      source: def.source,
      url: def.url,
      status: "blocked",
      blocked: true,
      observedTotal: null,
      fetchedAt,
      notes: ["Cloudflare challenge detected (prototype fetch mode)."],
      listings: []
    };
  }

  const linkRegex = /href="([^"]+)"/gi;
  const listings = [];
  const seen = new Set();
  for (const m of text.matchAll(linkRegex)) {
    const href = m[1];
    if (!/ilan|listing|satilik|kiralik/i.test(href)) {
      continue;
    }
    const absolute = toAbsoluteUrl(def.url, href);
    if (seen.has(absolute)) {
      continue;
    }
    seen.add(absolute);
    listings.push(
      createListing(def.source, extractTrailingNumber(absolute), absolute, {
        title: "",
        address: ""
      })
    );
  }

  return {
    source: def.source,
    url: def.url,
    status: "ok",
    blocked: false,
    observedTotal: listings.length || null,
    fetchedAt,
    notes: listings.length ? [] : ["No listing links parsed from non-blocked response."],
    listings
  };
}

async function crawlEmlakjet(ctx) {
  const { def } = ctx;
  const fetchedAt = new Date().toISOString();
  const rootHtml = await fetchTextWithBlockFallback(def.url, ctx.timeoutMs);

  if (isCloudflareBlocked(rootHtml)) {
    return {
      source: def.source,
      url: def.url,
      status: "blocked",
      blocked: true,
      observedTotal: null,
      fetchedAt,
      notes: ["Cloudflare challenge detected on main page."],
      listings: []
    };
  }

  const observedTotal = parseEmlakjetObservedTotal(rootHtml);
  const rootListingCount = extractEmlakjetListingPaths(rootHtml).length;
  const pageUrls = buildEmlakjetPageUrls(def.url, rootHtml, observedTotal, rootListingCount);

  const maxPages = 40;
  const limitedPageUrls = pageUrls.slice(0, maxPages);
  const pageResults = await mapLimit(limitedPageUrls, ctx.concurrency, async (pageUrl) => {
    try {
      const html = await fetchTextWithBlockFallback(pageUrl, ctx.timeoutMs);
      if (isCloudflareBlocked(html)) {
        return { pageUrl, blocked: true, listingPaths: [] };
      }
      const listingPaths = extractEmlakjetListingPaths(html);
      return { pageUrl, blocked: false, listingPaths };
    } catch (error) {
      return { pageUrl, blocked: false, listingPaths: [], error: normalizeError(error) };
    }
  });

  const listingPaths = uniqueBy(
    pageResults.flatMap((x) => x.listingPaths || []),
    (x) => x
  );

  const baseAddress = `${ctx.area.city} / ${ctx.area.district}`;
  const detailTargets = listingPaths.slice(0, Math.max(1, ctx.emlakjetDetailMax || 350));
  const detailConcurrency = Math.max(1, Math.min(ctx.concurrency || 4, 8));

  const detailResults = await mapLimit(detailTargets, detailConcurrency, async (listingPath) => {
    const listingId = extractTrailingNumber(listingPath);
    const listingUrl = `https://www.emlakjet.com${listingPath}`;
    const fallbackTitle = slugToTitle(listingPath);
    try {
      const detailHtml = await fetchTextWithBlockFallback(listingUrl, ctx.timeoutMs);
      if (isCloudflareBlocked(detailHtml)) {
        return {
          blocked: true,
          errored: false,
          parsed: false,
          listing: createListing(def.source, listingId, listingUrl, {
            title: fallbackTitle,
            address: baseAddress
          })
        };
      }
      const detail = parseEmlakjetListingDetail(detailHtml, ctx.area);
      return {
        blocked: false,
        errored: false,
        parsed: Boolean(detail._featureCount),
        listing: createListing(def.source, listingId, listingUrl, {
          title: detail.title || fallbackTitle,
          address: detail.address || baseAddress,
          neighborhood: detail.neighborhood || "",
          roomCount: detail.roomCount || "",
          buildingAge: detail.buildingAge || "",
          floorInfo: detail.floorInfo || "",
          priceTl: detail.priceTl,
          grossSqm: detail.grossSqm,
          netSqm: detail.netSqm,
          avgPriceForSale: detail.avgPriceForSale,
          endeksaMinPrice: detail.endeksaMinPrice,
          endeksaMaxPrice: detail.endeksaMaxPrice
        })
      };
    } catch {
      return {
        blocked: false,
        errored: true,
        parsed: false,
        listing: createListing(def.source, listingId, listingUrl, {
          title: fallbackTitle,
          address: baseAddress
        })
      };
    }
  });

  const detailListings = detailResults.map((x) => x.listing);
  const remainingListings = listingPaths.slice(detailTargets.length).map((listingPath) => {
    const listingId = extractTrailingNumber(listingPath);
    return createListing(def.source, listingId, `https://www.emlakjet.com${listingPath}`, {
      title: slugToTitle(listingPath),
      address: baseAddress
    });
  });
  const listings = [...detailListings, ...remainingListings];

  const notes = [];
  const blockedPages = pageResults.filter((x) => x.blocked).length;
  if (blockedPages > 0) {
    notes.push(`${blockedPages} page(s) returned challenge pages.`);
  }
  const errorPages = pageResults.filter((x) => x.error).length;
  if (errorPages > 0) {
    notes.push(`${errorPages} page(s) failed to fetch.`);
  }
  notes.push(`Fetched ${limitedPageUrls.length} emlakjet page(s).`);
  const detailParsedCount = detailResults.filter((x) => x.parsed).length;
  const detailBlockedCount = detailResults.filter((x) => x.blocked).length;
  const detailErrorCount = detailResults.filter((x) => x.errored).length;
  notes.push(`Parsed details for ${detailParsedCount}/${detailTargets.length} listing page(s).`);
  if (detailBlockedCount > 0) {
    notes.push(`${detailBlockedCount} detail page(s) returned challenge pages.`);
  }
  if (detailErrorCount > 0) {
    notes.push(`${detailErrorCount} detail page(s) failed to fetch.`);
  }
  if (listingPaths.length > detailTargets.length) {
    notes.push(`Detail parsing capped at ${detailTargets.length} listing(s).`);
  }

  return {
    source: def.source,
    url: def.url,
    status: "ok",
    blocked: false,
    observedTotal,
    fetchedAt,
    notes,
    listings
  };
}

async function crawlAtasehirSatilik(ctx) {
  const { def } = ctx;
  const fetchedAt = new Date().toISOString();
  const xml = await fetchTextWithBlockFallback(def.url, ctx.timeoutMs);

  const allUrls = extractLocUrlsFromSitemap(xml);
  const listingUrls = allUrls.filter((u) =>
    /^https:\/\/atasehirsatilik\.com\/[^/]+\/[^/]+\/[^/]+-\d+$/.test(u)
  );

  const listings = uniqueBy(listingUrls, (u) => u).map((url) =>
    createListing("atasehirsatilik", extractTrailingNumber(url), url, {
      title: slugToTitle(url),
      address: "Istanbul / Atasehir"
    })
  );

  return {
    source: def.source,
    url: def.url,
    status: "ok",
    blocked: false,
    observedTotal: listings.length,
    fetchedAt,
    notes: [`Parsed ${allUrls.length} URLs from sitemap.xml feed.`],
    listings
  };
}

async function crawlTuryapOffice251316(ctx) {
  const { def } = ctx;
  const fetchedAt = new Date().toISOString();
  const html = await fetchTextWithBlockFallback(def.url, ctx.timeoutMs);

  const observedTotal = parseTuryapObservedTotal(html);
  const entryRegex =
    /<a href="Portfoy_Bilgileri\.aspx\?ProductID=(\d+)"[\s\S]{0,2800}?<h2[^>]*class="item-title"[^>]*>[\s\S]*?<a href="Portfoy_Bilgileri\.aspx\?ProductID=\1">([^<]+)<\/a>[\s\S]{0,2000}?<address class="item-address">([\s\S]*?)<\/address>/gi;

  const rows = [];
  for (const match of html.matchAll(entryRegex)) {
    const listingId = match[1];
    const title = decodeHtml(match[2]).trim();
    const address = decodeHtml(stripTags(match[3])).replace(/\s+/g, " ").trim();
    rows.push({ listingId, title, address });
  }

  const atasehirRows = rows.filter((r) =>
    normalizeForMatch(r.address).includes("istanbul / atasehir")
  );

  const listings = uniqueBy(atasehirRows, (x) => x.listingId).map((row) =>
    createListing(def.source, row.listingId, `https://www.turyap.com.tr/Portfoy_Bilgileri.aspx?ProductID=${row.listingId}`, {
      title: row.title,
      address: row.address
    })
  );

  const notes = [];
  notes.push(`Parsed ${rows.length} listing cards on current page.`);
  if (observedTotal !== null) {
    notes.push(`Page reports ${observedTotal} total listings for this office filter.`);
  }
  if (listings.length === 0) {
    notes.push("No listing card with 'Istanbul / Ataşehir' address found on fetched page.");
  }

  return {
    source: def.source,
    url: def.url,
    status: "ok",
    blocked: false,
    observedTotal,
    fetchedAt,
    notes,
    listings
  };
}

async function fetchText(url, timeoutMs) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: "GET",
      headers: {
        "user-agent": BROWSER_UA,
        accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "tr-TR,tr;q=0.9,en;q=0.8"
      },
      signal: controller.signal
    });
    return await res.text();
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchTextViaCurl(url, timeoutMs) {
  const maxTimeSec = Math.max(5, Math.ceil(timeoutMs / 1000));
  const args = [
    "-sS",
    "-L",
    "--max-time",
    String(maxTimeSec),
    "-A",
    BROWSER_UA,
    "-H",
    "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "-H",
    "Accept-Language: tr-TR,tr;q=0.9,en;q=0.8",
    url
  ];
  const { stdout } = await execFileAsync("curl", args, {
    maxBuffer: 64 * 1024 * 1024
  });
  return stdout;
}

async function fetchTextWithBlockFallback(url, timeoutMs) {
  try {
    const html = await fetchText(url, timeoutMs);
    if (!isCloudflareBlocked(html)) {
      return html;
    }
  } catch {
    // fallback below
  }
  return fetchTextViaCurl(url, timeoutMs);
}

function isCloudflareBlocked(text) {
  const lower = text.toLowerCase();
  return (
    lower.includes("just a moment") ||
    lower.includes("enable javascript and cookies to continue") ||
    lower.includes("__cf_chl")
  );
}

function buildEmlakjetPageUrls(baseUrl, html, observedTotal, rootListingCount) {
  const pageNumbersFromHtml = [...html.matchAll(/[?&]sayfa=(\d+)/gi)]
    .map((m) => toPositiveInt(m[1], null))
    .filter((x) => x !== null);

  let targetPageCount = pageNumbersFromHtml.length ? Math.max(...pageNumbersFromHtml) : 1;
  const pageSizeGuess = rootListingCount > 0 ? rootListingCount : 30;
  if (observedTotal !== null && pageSizeGuess > 0) {
    const estimatedPages = Math.ceil(observedTotal / pageSizeGuess);
    if (Number.isFinite(estimatedPages) && estimatedPages > targetPageCount) {
      targetPageCount = estimatedPages;
    }
  }

  const hardPageCap = 80;
  targetPageCount = Math.max(1, Math.min(targetPageCount, hardPageCap));

  const urls = [baseUrl];
  for (let page = 2; page <= targetPageCount; page += 1) {
    urls.push(withQueryParam(baseUrl, "sayfa", String(page)));
  }
  return uniqueBy(urls, (x) => x);
}

function extractEmlakjetListingPaths(html) {
  const regex = /\/ilan\/[^\s"'<>()\\]+-\d+/gi;
  const cleaned = [...html.matchAll(regex)].map((m) =>
    m[0].replace(/\\+/g, "").replace(/[.,;:!?]+$/, "")
  );
  return uniqueBy(cleaned, (x) => x);
}

function parseEmlakjetObservedTotal(html) {
  const m1 = html.match(/"totalListingLength":\s*(\d+)/);
  if (m1) {
    return toPositiveInt(m1[1], null);
  }
  const m2 = html.match(/([0-9.]+)\s+adet[\s\S]{0,160}?sat[ıi]l[ıi]k\s+ev\s+ilan[ıi]/i);
  if (m2) {
    return toPositiveInt(m2[1].replace(/\./g, ""), null);
  }
  return null;
}

function parseEmlakjetListingDetail(html, area) {
  const title = cleanTitle(decodeHtml(extractRegexGroup(html, /<title>([^<]+)<\/title>/i) || ""));
  const cityName =
    decodeHtml(
      extractFirstRegexGroup(html, [
        /"il"\s*:\s*\{"definition":\{"id":[^}]*"name":"([^"]+)"/i,
        /"key":"il","value":\{"definition":\{"id":[^}]*"name":"([^"]+)"/i,
        /"cityName":"([^"]+)"/i
      ])
    ) || area.city;
  const districtName =
    decodeHtml(
      extractFirstRegexGroup(html, [
        /"ilce"\s*:\s*\{"definition":\{"id":[^}]*"name":"([^"]+)"/i,
        /"key":"ilce","value":\{"definition":\{"id":[^}]*"name":"([^"]+)"/i,
        /"districtName":"([^"]+)"/i
      ])
    ) || area.district;
  const neighborhood = decodeHtml(
    extractFirstRegexGroup(html, [
      /"mahalle"\s*:\s*\{"definition":\{"id":[^}]*"name":"([^"]+)"/i,
      /"key":"mahalle","value":\{"definition":\{"id":[^}]*"name":"([^"]+)"/i,
      /"townName":"([^"]+)"/i
    ])
  );

  const addressParts = [cityName, districtName, neighborhood].filter(Boolean);
  const address = addressParts.length ? addressParts.join(" / ") : `${area.city} / ${area.district}`;

  const priceTl = firstFiniteNumber([
    parseLooseNumber(extractRegexGroup(html, /"ilan_fiyat"\s*:\s*\{"definition"\s*:\s*([0-9.]+)/i)),
    parseLooseNumber(extractRegexGroup(html, /"key":"ilan_fiyat","value":\{"definition":([0-9.]+)/i)),
    parseLooseNumber(extractRegexGroup(html, /"ilan_fiyat":"([0-9.]+)"/i)),
    parseLooseNumber(extractRegexGroup(html, /"offers"\s*:\s*\{"@type":"Offer"[^}]*"price":"([0-9.]+)"/i))
  ]);
  const grossSqm = parseSqmValue(
    extractFirstRegexGroup(html, [
      /"ilan_metrekare_brut"\s*:\s*\{"definition"\s*:\s*"([^"]+)"/i,
      /"key":"ilan_metrekare_brut","value":\{"definition":"([^"]+)"/i,
      /"description":"[^"]*?([0-9]+(?:[.,][0-9]+)?)\s*m²/i,
      /"description":"[^"]*?([0-9]+(?:[.,][0-9]+)?)\s*m2/i
    ])
  );
  const netSqm = parseSqmValue(
    extractFirstRegexGroup(html, [
      /"ilan_metrekare_net"\s*:\s*\{"definition"\s*:\s*"([^"]+)"/i,
      /"key":"ilan_metrekare_net","value":\{"definition":"([^"]+)"/i
    ])
  );
  const roomCount =
    extractFirstRegexGroup(html, [
      /"ilan_oda"\s*:\s*\{"definition"\s*:\s*"([^"]+)"/i,
      /"key":"ilan_oda","value":\{"definition":"([^"]+)"/i,
      /"oda_sayisi":"([^"]+)"/i
    ]) || "";
  const buildingAge =
    extractFirstRegexGroup(html, [
      /"ilan_bina_yas"\s*:\s*\{"definition"\s*:\s*"([^"]+)"/i,
      /"key":"ilan_bina_yas","value":\{"definition":"([^"]+)"/i
    ]) || "";
  const floorInfo =
    extractFirstRegexGroup(html, [
      /"ilan_kat"\s*:\s*\{"definition"\s*:\s*"([^"]+)"/i,
      /"key":"ilan_kat","value":\{"definition":"([^"]+)"/i
    ]) || "";
  const avgPriceForSale = parseLooseNumber(extractRegexGroup(html, /"averagePriceForSale"\s*:\s*([0-9.]+)/i));
  const endeksaMinPrice = parseLooseNumber(extractRegexGroup(html, /"endeksaValuation"\s*:\s*\{"minPrice"\s*:\s*([0-9.]+)/i));
  const endeksaMaxPrice = parseLooseNumber(extractRegexGroup(html, /"endeksaValuation"\s*:\s*\{[^}]*"maxPrice"\s*:\s*([0-9.]+)/i));

  const featureCount = [priceTl, grossSqm, netSqm, roomCount, buildingAge, floorInfo, avgPriceForSale].filter(Boolean)
    .length;

  return {
    _featureCount: featureCount,
    title,
    address,
    neighborhood,
    roomCount,
    buildingAge,
    floorInfo,
    priceTl,
    grossSqm,
    netSqm,
    avgPriceForSale,
    endeksaMinPrice,
    endeksaMaxPrice
  };
}

function parseTuryapObservedTotal(html) {
  const m = html.match(/Toplamda\s*:\s*([0-9.]+)\s*ilan\s*listelendi\./i);
  if (!m) {
    return null;
  }
  return toPositiveInt(m[1].replace(/\./g, ""), null);
}

function extractLocUrlsFromSitemap(xml) {
  return uniqueBy(
    [...xml.matchAll(/<loc>([^<]+)<\/loc>/gi)].map((m) => decodeHtml(m[1]).trim()),
    (x) => x
  );
}

function createListing(source, listingId, url, extra) {
  const id = listingId ? String(listingId) : "";
  const listingKey = buildListingKey(url);
  return {
    source,
    listingId: id,
    listingKey,
    url,
    title: extra.title || "",
    address: extra.address || "",
    neighborhood: extra.neighborhood || "",
    roomCount: extra.roomCount || "",
    buildingAge: extra.buildingAge || "",
    floorInfo: extra.floorInfo || "",
    priceTl: toNullableNumber(extra.priceTl),
    grossSqm: toNullableNumber(extra.grossSqm),
    netSqm: toNullableNumber(extra.netSqm),
    avgPriceForSale: toNullableNumber(extra.avgPriceForSale),
    endeksaMinPrice: toNullableNumber(extra.endeksaMinPrice),
    endeksaMaxPrice: toNullableNumber(extra.endeksaMaxPrice),
    crawledAt: new Date().toISOString()
  };
}

function buildListingKey(url) {
  try {
    const u = new URL(url);
    const normalizedPath = u.pathname.endsWith("/") && u.pathname.length > 1 ? u.pathname.slice(0, -1) : u.pathname;
    return `${u.origin}${normalizedPath}${u.search}`;
  } catch {
    return String(url).replace(/#.*$/, "");
  }
}

function uniqueBy(arr, keyFn) {
  const seen = new Set();
  const out = [];
  for (const item of arr) {
    const key = keyFn(item);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    out.push(item);
  }
  return out;
}

function toAbsoluteUrl(baseUrl, maybeRelativeUrl) {
  try {
    return new URL(maybeRelativeUrl, baseUrl).toString();
  } catch {
    return maybeRelativeUrl;
  }
}

function withQueryParam(url, key, value) {
  try {
    const u = new URL(url);
    u.searchParams.set(key, value);
    return u.toString();
  } catch {
    const cleaned = String(url).replace(/#.*$/, "");
    const sep = cleaned.includes("?") ? "&" : "?";
    return `${cleaned}${sep}${encodeURIComponent(key)}=${encodeURIComponent(value)}`;
  }
}

function extractTrailingNumber(text) {
  const m = text.match(/(\d+)(?!.*\d)/);
  return m ? m[1] : "";
}

function slugToTitle(text) {
  const cleaned = text
    .replace(/^https?:\/\/[^/]+/i, "")
    .replace(/^\/+/g, "")
    .replace(/[-_/]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return cleaned;
}

function stripTags(input) {
  return input.replace(/<[^>]+>/g, " ");
}

function cleanTitle(title) {
  return String(title || "")
    .replace(/\s+/g, " ")
    .replace(/#\d+\s*$/, "")
    .trim();
}

function extractRegexGroup(text, regex) {
  const raw = String(text || "");
  const candidates = [raw];
  if (raw.includes('\\"')) {
    candidates.push(raw.replace(/\\"/g, '"'));
  }
  for (const candidate of candidates) {
    const match = candidate.match(regex);
    if (match && match[1]) {
      return decodeUnicodeEscapes(match[1]);
    }
  }
  return "";
}

function extractFirstRegexGroup(text, regexes) {
  for (const regex of regexes) {
    const value = extractRegexGroup(text, regex);
    if (value) {
      return value;
    }
  }
  return "";
}

function parseLooseNumber(text) {
  if (text == null) {
    return null;
  }
  const cleaned = String(text).replace(/[^0-9.,-]/g, "");
  if (!cleaned) {
    return null;
  }
  const normalized = cleaned.includes(",") && !cleaned.includes(".") ? cleaned.replace(",", ".") : cleaned.replace(/,/g, "");
  const n = Number(normalized);
  return Number.isFinite(n) ? n : null;
}

function parseSqmValue(text) {
  if (!text) {
    return null;
  }
  const match = String(text).match(/([0-9]+(?:[.,][0-9]+)?)/);
  if (!match) {
    return null;
  }
  return parseLooseNumber(match[1]);
}

function firstFiniteNumber(values) {
  for (const value of values) {
    if (Number.isFinite(value)) {
      return value;
    }
  }
  return null;
}

function decodeHtml(input) {
  return input
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">");
}

function decodeUnicodeEscapes(input) {
  return String(input || "").replace(/\\u([0-9a-fA-F]{4})/g, (_, hex) =>
    String.fromCharCode(Number.parseInt(hex, 16))
  );
}

function normalizeAreaName(text) {
  const value = String(text || "")
    .replace(/\s+/g, " ")
    .trim();
  return value || "";
}

function toTurkishSlug(text) {
  return normalizeAreaName(text)
    .toLocaleLowerCase("tr-TR")
    .replace(/ç/g, "c")
    .replace(/ğ/g, "g")
    .replace(/ı/g, "i")
    .replace(/ö/g, "o")
    .replace(/ş/g, "s")
    .replace(/ü/g, "u")
    .replace(/'/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+/, "")
    .replace(/-+$/, "");
}

function normalizeForMatch(text) {
  return text
    .replace(/İ/g, "I")
    .replace(/ı/g, "i")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function isAtasehirArea(area) {
  return (
    normalizeForMatch(area.city) === "istanbul" &&
    normalizeForMatch(area.district) === "atasehir"
  );
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

function toPositiveInt(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) {
    return fallback;
  }
  return Math.floor(n);
}

function toNullableNumber(value) {
  if (value == null || value === "") {
    return null;
  }
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

async function mapLimit(items, limit, mapper) {
  if (items.length === 0) {
    return [];
  }
  const workerCount = Math.max(1, Math.min(limit || 1, items.length));
  const results = new Array(items.length);
  let index = 0;

  async function worker() {
    while (true) {
      const current = index;
      index += 1;
      if (current >= items.length) {
        return;
      }
      results[current] = await mapper(items[current], current);
    }
  }

  await Promise.all(new Array(workerCount).fill(0).map(worker));
  return results;
}

function toCsv(rows) {
  const headers = [
    "source",
    "listingKey",
    "listingId",
    "url",
    "title",
    "address",
    "neighborhood",
    "roomCount",
    "buildingAge",
    "floorInfo",
    "priceTl",
    "grossSqm",
    "netSqm",
    "avgPriceForSale",
    "endeksaMinPrice",
    "endeksaMaxPrice",
    "crawledAt"
  ];
  const lines = [headers.join(",")];
  for (const row of rows) {
    const values = headers.map((h) => csvEscape(row[h]));
    lines.push(values.join(","));
  }
  return `${lines.join("\n")}\n`;
}

function csvEscape(value) {
  const text = value == null ? "" : String(value);
  if (!/[",\n]/.test(text)) {
    return text;
  }
  return `"${text.replace(/"/g, '""')}"`;
}

function formatRunTag(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  const ms = String(d.getMilliseconds()).padStart(3, "0");
  return `${y}${m}${day}-${hh}${mm}${ss}${ms}`;
}

function printHumanSummary(summary) {
  console.log("Crawl Summary");
  console.log("=============");
  console.log(`Area: ${summary.area.city}/${summary.area.district}`);
  console.log(`Started: ${summary.startedAt}`);
  console.log(`Finished: ${summary.finishedAt}`);
  console.log("");

  for (const s of summary.sourceSummaries) {
    console.log(
      `- ${s.source}: status=${s.status}, blocked=${s.blocked}, observedTotal=${s.observedTotal ?? "n/a"}, crawledListings=${s.crawledListings}`
    );
  }

  console.log("");
  console.log(`Total raw listings: ${summary.totals.crawledRaw}`);
  console.log(`Total unique listings: ${summary.totals.crawledUnique}`);
  if (summary.outputRunDir) {
    console.log(`Output folder: ${summary.outputRunDir}`);
  }
  if (summary.sqlite) {
    console.log(`SQLite db: ${summary.sqlite.dbPath} (run_id=${summary.sqlite.runId})`);
  }
}

main().catch((error) => {
  console.error("Crawler failed:", normalizeError(error));
  process.exitCode = 1;
});
