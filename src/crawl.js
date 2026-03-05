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
    crawl: crawlHepsiemlak
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
    hepsiemlakMaxPages: 24,
    hepsiemlakWaitChallengeMs: 45000,
    hepsiemlakDetailMax: 60,
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
    } else if (arg.startsWith("--hepsiemlak-max-pages=")) {
      opts.hepsiemlakMaxPages = toPositiveInt(arg.slice("--hepsiemlak-max-pages=".length), 24);
    } else if (arg.startsWith("--hepsiemlak-wait-ms=")) {
      opts.hepsiemlakWaitChallengeMs = toPositiveInt(arg.slice("--hepsiemlak-wait-ms=".length), 45000);
    } else if (arg.startsWith("--hepsiemlak-detail-max=")) {
      opts.hepsiemlakDetailMax = toPositiveInt(arg.slice("--hepsiemlak-detail-max=".length), 60);
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

async function crawlHepsiemlak(ctx) {
  try {
    return await crawlHepsiemlakViaPlaywright(ctx);
  } catch (error) {
    const fallback = await crawlCloudflareProneSource(ctx);
    const reason = normalizeError(error);
    fallback.notes = [`Playwright mode failed: ${reason}`, ...(fallback.notes || [])];
    return fallback;
  }
}

async function crawlHepsiemlakViaPlaywright(ctx) {
  const { def } = ctx;
  const fetchedAt = new Date().toISOString();
  const notes = [];
  const chromium = await loadPlaywrightChromium();
  if (!chromium) {
    throw new Error("Playwright chromium is unavailable. Run: npx playwright install chromium");
  }

  const browser = await chromium.launch({
    headless: true,
    args: [
      "--disable-blink-features=AutomationControlled",
      "--no-sandbox",
      "--disable-dev-shm-usage"
    ]
  });

  let pageCountFromDom = 1;
  let observedTotal = null;
  let blockedPages = 0;
  const crawledListings = [];

  try {
    const context = await browser.newContext({
      userAgent: BROWSER_UA,
      locale: "tr-TR",
      timezoneId: "Europe/Istanbul",
      viewport: { width: 1365, height: 900 }
    });
    await context.route("**/*", (route) => {
      const type = route.request().resourceType();
      if (type === "image" || type === "font" || type === "media") {
        route.abort();
        return;
      }
      route.continue();
    });

    const page = await context.newPage();
    const candidateUrls = buildHepsiemlakCandidateUrls(ctx.area, def.url);
    let resolvedUrl = def.url;
    let initialNav = null;

    for (const candidateUrl of candidateUrls) {
      const nav = await openHepsiemlakPageWithChallengeHandling(
        page,
        candidateUrl,
        ctx.timeoutMs,
        ctx.hepsiemlakWaitChallengeMs
      );
      if (nav.notFound) {
        notes.push(`URL not found: ${candidateUrl}`);
        continue;
      }
      resolvedUrl = candidateUrl;
      initialNav = nav;
      break;
    }

    if (!initialNav) {
      return {
        source: def.source,
        url: def.url,
        status: "blocked",
        blocked: true,
        observedTotal: null,
        fetchedAt,
        notes: [...notes, "No hepsiemlak URL candidate returned a listing page."],
        listings: []
      };
    }

    if (initialNav.blocked) {
      const challengeSignal = formatHepsiemlakChallengeSignal(initialNav);
      return {
        source: def.source,
        url: resolvedUrl,
        status: "blocked",
        blocked: true,
        observedTotal: null,
        fetchedAt,
        notes: [
          ...notes,
          "Cloudflare challenge not solved in Playwright mode.",
          ...(challengeSignal ? [challengeSignal] : [])
        ],
        listings: []
      };
    }

    const firstPageData = await extractHepsiemlakPageData(page);
    pageCountFromDom = Math.max(1, firstPageData.paginationPageCount || 1);
    observedTotal = firstPageData.observedTotal;
    crawledListings.push(...(firstPageData.listings || []));

    const pageUrls = buildHepsiemlakPageUrls(
      resolvedUrl,
      pageCountFromDom,
      observedTotal,
      firstPageData.listings.length,
      ctx.hepsiemlakMaxPages
    );
    const targetPageUrls = pageUrls.slice(1);
    let blockedPageSignal = "";

    for (const pageUrl of targetPageUrls) {
      const pageNav = await openHepsiemlakPageWithChallengeHandling(
        page,
        pageUrl,
        ctx.timeoutMs,
        Math.min(ctx.hepsiemlakWaitChallengeMs, 18000)
      );
      if (pageNav.blocked) {
        blockedPages += 1;
        if (!blockedPageSignal) {
          blockedPageSignal = formatHepsiemlakChallengeSignal(pageNav);
        }
        continue;
      }
      const pageData = await extractHepsiemlakPageData(page);
      if (observedTotal == null && Number.isFinite(pageData.observedTotal)) {
        observedTotal = pageData.observedTotal;
      }
      crawledListings.push(...(pageData.listings || []));
      await page.waitForTimeout(350);
    }

    const baseAddress = `${ctx.area.city} / ${ctx.area.district}`;
    const listingRows = uniqueBy(
      crawledListings.filter((row) => row?.url && isHepsiemlakListingUrl(row.url)),
      (row) => row.url
    ).map((row) => ({
      ...row,
      listingId: row.listingId || parseHepsiemlakListingIdFromUrl(row.url),
      address: row.address || baseAddress,
      neighborhood: row.neighborhood || parseNeighborhoodFromHepsiemlakUrl(row.url)
    }));

    const detailTargets = listingRows.slice(0, Math.max(1, ctx.hepsiemlakDetailMax || 60));
    let detailParsedCount = 0;
    let detailBlockedCount = 0;
    let detailErrorCount = 0;
    for (const row of detailTargets) {
      try {
        let detailNav = { blocked: true, notFound: false };
        for (let attempt = 0; attempt < 2; attempt += 1) {
          detailNav = await openHepsiemlakPageWithChallengeHandling(
            page,
            row.url,
            ctx.timeoutMs,
            Math.min(ctx.hepsiemlakWaitChallengeMs, 18000)
          );
          if (!detailNav.blocked && !detailNav.notFound) {
            break;
          }
          await page.waitForTimeout(1400 + attempt * 600);
        }
        if (detailNav.blocked || detailNav.notFound) {
          detailBlockedCount += 1;
          continue;
        }
        const pageTitle = await page.title();
        const detailText = await page.evaluate(() => (document.body && document.body.innerText) || "");
        const detailHtml = await page.content();
        const parsed = parseHepsiemlakDetailText(detailText, detailHtml, row.url, ctx.area);
        if (pageTitle) {
          parsed.title = cleanTitle(String(pageTitle).replace(/\|\s*hepsiemlak.*$/i, "").trim());
        }
        Object.assign(row, parsed);
        if (parsed._featureCount > 0) {
          detailParsedCount += 1;
        }
      } catch {
        detailErrorCount += 1;
      }
      await page.waitForTimeout(700);
    }

    const listings = listingRows.map((row) =>
      createListing(def.source, row.listingId, row.url, {
        title: row.title || slugToTitle(row.url),
        address: row.address || baseAddress,
        neighborhood: row.neighborhood || "",
        roomCount: row.roomCount || "",
        buildingAge: row.buildingAge || "",
        floorInfo: row.floorInfo || "",
        deedStatus: row.deedStatus || "",
        creditSuitability: row.creditSuitability || "",
        inSite: row.inSite || "",
        usageStatus: row.usageStatus || "",
        priceTl: row.priceTl,
        grossSqm: row.grossSqm,
        netSqm: row.netSqm
      })
    );

    notes.push(
      `Playwright fetched ${Math.max(1, pageUrls.length - blockedPages)}/${pageUrls.length} hepsiemlak page(s).`
    );
    if (blockedPages > 0) {
      notes.push(`${blockedPages} page(s) were still challenge-blocked.`);
      if (blockedPageSignal) {
        notes.push(`Blocked page sample: ${blockedPageSignal}`);
      }
    }
    if (pageCountFromDom > (ctx.hepsiemlakMaxPages || 24)) {
      notes.push(`Pagination capped at ${ctx.hepsiemlakMaxPages} page(s).`);
    }
    notes.push(`Parsed details for ${detailParsedCount}/${detailTargets.length} listing page(s).`);
    if (detailBlockedCount > 0) {
      notes.push(`${detailBlockedCount} detail page(s) were challenge-blocked or unavailable.`);
    }
    if (detailErrorCount > 0) {
      notes.push(`${detailErrorCount} detail page(s) failed to parse.`);
    }
    if (listingRows.length > detailTargets.length) {
      notes.push(`Detail parsing capped at ${detailTargets.length} listing(s).`);
    }
    if (listings.length === 0) {
      notes.push("No listing cards parsed from rendered page.");
    }

    return {
      source: def.source,
      url: resolvedUrl,
      status: listings.length > 0 ? "ok" : "blocked",
      blocked: listings.length === 0,
      observedTotal: observedTotal ?? null,
      fetchedAt,
      notes,
      listings
    };
  } finally {
    await browser.close();
  }
}

async function loadPlaywrightChromium() {
  try {
    const mod = require("playwright");
    return mod.chromium || null;
  } catch {
    return null;
  }
}

async function openHepsiemlakPageWithChallengeHandling(page, targetUrl, timeoutMs, challengeWaitMs) {
  await page.goto(targetUrl, { waitUntil: "domcontentloaded", timeout: timeoutMs });
  try {
    await page.waitForLoadState("networkidle", { timeout: Math.min(timeoutMs, 10000) });
  } catch {
    // Dynamic pages may keep requests open; continue.
  }

  const deadline = Date.now() + Math.max(6000, challengeWaitMs || timeoutMs);
  while (Date.now() < deadline) {
    const state = await page.evaluate(() => {
      function normalizeForDetect(value) {
        return String(value || "")
          .toLowerCase()
          .replace(/[ç]/g, "c")
          .replace(/[ğ]/g, "g")
          .replace(/[ı]/g, "i")
          .replace(/[ö]/g, "o")
          .replace(/[ş]/g, "s")
          .replace(/[ü]/g, "u");
      }
      const titleRaw = document.title || "";
      const bodyRaw = (document.body && document.body.innerText) || "";
      const title = normalizeForDetect(titleRaw);
      const body = normalizeForDetect(bodyRaw);
      const notFound =
        title.includes("aradiginiz sayfaya ulasilamiyor") ||
        body.includes("404 - aradiginiz sayfaya ulasilamiyor");
      const blocked =
        title.includes("just a moment") ||
        title.includes("bir dakika lutfen") ||
        body.includes("enable javascript and cookies to continue") ||
        body.includes("__cf_chl") ||
        body.includes("guvenlik dogrulamasi gerceklestirme") ||
        body.includes("web sitesi bir bot olmadiginizi dogruladiktan sonra");
      const listingAnchors = [...document.querySelectorAll("a[href]")]
        .map((a) => a.getAttribute("href") || "")
        .filter((href) => /\/\d+-\d+\/?$/.test(href) && /satilik/i.test(href)).length;
      return {
        blocked,
        notFound,
        listingAnchors,
        title: String(titleRaw || "").slice(0, 120),
        bodySnippet: String(bodyRaw || "")
          .replace(/\s+/g, " ")
          .trim()
          .slice(0, 240)
      };
    });

    if (state.notFound) {
      return { blocked: false, notFound: true, title: state.title, bodySnippet: state.bodySnippet };
    }
    if (!state.blocked && state.listingAnchors > 0) {
      return { blocked: false, notFound: false, title: state.title, bodySnippet: state.bodySnippet };
    }
    if (!state.blocked) {
      return { blocked: false, notFound: false, title: state.title, bodySnippet: state.bodySnippet };
    }
    await page.waitForTimeout(1500);
  }

  const finalState = await page.evaluate(() => {
    function normalizeForDetect(value) {
      return String(value || "")
        .toLowerCase()
        .replace(/[ç]/g, "c")
        .replace(/[ğ]/g, "g")
        .replace(/[ı]/g, "i")
        .replace(/[ö]/g, "o")
        .replace(/[ş]/g, "s")
        .replace(/[ü]/g, "u");
    }
    const titleRaw = document.title || "";
    const bodyRaw = (document.body && document.body.innerText) || "";
    const title = normalizeForDetect(titleRaw);
    const body = normalizeForDetect(bodyRaw);
    const notFound =
      title.includes("aradiginiz sayfaya ulasilamiyor") ||
      body.includes("404 - aradiginiz sayfaya ulasilamiyor");
    const blocked =
      title.includes("just a moment") ||
      title.includes("bir dakika lutfen") ||
      body.includes("enable javascript and cookies to continue") ||
      body.includes("__cf_chl") ||
      body.includes("guvenlik dogrulamasi gerceklestirme") ||
      body.includes("web sitesi bir bot olmadiginizi dogruladiktan sonra");
    return {
      blocked,
      notFound,
      title: String(titleRaw || "").slice(0, 120),
      bodySnippet: String(bodyRaw || "")
        .replace(/\s+/g, " ")
        .trim()
        .slice(0, 240)
    };
  });
  return finalState;
}

async function extractHepsiemlakPageData(page) {
  return page.evaluate(() => {
    function clean(text) {
      return String(text || "").replace(/\s+/g, " ").trim();
    }

    function parseNumber(value) {
      if (value == null) {
        return null;
      }
      const cleaned = String(value).replace(/[^0-9.,-]/g, "");
      if (!cleaned) {
        return null;
      }
      const normalized =
        cleaned.includes(",") && !cleaned.includes(".")
          ? cleaned.replace(",", ".")
          : cleaned.replace(/,/g, "");
      const n = Number(normalized);
      return Number.isFinite(n) ? n : null;
    }

    function parsePrice(text) {
      const m = String(text).match(/([0-9][0-9.\s]*)\s*(TL|₺)/i);
      return m ? parseNumber(m[1].replace(/\s+/g, "")) : null;
    }

    function parseRoom(text) {
      const m = String(text).match(/(\d+\s*\+\s*\d+|st[üu]dyo)/i);
      return m ? clean(m[1]).replace(/\s+/g, "") : "";
    }

    function parseSqm(text) {
      const m = String(text).match(/([0-9]{2,4}(?:[.,][0-9]+)?)\s*m²/i);
      return m ? parseNumber(m[1]) : null;
    }

    function parseBuildingAge(text) {
      const m = String(text).match(/([0-9]{1,2}\s*-\s*[0-9]{1,2}|[0-9]{1,2}\s*(?:ve\s*)?uzeri|s[ıi]f[ıi]r\s*bina|yeni)/i);
      return m ? clean(m[1]) : "";
    }

    function parseFloor(text) {
      const m = String(text).match(
        /(ara\s*kat|bahce\s*kati|bah[çc]e\s*kat[ıi]|zemin|giris|giri[sş]|en\s*[uü]st\s*kat|[çc]at[ıi]\s*kat[ıi]|[0-9]{1,2}\.?\s*kat)/i
      );
      return m ? clean(m[1]) : "";
    }

    function parseAddress(text) {
      const lines = clean(text)
        .split(" ")
        .join(" ")
        .split("•")
        .map((x) => clean(x))
        .filter(Boolean);
      for (const line of lines) {
        if (/istanbul/i.test(line) && /(atasehir|ataşehir)/i.test(line)) {
          return line.replace(/\s*-\s*/g, " / ");
        }
      }
      return "";
    }

    function parseNeighborhood(text, title) {
      const neighborhoodFromText = String(text).match(/([A-Za-zÇĞİÖŞÜçğıöşü0-9.\- ]+Mah(?:\.|allesi)?)/i);
      if (neighborhoodFromText) {
        return clean(neighborhoodFromText[1]);
      }
      const neighborhoodFromTitle = String(title).match(/([A-Za-zÇĞİÖŞÜçğıöşü0-9.\- ]+Mah(?:\.|allesi)?)/i);
      if (neighborhoodFromTitle) {
        return clean(neighborhoodFromTitle[1]);
      }
      return "";
    }

    function isListingUrl(urlText) {
      try {
        const u = new URL(urlText);
        if (!/hepsiemlak\.com$/i.test(u.hostname)) {
          return false;
        }
        if (!/satilik/i.test(u.pathname)) {
          return false;
        }
        return /\/\d+-\d+\/?$/.test(u.pathname);
      } catch {
        return false;
      }
    }

    const anchors = Array.from(document.querySelectorAll("a[href]"));
    const listingByUrl = new Map();
    const pageNumberCandidates = [];

    for (const anchor of anchors) {
      const href = anchor.getAttribute("href");
      if (!href) {
        continue;
      }

      let absUrl = "";
      try {
        absUrl = new URL(href, window.location.origin).toString();
      } catch {
        continue;
      }

      try {
        const p = new URL(absUrl).searchParams.get("sayfa");
        const pageNum = p ? Number(p) : NaN;
        if (Number.isFinite(pageNum) && pageNum > 1) {
          pageNumberCandidates.push(Math.floor(pageNum));
        }
      } catch {
        // ignore
      }

      if (!isListingUrl(absUrl) || listingByUrl.has(absUrl)) {
        continue;
      }

      const card = anchor.closest("article, li, div");
      const container = card || anchor;
      const text = clean(container.innerText || anchor.innerText || "");
      const titleEl = container.querySelector
        ? container.querySelector("h1, h2, h3, h4, h5, h6")
        : null;
      const title = clean(
        (titleEl && titleEl.innerText) || anchor.getAttribute("title") || anchor.textContent || ""
      );
      const address = parseAddress(text);
      const listingIdMatch = absUrl.match(/\/(\d+-\d+)\/?$/);

      listingByUrl.set(absUrl, {
        url: absUrl,
        listingId: listingIdMatch ? listingIdMatch[1] : "",
        title,
        address,
        neighborhood: parseNeighborhood(text, title),
        roomCount: parseRoom(text),
        buildingAge: parseBuildingAge(text),
        floorInfo: parseFloor(text),
        priceTl: parsePrice(text),
        grossSqm: parseSqm(text),
        netSqm: null
      });
    }

    const html = document.documentElement ? document.documentElement.outerHTML : "";
    const urlRegex = /https:\/\/www\.hepsiemlak\.com\/[^"'<>\\\s]+?\/\d+-\d+\/?/gi;
    for (const m of html.matchAll(urlRegex)) {
      const absolute = clean(m[0]);
      if (!isListingUrl(absolute) || listingByUrl.has(absolute)) {
        continue;
      }
      const listingIdMatch = absolute.match(/\/(\d+-\d+)\/?$/);
      listingByUrl.set(absolute, {
        url: absolute,
        listingId: listingIdMatch ? listingIdMatch[1] : "",
        title: "",
        address: "",
        neighborhood: "",
        roomCount: "",
        buildingAge: "",
        floorInfo: "",
        priceTl: null,
        grossSqm: null,
        netSqm: null
      });
    }

    const pageText = clean((document.body && document.body.innerText) || "");
    let observedTotal = null;
    for (const regex of [/([0-9][0-9.]*)\s*ilan\s*bulundu/i, /([0-9][0-9.]*)\s*sonuc/i]) {
      const m = pageText.match(regex);
      if (!m) {
        continue;
      }
      const n = Number(String(m[1]).replace(/\./g, ""));
      if (Number.isFinite(n) && n > 0) {
        observedTotal = n;
        break;
      }
    }

    return {
      observedTotal,
      paginationPageCount: pageNumberCandidates.length ? Math.max(...pageNumberCandidates) : 1,
      listings: Array.from(listingByUrl.values())
    };
  });
}

function buildHepsiemlakPageUrls(baseUrl, domPageCount, observedTotal, firstPageListingCount, maxPages) {
  let targetPageCount = Math.max(1, Number(domPageCount) || 1);
  const pageSizeGuess = Math.max(1, Number(firstPageListingCount) || 24);
  if (Number.isFinite(observedTotal) && observedTotal > 0) {
    const estimatedPages = Math.ceil(observedTotal / pageSizeGuess);
    if (estimatedPages > targetPageCount) {
      targetPageCount = estimatedPages;
    }
  }
  targetPageCount = Math.min(Math.max(1, targetPageCount), Math.max(1, maxPages || 24));

  const out = [baseUrl];
  for (let page = 2; page <= targetPageCount; page += 1) {
    out.push(withQueryParam(baseUrl, "sayfa", String(page)));
  }
  return uniqueBy(out, (x) => x);
}

function buildHepsiemlakCandidateUrls(area, fallbackUrl) {
  const districtSlug = toTurkishSlug(area?.district || "");
  const citySlug = toTurkishSlug(area?.city || "");
  const candidates = [];
  if (fallbackUrl) {
    candidates.push(fallbackUrl);
  }
  if (districtSlug) {
    candidates.push(`https://www.hepsiemlak.com/${districtSlug}-satilik`);
    candidates.push(`https://www.hepsiemlak.com/${districtSlug}-satilik/daire`);
    candidates.push(`https://www.hepsiemlak.com/${districtSlug}-${districtSlug}-satilik`);
    candidates.push(`https://www.hepsiemlak.com/${districtSlug}-${districtSlug}-satilik/daire`);
  }
  if (citySlug && districtSlug) {
    candidates.push(`https://www.hepsiemlak.com/${citySlug}-${districtSlug}-satilik`);
    candidates.push(`https://www.hepsiemlak.com/${citySlug}-${districtSlug}-satilik/daire`);
  }
  return uniqueBy(candidates, (x) => x);
}

function isHepsiemlakListingUrl(urlText) {
  try {
    const u = new URL(urlText);
    if (!/hepsiemlak\.com$/i.test(u.hostname)) {
      return false;
    }
    if (!/satilik/i.test(u.pathname)) {
      return false;
    }
    return /\/\d+-\d+\/?$/.test(u.pathname);
  } catch {
    return false;
  }
}

function parseHepsiemlakListingIdFromUrl(urlText) {
  const m = String(urlText || "").match(/\/(\d+-\d+)\/?$/);
  return m ? m[1] : extractTrailingNumber(String(urlText || ""));
}

function parseNeighborhoodFromHepsiemlakUrl(urlText) {
  try {
    const pathname = new URL(urlText).pathname.replace(/^\/+/, "");
    const firstSegment = pathname.split("/")[0] || "";
    const slug = firstSegment.replace(/-satilik.*/i, "");
    const parts = slug.split("-").filter(Boolean);
    if (parts.length < 3) {
      return "";
    }
    const neighborhoodParts = parts.slice(2);
    return neighborhoodParts
      .map((part) => part.charAt(0).toLocaleUpperCase("tr-TR") + part.slice(1))
      .join(" ")
      .trim();
  } catch {
    return "";
  }
}

function formatHepsiemlakChallengeSignal(navState) {
  if (!navState) {
    return "";
  }
  const title = cleanTitle(String(navState.title || ""));
  const snippet = cleanTitle(String(navState.bodySnippet || ""));
  const signal = [title, snippet].filter(Boolean).join(" | ");
  return signal ? `Challenge page signal: ${signal}` : "";
}

function parseHepsiemlakDetailText(rawText, rawHtml, listingUrl, area) {
  const text = String(rawText || "").replace(/\s+/g, " ").trim();
  const html = String(rawHtml || "");
  const empty = {
    _featureCount: 0,
    title: "",
    address: `${area.city} / ${area.district}`,
    neighborhood: parseNeighborhoodFromHepsiemlakUrl(listingUrl),
    roomCount: "",
    buildingAge: "",
    floorInfo: "",
    deedStatus: "",
    creditSuitability: "",
    inSite: "",
    usageStatus: "",
    priceTl: null,
    grossSqm: null,
    netSqm: null
  };
  if (!text && !html) {
    return empty;
  }

  function esc(value) {
    return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  function sliceFeaturesSegment(fullText) {
    const lower = normalizeForMatch(fullText);
    const startIdx = lower.indexOf("ilan ozellikleri");
    if (startIdx < 0) {
      return fullText;
    }
    const endTokens = [
      "trend aramalar",
      "guvenlik hatirlatmalari",
      "hatali ilan bildir",
      "firmanin diger ilanlari"
    ];
    let endIdx = fullText.length;
    for (const token of endTokens) {
      const idx = lower.indexOf(token, startIdx + 20);
      if (idx > 0 && idx < endIdx) {
        endIdx = idx;
      }
    }
    return fullText.slice(startIdx, endIdx);
  }

  function labelValue(sourceText, label, nextLabels) {
    const next = nextLabels.map((x) => esc(x)).join("|");
    const pattern = next
      ? new RegExp(`${esc(label)}\\s*([^]+?)\\s*(?=(?:${next})\\s|$)`, "i")
      : new RegExp(`${esc(label)}\\s*([^]+)$`, "i");
    const m = sourceText.match(pattern);
    return m ? cleanTitle(m[1]) : "";
  }

  const featureText = sliceFeaturesSegment(text);
  const labels = [
    "İlan no",
    "Son Güncelleme",
    "İlan Durumu",
    "Konut Tipi",
    "Konut Şekli",
    "Oda Sayısı",
    "Banyo Sayısı",
    "Brüt / Net M2",
    "Kat Sayısı",
    "Bulunduğu Kat",
    "Bina Yaşı",
    "Isınma Tipi",
    "Tapu Durumu",
    "Krediye Uygunluk",
    "Site İçerisinde",
    "Kullanım Durumu",
    "Eşya Durumu",
    "Aidat",
    "Takas"
  ];
  const values = {};
  for (let i = 0; i < labels.length; i += 1) {
    const label = labels[i];
    const restLabels = labels.slice(i + 1);
    const value = labelValue(featureText, label, restLabels);
    if (value) {
      values[label] = value;
    }
  }

  const structuredPrice = parseLooseNumber((html.match(/"price"\s*:\s*"([0-9.]+)"/i) || [])[1]);
  const topText = text.slice(0, Math.min(text.length, 350));
  const priceTl = firstFiniteNumber([
    structuredPrice,
    parseLooseNumber((topText.match(/([0-9][0-9.\s]*)\s*(?:TL|₺)/i) || [])[1]),
    parseLooseNumber((featureText.match(/([0-9][0-9.\s]*)\s*(?:TL|₺)/i) || [])[1])
  ]);

  let grossSqm = null;
  let netSqm = null;
  const grossNet = values["Brüt / Net M2"] || "";
  const grossNetMatch = grossNet.match(/([0-9]+(?:[.,][0-9]+)?)\s*\/\s*([0-9]+(?:[.,][0-9]+)?)/i);
  if (grossNetMatch) {
    grossSqm = parseLooseNumber(grossNetMatch[1]);
    netSqm = parseLooseNumber(grossNetMatch[2]);
  }
  if (!Number.isFinite(grossSqm)) {
    grossSqm = parseLooseNumber((html.match(/Brüt Metrekare:\s*([0-9]+(?:[.,][0-9]+)?)/i) || [])[1]);
  }
  if (!Number.isFinite(netSqm)) {
    netSqm = parseLooseNumber((html.match(/Net Metrekare:\s*([0-9]+(?:[.,][0-9]+)?)/i) || [])[1]);
  }
  const topSqm = parseLooseNumber((topText.match(/([0-9]{2,4}(?:[.,][0-9]+)?)\s*m2/i) || [])[1]);
  if (!Number.isFinite(grossSqm) || grossSqm < 10) {
    grossSqm = topSqm;
  }
  if (Number.isFinite(netSqm) && netSqm < 10) {
    netSqm = null;
  }

  const roomCountRaw =
    values["Oda Sayısı"] ||
    (featureText.match(/Oda Sayısı\s*([0-9]+\s*\+\s*[0-9]+|Stüdyo)/i) || [])[1] ||
    (html.match(/Oda Sayısı:\s*([0-9]+\s*\+\s*[0-9]+|Stüdyo)/i) || [])[1] ||
    "";
  const roomCount = roomCountRaw ? roomCountRaw.replace(/\s+/g, "") : "";
  const buildingAge = values["Bina Yaşı"] || (featureText.match(/([0-9]{1,2}\s*Yaşında|Sıfır Bina|Yeni)/i) || [])[1] || "";
  const floorInfo = values["Bulunduğu Kat"] || values["Konut Şekli"] || "";

  function compactFeatureValue(value) {
    return cleanTitle(String(value || "").split(/İlan Açıklaması|Özellikler|Çevre|Trend Aramalar/i)[0] || "").slice(0, 80);
  }

  const deedStatus = normalizeFeatureValue(compactFeatureValue(values["Tapu Durumu"] || ""));
  let creditSuitability = normalizeFeatureValue(compactFeatureValue(values["Krediye Uygunluk"] || values["Krediye Uygun"] || ""));
  if (!creditSuitability) {
    const lowText = normalizeForMatch(featureText);
    if (lowText.includes("krediye uygun degil")) {
      creditSuitability = "Krediye Uygun Değil";
    } else if (lowText.includes("krediye uygun")) {
      creditSuitability = "Krediye Uygun";
    }
  }
  let inSite = normalizeFeatureValue(compactFeatureValue(values["Site İçerisinde"] || ""));
  if (!inSite) {
    const lowText = normalizeForMatch(featureText);
    if (lowText.includes("site icerisinde")) {
      inSite = lowText.includes("hayir") ? "Hayır" : "Evet";
    }
  }
  let usageStatus = normalizeFeatureValue(compactFeatureValue(values["Kullanım Durumu"] || ""));
  if (!usageStatus) {
    const lowText = normalizeForMatch(featureText);
    if (lowText.includes("kiracili") || lowText.includes("kiraci")) {
      usageStatus = "Kiracı Oturuyor";
    } else if (lowText.includes("bos")) {
      usageStatus = "Boş";
    } else if (lowText.includes("mulk sahibi")) {
      usageStatus = "Mülk Sahibi Oturuyor";
    }
  }
  const usageNorm = normalizeForMatch(usageStatus);
  if (usageNorm.includes("kiraci")) {
    usageStatus = "Kiracı Oturuyor";
  } else if (usageNorm.includes("mulk sahibi")) {
    usageStatus = "Mülk Sahibi Oturuyor";
  } else if (usageNorm.includes("bos")) {
    usageStatus = "Boş";
  }

  if (inSite && !["Evet", "Hayır"].includes(inSite)) {
    const inSiteNorm = normalizeForMatch(inSite);
    inSite = inSiteNorm.includes("site") ? "Evet" : inSite;
  }
  const neighborhood = normalizeFeatureValue(parseNeighborhoodFromHepsiemlakUrl(listingUrl) || "");

  const featureCount = [
    priceTl,
    grossSqm,
    netSqm,
    roomCount,
    buildingAge,
    floorInfo,
    deedStatus,
    creditSuitability,
    inSite,
    usageStatus
  ].filter((x) => x != null && x !== "").length;

  return {
    _featureCount: featureCount,
    title: "",
    address: `${area.city} / ${area.district}`,
    neighborhood,
    roomCount,
    buildingAge,
    floorInfo,
    deedStatus,
    creditSuitability,
    inSite,
    usageStatus,
    priceTl,
    grossSqm: Number.isFinite(grossSqm) ? grossSqm : null,
    netSqm: Number.isFinite(netSqm) ? netSqm : null
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
          deedStatus: detail.deedStatus || "",
          creditSuitability: detail.creditSuitability || "",
          inSite: detail.inSite || "",
          usageStatus: detail.usageStatus || "",
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
  const lower = normalizeForMatch(String(text || ""));
  return (
    lower.includes("just a moment") ||
    lower.includes("bir dakika lutfen") ||
    lower.includes("enable javascript and cookies to continue") ||
    lower.includes("guvenlik dogrulamasi gerceklestirme") ||
    lower.includes("web sitesi bir bot olmadiginizi dogruladiktan sonra") ||
    lower.includes("cloudflare ile performans ve guvenlik") ||
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
  const deedStatus = normalizeFeatureValue(
    extractFirstRegexGroup(html, [
      /"ilan_tapu"\s*:\s*\{"definition"\s*:\s*"([^"]+)"/i,
      /"key":"ilan_tapu","value":\{"definition":"([^"]+)"/i,
      /"ilan_tapu_durumu"\s*:\s*\{"definition"\s*:\s*"([^"]+)"/i,
      /"key":"ilan_tapu_durumu","value":\{"definition":"([^"]+)"/i,
      /"key":"deed_status","name":"Tapu Durumu","value":"([^"]+)"/i,
      /"key":"deed_status"[^}]{0,220}"value":"([^"]+)"/i,
      /Tapu Durumu<\/span><span[^>]*>([^<]+)<\/span>/i
    ])
  );
  const creditSuitability = normalizeFeatureValue(
    extractFirstRegexGroup(html, [
      /"ilan_krediye_uygun"\s*:\s*\{"definition"\s*:\s*"([^"]+)"/i,
      /"key":"ilan_krediye_uygun","value":\{"definition":"([^"]+)"/i,
      /"key":"suitability_for_credit","name":"Krediye Uygunluk","value":"([^"]+)"/i,
      /"key":"suitability_for_credit"[^}]{0,220}"value":"([^"]+)"/i,
      /Krediye Uygunluk<\/span><span[^>]*>([^<]+)<\/span>/i
    ])
  );
  const inSite = normalizeFeatureValue(
    extractFirstRegexGroup(html, [
      /"ilan_site_icerisinde"\s*:\s*\{"definition"\s*:\s*"([^"]+)"/i,
      /"key":"ilan_site_icerisinde","value":\{"definition":"([^"]+)"/i,
      /"key":"in_site","name":"Site İçerisinde","value":"([^"]+)"/i,
      /"key":"in_site"[^}]{0,220}"value":"([^"]+)"/i,
      /Site İçerisinde<\/span><span[^>]*>([^<]+)<\/span>/i
    ])
  );
  const usageStatus = normalizeFeatureValue(
    extractFirstRegexGroup(html, [
      /"ilan_kullanim"\s*:\s*\{"definition"\s*:\s*"([^"]+)"/i,
      /"key":"ilan_kullanim","value":\{"definition":"([^"]+)"/i,
      /"key":"usability","name":"Kullanım Durumu","value":"([^"]+)"/i,
      /"key":"usability"[^}]{0,220}"value":"([^"]+)"/i,
      /Kullanım Durumu<\/span><span[^>]*>([^<]+)<\/span>/i
    ])
  );
  const avgPriceForSale = parseLooseNumber(extractRegexGroup(html, /"averagePriceForSale"\s*:\s*([0-9.]+)/i));
  const endeksaMinPrice = parseLooseNumber(extractRegexGroup(html, /"endeksaValuation"\s*:\s*\{"minPrice"\s*:\s*([0-9.]+)/i));
  const endeksaMaxPrice = parseLooseNumber(extractRegexGroup(html, /"endeksaValuation"\s*:\s*\{[^}]*"maxPrice"\s*:\s*([0-9.]+)/i));

  const featureCount = [
    priceTl,
    grossSqm,
    netSqm,
    roomCount,
    buildingAge,
    floorInfo,
    deedStatus,
    creditSuitability,
    inSite,
    usageStatus,
    avgPriceForSale
  ].filter(Boolean).length;

  return {
    _featureCount: featureCount,
    title,
    address,
    neighborhood,
    roomCount,
    buildingAge,
    floorInfo,
    deedStatus,
    creditSuitability,
    inSite,
    usageStatus,
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
    deedStatus: extra.deedStatus || "",
    creditSuitability: extra.creditSuitability || "",
    inSite: extra.inSite || "",
    usageStatus: extra.usageStatus || "",
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

function normalizeFeatureValue(text) {
  const raw = decodeHtml(String(text || ""))
    .replace(/\s+/g, " ")
    .trim();
  if (!raw || /^(undefined|null|yok)$/i.test(raw)) {
    return "";
  }

  const normalized = normalizeForMatch(raw.replace(/-/g, " "));
  const aliases = {
    evet: "Evet",
    hayir: "Hayır",
    bos: "Boş",
    "kiraci oturuyor": "Kiracı Oturuyor",
    "mulk sahibi oturuyor": "Mülk Sahibi Oturuyor",
    "krediye uygun": "Krediye Uygun",
    "krediye uygun degil": "Krediye Uygun Değil",
    "kat mulkiyeti": "Kat Mülkiyeti",
    "kat irtifaki": "Kat İrtifakı",
    mustakil: "Müstakil",
    "mustakil tapu": "Müstakil Tapu",
    hisseli: "Hisseli",
    "hisseli tapu": "Hisseli Tapu"
  };
  if (aliases[normalized]) {
    return aliases[normalized];
  }

  if (raw.includes("-") && !raw.includes(" ")) {
    return raw
      .split("-")
      .filter(Boolean)
      .map((part) => part[0].toLocaleUpperCase("tr-TR") + part.slice(1))
      .join(" ");
  }

  return raw;
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
    "deedStatus",
    "creditSuitability",
    "inSite",
    "usageStatus",
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
