function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store"
    }
  });
}

async function parseBody(request) {
  try {
    return await request.json();
  } catch {
    return null;
  }
}

function pick(value, fallback) {
  const v = String(value || "").trim();
  return v || fallback;
}

function sanitizeInput(value, fallback, maxLen = 120) {
  return pick(value, fallback).slice(0, maxLen);
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

function safeJson(value) {
  try {
    return JSON.stringify(value);
  } catch {
    return JSON.stringify({ note: "metadata serialization failed" });
  }
}

function normalizeError(error) {
  if (!error) {
    return "Unknown error";
  }
  if (typeof error === "string") {
    return error;
  }
  return String(error.message || error);
}

async function writeSystemLog(DB, entry) {
  if (!DB) {
    return;
  }
  const level = String(entry?.level || "info");
  const eventType = String(entry?.eventType || "system");
  const message = String(entry?.message || "");
  const metadataJson = entry?.metadata == null ? null : safeJson(entry.metadata);
  const runTag = entry?.runTag ? String(entry.runTag) : null;
  const runId = entry?.runId == null ? null : Number(entry.runId);
  const areaCountry = entry?.areaCountry ? canonicalCountryCode(entry.areaCountry) : null;
  const areaCity = entry?.areaCity ? String(entry.areaCity) : null;
  const areaDistrict = entry?.areaDistrict ? String(entry.areaDistrict) : null;
  const source = entry?.source ? String(entry.source) : null;

  console.log(`[${level}] ${eventType}: ${message}`, entry?.metadata || {});

  try {
    await DB.prepare(
      `
        INSERT INTO system_logs (
          level, event_type, run_tag, run_id, area_country, area_city, area_district, source, message, metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    )
      .bind(level, eventType, runTag, runId, areaCountry, areaCity, areaDistrict, source, message, metadataJson)
      .run();
  } catch (error) {
    console.log("Failed to write system log row:", String(error?.message || error));
  }
}

export async function onRequestPost(context) {
  const body = await parseBody(context.request);
  if (!body || typeof body !== "object") {
    return json({ ok: false, error: "Invalid JSON body." }, 400);
  }
  return handleDispatch(context, body);
}

async function handleDispatch(context, body) {
  const requiredToken = context.env?.CRAWL_REQUEST_TOKEN;
  if (requiredToken) {
    const auth = context.request.headers.get("authorization") || "";
    const headerToken = context.request.headers.get("x-crawl-token") || "";
    const supplied = auth.startsWith("Bearer ") ? auth.slice("Bearer ".length) : headerToken;
    if (supplied !== requiredToken) {
      return json({ ok: false, error: "Unauthorized." }, 401);
    }
  }

  const githubToken = context.env?.GITHUB_TOKEN;
  const owner = context.env?.GITHUB_OWNER;
  const repo = context.env?.GITHUB_REPO;
  const workflowFile = context.env?.GITHUB_WORKFLOW_FILE || "crawl.yml";
  const ref = context.env?.GITHUB_REF || "main";
  const DB = context.env?.DB;

  const missing = [];
  if (!githubToken) {
    missing.push("GITHUB_TOKEN");
  }
  if (!owner) {
    missing.push("GITHUB_OWNER");
  }
  if (!repo) {
    missing.push("GITHUB_REPO");
  }

  if (missing.length > 0) {
    return json(
      {
        ok: false,
        error: `Missing GitHub configuration env vars: ${missing.join(", ")}`
      },
      500
    );
  }

  const country = canonicalCountryCode(sanitizeInput(body.country, "TR", 8), "TR");
  const defaults = areaDefaultsForCountry(country);
  const city = canonicalAreaName(sanitizeInput(body.city, defaults.city, 80), defaults.city);
  const district = canonicalAreaName(sanitizeInput(body.district, defaults.district, 80), defaults.district);
  const citySlug = sanitizeInput(body.citySlug || body.city_slug, "", 120);
  const districtSlug = sanitizeInput(body.districtSlug || body.district_slug, "", 120);
  const sources = sanitizeInput(body.sources, "all", 240);

  await writeSystemLog(DB, {
    level: "info",
    eventType: "dispatch.requested",
    areaCountry: country,
    areaCity: city,
    areaDistrict: district,
    message: "Crawl workflow dispatch requested.",
    metadata: { sources, workflowFile, ref, country, citySlug, districtSlug }
  });

  let ghRes;
  try {
    ghRes = await fetch(
      `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflowFile}/dispatches`,
      {
        method: "POST",
        headers: {
          authorization: `Bearer ${githubToken}`,
          accept: "application/vnd.github+json",
          "content-type": "application/json",
          "user-agent": "real-estate-crawler-cloudflare-pages"
        },
        body: JSON.stringify({
          ref,
          inputs: {
            country,
            city,
            district,
            city_slug: citySlug,
            district_slug: districtSlug,
            sources
          }
        })
      }
    );
  } catch (error) {
    const details = normalizeError(error);
    await writeSystemLog(DB, {
      level: "error",
      eventType: "dispatch.failed",
      areaCountry: country,
      areaCity: city,
      areaDistrict: district,
      message: "GitHub workflow dispatch request failed before response.",
      metadata: { details }
    });
    return json(
      {
        ok: false,
        error: "GitHub workflow dispatch request failed.",
        details
      },
      502
    );
  }

  if (!ghRes.ok) {
    const txt = await ghRes.text();
    await writeSystemLog(DB, {
      level: "error",
      eventType: "dispatch.failed",
      areaCountry: country,
      areaCity: city,
      areaDistrict: district,
      message: "GitHub workflow dispatch failed.",
      metadata: {
        status: ghRes.status,
        details: txt.slice(0, 1000)
      }
    });
    return json(
      {
        ok: false,
        error: "GitHub workflow dispatch failed.",
        status: ghRes.status,
        details: txt
      },
      502
    );
  }

  await writeSystemLog(DB, {
    level: "info",
    eventType: "dispatch.completed",
    areaCountry: country,
    areaCity: city,
    areaDistrict: district,
    message: "GitHub workflow dispatched successfully.",
    metadata: { sources, workflowFile, ref, country, citySlug, districtSlug }
  });

  return json({
    ok: true,
    message: "Workflow dispatched.",
    workflowFile,
    ref,
    inputs: { country, city, district, city_slug: citySlug, district_slug: districtSlug, sources }
  });
}

export async function onRequestGet(context) {
  const url = new URL(context.request.url);
  const p = url.searchParams;
  const dispatchHint = String(p.get("dispatch") || "")
    .trim()
    .toLowerCase();
  const wantsDispatch =
    dispatchHint === "1" ||
    dispatchHint === "true" ||
    dispatchHint === "yes" ||
    p.has("country") ||
    p.has("city") ||
    p.has("district");

  if (wantsDispatch) {
    return handleDispatch(context, {
      country: p.get("country") || "",
      city: p.get("city") || "",
      district: p.get("district") || "",
      city_slug: p.get("city_slug") || p.get("citySlug") || "",
      district_slug: p.get("district_slug") || p.get("districtSlug") || "",
      sources: p.get("sources") || "all"
    });
  }

  return json({
    ok: true,
    endpoint: "/api/crawl-request",
    method: "POST",
    body: {
      country: "TR",
      city: "Istanbul",
      district: "Atasehir",
      city_slug: "",
      district_slug: "",
      sources: "all"
    }
  });
}
