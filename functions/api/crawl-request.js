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

function safeJson(value) {
  try {
    return JSON.stringify(value);
  } catch {
    return JSON.stringify({ note: "metadata serialization failed" });
  }
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
  const areaCity = entry?.areaCity ? String(entry.areaCity) : null;
  const areaDistrict = entry?.areaDistrict ? String(entry.areaDistrict) : null;
  const source = entry?.source ? String(entry.source) : null;

  console.log(`[${level}] ${eventType}: ${message}`, entry?.metadata || {});

  try {
    await DB.prepare(
      `
        INSERT INTO system_logs (
          level, event_type, run_tag, run_id, area_city, area_district, source, message, metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    )
      .bind(level, eventType, runTag, runId, areaCity, areaDistrict, source, message, metadataJson)
      .run();
  } catch (error) {
    console.log("Failed to write system log row:", String(error?.message || error));
  }
}

export async function onRequestPost(context) {
  const body = await parseBody(context.request);
  if (!body) {
    return json({ ok: false, error: "Invalid JSON body." }, 400);
  }

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

  const city = canonicalAreaName(sanitizeInput(body.city, "Istanbul", 80), "Istanbul");
  const district = canonicalAreaName(sanitizeInput(body.district, "Atasehir", 80), "Atasehir");
  const sources = sanitizeInput(body.sources, "all", 240);

  await writeSystemLog(DB, {
    level: "info",
    eventType: "dispatch.requested",
    areaCity: city,
    areaDistrict: district,
    message: "Crawl workflow dispatch requested.",
    metadata: { sources, workflowFile, ref }
  });

  const ghRes = await fetch(
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
          city,
          district,
          sources
        }
      })
    }
  );

  if (!ghRes.ok) {
    const txt = await ghRes.text();
    await writeSystemLog(DB, {
      level: "error",
      eventType: "dispatch.failed",
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
    areaCity: city,
    areaDistrict: district,
    message: "GitHub workflow dispatched successfully.",
    metadata: { sources, workflowFile, ref }
  });

  return json({
    ok: true,
    message: "Workflow dispatched.",
    workflowFile,
    ref,
    inputs: { city, district, sources }
  });
}

export async function onRequestGet() {
  return json({
    ok: true,
    endpoint: "/api/crawl-request",
    method: "POST",
    body: {
      city: "Istanbul",
      district: "Atasehir",
      sources: "all"
    }
  });
}
