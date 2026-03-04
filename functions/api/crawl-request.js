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

  if (!githubToken || !owner || !repo) {
    return json(
      {
        ok: false,
        error: "Missing GitHub configuration env vars. Required: GITHUB_TOKEN, GITHUB_OWNER, GITHUB_REPO."
      },
      500
    );
  }

  const city = sanitizeInput(body.city, "Istanbul", 80);
  const district = sanitizeInput(body.district, "Atasehir", 80);
  const sources = sanitizeInput(body.sources, "all", 240);

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
