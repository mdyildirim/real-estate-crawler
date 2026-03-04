#!/usr/bin/env node
"use strict";

const fs = require("fs/promises");
const path = require("path");

async function main() {
  const payloadPath = process.argv[2] || "output/latest-results.json";
  const ingestUrl = process.env.CF_INGEST_URL;
  const token = process.env.CF_INGEST_TOKEN || "";

  if (!ingestUrl) {
    throw new Error("Missing CF_INGEST_URL env var.");
  }

  const absPath = path.resolve(payloadPath);
  const payload = JSON.parse(await fs.readFile(absPath, "utf8"));

  const headers = {
    "content-type": "application/json"
  };
  if (token) {
    headers.authorization = `Bearer ${token}`;
  }

  const res = await fetch(ingestUrl, {
    method: "POST",
    headers,
    body: JSON.stringify(payload)
  });
  const text = await res.text();

  if (!res.ok) {
    throw new Error(`Ingest failed (${res.status}): ${text}`);
  }

  console.log(`Ingested successfully to ${ingestUrl}`);
  console.log(text);
}

main().catch((error) => {
  console.error(error.message || String(error));
  process.exitCode = 1;
});
