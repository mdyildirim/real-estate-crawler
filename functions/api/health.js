function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store"
    }
  });
}

export async function onRequestGet(context) {
  const hasDb = Boolean(context.env?.DB);
  return json({
    ok: true,
    service: "real-estate-crawler",
    dbBinding: hasDb ? "present" : "missing",
    now: new Date().toISOString()
  });
}
