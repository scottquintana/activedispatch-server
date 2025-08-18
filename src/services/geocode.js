const { request } = require("undici");

const KEY = process.env.OPENCAGE_KEY;
const TTL = Number(process.env.GEOCODE_TTL_SECONDS || 30 * 24 * 3600);

// very simple in-memory cache (you can replace with Redis later)
const cache = new Map();

function normalizeAddress(addr) {
  return String(addr || "").trim().replace(/\s+/g, " ").toLowerCase();
}

async function geocode(address) {
  if (!KEY) throw new Error("OPENCAGE_KEY is not set");
  const q = normalizeAddress(address);
  const now = Date.now();

  const cached = cache.get(q);
  if (cached && cached.expiresAt > now) return cached.data;

  const url = new URL("https://api.opencagedata.com/geocode/v1/json");
  url.searchParams.set("q", address);
  url.searchParams.set("key", KEY);
  url.searchParams.set("limit", "1");
  url.searchParams.set("no_annotations", "1");

  const res = await request(url.toString(), { method: "GET" });
  if (res.statusCode >= 400) {
    const text = await res.body.text();
    throw new Error(`Geocode HTTP ${res.statusCode}: ${text.slice(0, 200)}`);
  }
  const body = await res.body.json();

  const hit = body?.results?.[0];
  const lat = hit?.geometry?.lat;
  const lon = hit?.geometry?.lng;
  const formatted = hit?.formatted;

  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    throw new Error("Geocode: no results");
  }

  const data = { lat, lon, formatted };
  cache.set(q, { data, expiresAt: now + TTL * 1000 });
  return data;
}

module.exports = { geocode, normalizeAddress };
