const { request } = require("undici");
const { Firestore } = require("@google-cloud/firestore");
const crypto = require("crypto");

const TTL = Number(process.env.GEOCODE_TTL_SECONDS || 30 * 24 * 3600);
const CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress";

// In-memory cache — fastest layer, lives for process lifetime
const cache = new Map();

// Firestore — persistent layer, survives restarts
// Set FIRESTORE_DISABLED=true in .env to skip for local dev
let db = null;
let firestoreDisabled = process.env.FIRESTORE_DISABLED === "true";

function getDb() {
  if (firestoreDisabled) return null;
  if (!db) {
    try {
      db = new Firestore();
    } catch {
      firestoreDisabled = true;
    }
  }
  return db;
}

function docId(normalizedAddr) {
  return crypto.createHash("sha256").update(normalizedAddr).digest("hex");
}

async function readFromFirestore(q, now) {
  const fs = getDb();
  if (!fs) return null;
  try {
    const doc = await fs.collection("geocode_cache").doc(docId(q)).get();
    if (!doc.exists) return null;
    const d = doc.data();
    if (d.expiresAt <= now) return null;
    return { lat: d.lat, lon: d.lon, formatted: d.formatted, neighborhood: d.neighborhood ?? undefined };
  } catch {
    firestoreDisabled = true; // stop retrying if credentials fail
    return null;
  }
}

function writeToFirestore(q, data, expiresAt) {
  const fs = getDb();
  if (!fs) return;
  fs.collection("geocode_cache")
    .doc(docId(q))
    .set({ ...data, expiresAt })
    .catch(() => { firestoreDisabled = true; });
}

function normalizeAddress(addr) {
  return String(addr || "").trim().replace(/\s+/g, " ").toLowerCase();
}

// Census geocoder uses "&" for intersections; our adapters use " / "
function toCensusAddress(addr) {
  return String(addr || "").replace(/\s*\/\s*/g, " & ");
}

async function geocode(address) {
  const q = normalizeAddress(address);
  const now = Date.now();

  // 1. In-memory cache
  const cached = cache.get(q);
  if (cached && cached.expiresAt > now) return cached.data;

  // 2. Firestore cache
  const persisted = await readFromFirestore(q, now);
  if (persisted) {
    cache.set(q, { data: persisted, expiresAt: now + TTL * 1000 });
    return persisted;
  }

  // 3. US Census Geocoder (free, no API key)
  const url = new URL(CENSUS_URL);
  url.searchParams.set("address", toCensusAddress(address));
  url.searchParams.set("benchmark", "Public_AR_Current");
  url.searchParams.set("format", "json");

  const res = await request(url.toString(), { method: "GET" });
  if (res.statusCode >= 400) {
    const text = await res.body.text();
    throw new Error(`Geocode HTTP ${res.statusCode}: ${text.slice(0, 200)}`);
  }
  const body = await res.body.json();

  const hit = body?.result?.addressMatches?.[0];
  const lat = hit?.coordinates?.y;
  const lon = hit?.coordinates?.x;
  const formatted = hit?.matchedAddress;

  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    throw new Error("Geocode: no results");
  }

  const data = { lat, lon, formatted };
  const expiresAt = now + TTL * 1000;

  cache.set(q, { data, expiresAt });
  writeToFirestore(q, data, expiresAt);

  return data;
}

module.exports = { geocode, normalizeAddress };
