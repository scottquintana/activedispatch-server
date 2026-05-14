// src/adapters/orl.js
const { request } = require("undici");
const { XMLParser } = require("fast-xml-parser");
const { geocode, normalizeAddress } = require("../services/geocode");


/** ---------- helpers ---------- */
function withCityState(address) {
  if (!address) return "Orlando, FL";
  const lower = String(address).toLowerCase();
  if (lower.includes("orlando") || lower.includes(" fl") || lower.includes("florida")) return address;
  return `${address}, Orlando, FL`;
}

// Minimal title-case for street addresses (data comes in ALL CAPS)
const DIRECTIONS = new Set(["N", "S", "E", "W", "NE", "NW", "SE", "SW"]);
const SUFFIX_MAP = new Map(Object.entries({
  "st": "St", "ave": "Ave", "blvd": "Blvd", "rd": "Rd", "dr": "Dr",
  "ct": "Ct", "ln": "Ln", "pl": "Pl", "pkwy": "Pkwy", "hwy": "Hwy",
  "way": "Way", "trl": "Trl", "ter": "Ter", "cir": "Cir",
}));

function titleCaseWord(w) {
  const up = w.toUpperCase();
  if (DIRECTIONS.has(up)) return up;
  const lo = w.toLowerCase();
  if (SUFFIX_MAP.has(lo)) return SUFFIX_MAP.get(lo);
  return lo.charAt(0).toUpperCase() + lo.slice(1);
}

function prettifyLocation(loc = "") {
  if (!loc) return "";
  // Intersections use " / "
  return loc
    .split("/")
    .map(part => part.trim().split(/\s+/).map(titleCaseWord).join(" "))
    .join(" / ");
}

// Parse "M/D/YYYY HH:MM" as Eastern time
function parseOrlandoDate(dateStr) {
  if (!dateStr) return undefined;
  const m = String(dateStr).match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{2})$/);
  if (!m) return undefined;
  const [, month, day, year, hour, minute] = m;
  const iso = `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}T${hour.padStart(2, "0")}:${minute}:00`;
  // Try EDT (-04:00) then EST (-05:00)
  const d = new Date(`${iso}-04:00`);
  return isNaN(d.getTime()) ? undefined : d.toISOString();
}

const NEIGHBORHOOD_URL = "https://ocgis4.ocfl.net/arcgis/rest/services/Public_Dynamic/MapServer/85/query";
const neighborhoodCache = new Map();

async function lookupNeighborhood(lat, lon) {
  const key = `${lat.toFixed(4)},${lon.toFixed(4)}`;
  if (neighborhoodCache.has(key)) return neighborhoodCache.get(key);

  const url = new URL(NEIGHBORHOOD_URL);
  url.searchParams.set("geometry", `${lon},${lat}`);
  url.searchParams.set("geometryType", "esriGeometryPoint");
  url.searchParams.set("spatialRel", "esriSpatialRelIntersects");
  url.searchParams.set("outFields", "NEIGHBORHOODNAME");
  url.searchParams.set("returnGeometry", "false");
  url.searchParams.set("inSR", "4326");
  url.searchParams.set("f", "json");

  try {
    const res = await request(url.toString(), { method: "GET" });
    const body = await res.body.json();
    const name = body?.features?.[0]?.attributes?.NEIGHBORHOODNAME || null;
    neighborhoodCache.set(key, name);
    return name;
  } catch {
    return null;
  }
}

async function mapWithConcurrency(items, limit, worker) {
  const results = new Array(items.length);
  let i = 0;
  async function run() {
    while (i < items.length) {
      const idx = i++;
      results[idx] = await worker(items[idx], idx);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, run));
  return results;
}

/** ---------- main adapter ---------- */
module.exports = {
  name: "orl",
  ttl: 30, // Orlando's feed refreshes every ~3s; 30s balances freshness vs. load

  async fetchCity(city, log) {
    const url = process.env.ORLANDO_URL;
    if (!url) throw new Error("ORLANDO_URL is not set");

    const res = await request(url, { method: "GET", maxRedirections: 5 });
    if (res.statusCode >= 400) {
      const txt = await res.body.text();
      throw new Error(`ORL adapter HTTP ${res.statusCode}: ${txt.slice(0, 200)}`);
    }

    const text = await res.body.text();

    const parser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: "",
      textNodeName: "text",
    });
    const parsed = parser.parse(text);
    let rawCalls = parsed?.CALLS?.CALL ?? [];
    if (!Array.isArray(rawCalls)) rawCalls = [rawCalls];

    const rows = rawCalls.map(call => ({
      id: String(call?.incident ?? Math.random().toString(36).slice(2)),
      name: String(call?.DESC || "Incident"),
      address: withCityState(prettifyLocation(String(call?.LOCATION || ""))),
      updatedAt: parseOrlandoDate(call?.DATE),
      district: String(call?.DISTRICT || "") || undefined,
    }));

    // Geocode — Orlando XML provides no coordinates
    const uniqueMap = new Map();
    for (const r of rows) {
      const norm = normalizeAddress(r.address);
      if (norm && !uniqueMap.has(norm)) uniqueMap.set(norm, r.address);
    }
    const uniqueKeys = Array.from(uniqueMap.keys());

    // Geocode then immediately chain neighborhood lookup — single concurrent pass
    const geoResults = new Map();
    let geocodeFailed = 0;
    await mapWithConcurrency(uniqueKeys, 5, async (norm) => {
      const original = uniqueMap.get(norm);
      try {
        const g = await geocode(original);
        geoResults.set(norm, g);
        await lookupNeighborhood(g.lat, g.lon);
      } catch {
        geocodeFailed++;
      }
    });

    if (geocodeFailed > 0) {
      log?.warn({ city: "orl", geocodeFailed, geocodeAttempted: uniqueKeys.length }, "geocode failures");
    }

    const places = [];
    for (const r of rows) {
      const g = geoResults.get(normalizeAddress(r.address));
      if (!g || !Number.isFinite(g.lat) || !Number.isFinite(g.lon)) continue;

      const neighborhood = await lookupNeighborhood(g.lat, g.lon) || undefined;

      places.push({
        id: r.id,
        name: r.name,
        category: undefined,
        lat: g.lat,
        lon: g.lon,
        address: g.formatted || r.address,
        callTimeReceived: r.updatedAt,
        extras: {
          incidentTypeName: r.name,
          district: r.district,
          neighborhood,
        },
      });
    }

    return {
      city: String(city || "orlando").toLowerCase(),
      source: "orl",
      fetchedAt: new Date().toISOString(),
      places,
    };
  },
};
