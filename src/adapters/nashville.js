// src/adapters/nashville.js
const { request } = require("undici");
const { geocode, normalizeAddress } = require("../services/geocode");

/* -------------------- helpers -------------------- */

// Normalize intersections and title-case a street string.
function normalizeIntersection(s) {
  let t = String(s || "").trim();
  // Backslashes -> slash; normalize spacing around slash; collapse spaces
  t = t.replace(/\\+/g, "/").replace(/\s*\/\s*/g, " / ").replace(/\s{2,}/g, " ");
  return t;
}
function titleCase(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/\b([a-z])([a-z0-9']*)/g, (_, a, rest) => a.toUpperCase() + rest);
}

// Pick a display city: if the feed gives a directional precinct, fall back to Nashville.
const PRECINCT_OR_DIRECTION = new Set([
  "EAST", "WEST", "NORTH", "SOUTH", "CENTRAL", "MIDTOWN", "DOWNTOWN"
]);
function pickDisplayCity(cityName) {
  const raw = (cityName || "").toString().trim();
  if (!raw) return "Nashville";
  const upper = raw.toUpperCase();
  if (PRECINCT_OR_DIRECTION.has(upper)) return "Nashville";
  return titleCase(raw);
}

// Build the final printable address strictly from the feed + chosen city (never the geocoder fmt).
function buildDisplayAddress(rawStreet, cityName) {
  const street = titleCase(normalizeIntersection(rawStreet || ""));
  const city = pickDisplayCity(cityName);
  if (!street) return `${city}, TN`;
  return `${street}, ${city}, TN`;
}

function extractAddress(props = {}) {
  return (
    props.Address ??
    props.ADDRESS ??
    props.Location ??
    props.LOCATION ??
    props.addr_full ??
    props.Street ??
    props.STREET ??
    null
  );
}

function extractId(props = {}) {
  return String(
    props.GlobalID ??
      props.GLOBALID ??
      props.OBJECTID ??
      props.objectid ??
      props.IncidentNumber ??
      props.Incident_No ??
      props.CallID ??
      props.id ??
      Math.random().toString(36).slice(2)
  );
}

function extractCategory(props = {}) {
  return (
    props.IncidentDescription ??
    props.CALL_TYPE ??
    props.Event_Type ??
    props.CallType ??
    props.Description ??
    props.TYPE ??
    undefined
  );
}

function extractName(props = {}, fallbackCat) {
  return props.Headline ?? props.Title ?? fallbackCat ?? "Incident";
}

// helper to title-case incident type names nicely
function formatIncidentTypeName(raw) {
  if (!raw) return undefined;
  return String(raw)
    .toLowerCase()
    // split on space, hyphen, or slash but keep the delimiters
    .split(/([\/\- ]+)/)
    .map((part) => {
      if (/^[\/\- ]+$/.test(part)) return part; // keep delimiters as-is
      return part.charAt(0).toUpperCase() + part.slice(1);
    })
    .join('')
    .trim();
}

function toISO(ts) {
  if (ts == null) return undefined;
  if (typeof ts === "number") return new Date(ts).toISOString(); // ArcGIS often ms epoch
  if (typeof ts === "string") {
    const d = new Date(ts);
    return isNaN(d.getTime()) ? undefined : d.toISOString();
  }
  return undefined;
}

function extractUpdatedAt(props = {}) {
  const tsRaw = props.LastUpdated ?? props.LastUpdate ?? props.UpdatedAt;
  return toISO(tsRaw);
}

function extractCallTimeReceived(props = {}) {
  const tsRaw =
    props.CallReceivedTime ?? props.call_received ?? props.Call_Received ?? props.datetime;
  return toISO(tsRaw);
}

// simple concurrency helper
async function mapWithConcurrency(items, limit, worker) {
  const results = new Array(items.length);
  let i = 0;
  async function run() {
    while (i < items.length) {
      const idx = i++;
      results[idx] = await worker(items[idx], idx);
    }
  }
  const runners = Array.from({ length: Math.min(limit, items.length) }, run);
  await Promise.all(runners);
  return results;
}

/* -------------------- adapter -------------------- */

module.exports = {
  name: "nashvilleMNPD",

  async fetchCity(city) {
    const url = process.env.NASHVILLE_URL;
    if (!url) throw new Error("NASHVILLE_URL is not set");

    const res = await request(url, { method: "GET" });
    if (res.statusCode >= 400) {
      const text = await res.body.text();
      throw new Error(`Nashville adapter HTTP ${res.statusCode}: ${text.slice(0, 200)}`);
    }
    const geojson = await res.body.json();
    const features = Array.isArray(geojson?.features) ? geojson.features : [];

    // Step 1: pre-map with props carried through
    const prelim = features.map((f) => {
      const props = f?.properties || {};
      const baseAddr = extractAddress(props);
      const cityName = props.CityName || props.city || props.CITYNAME;
      const displayAddress = buildDisplayAddress(baseAddr, cityName);

      return {
        props,
        id: extractId(props),
        category: extractCategory(props),
        name: extractName(props, extractCategory(props)),
        displayAddress, // <- we will ALWAYS return this as the output address
        updatedAt: extractUpdatedAt(props),
        callTimeReceived: extractCallTimeReceived(props),
      };
    });

    // Step 2: dedupe addresses to minimize geocoding (for coords only)
    const uniqueAddrMap = new Map();
    for (const row of prelim) {
      if (!row.displayAddress) continue;
      const norm = normalizeAddress(row.displayAddress);
      if (norm && !uniqueAddrMap.has(norm)) uniqueAddrMap.set(norm, row.displayAddress);
    }
    const uniqueNormKeys = Array.from(uniqueAddrMap.keys());

    // Step 3: geocode uniques (coords only; ignore geocoder's formatted address)
    const geocodeResults = new Map();
    await mapWithConcurrency(uniqueNormKeys, 5, async (norm) => {
      const original = uniqueAddrMap.get(norm);
      try {
        const g = await geocode(original);
        geocodeResults.set(norm, g);
      } catch {
        // ignore failures
      }
    });

    // Step 4: build final places
    const places = [];
    for (const r of prelim) {
      const norm = r.displayAddress ? normalizeAddress(r.displayAddress) : null;
      const g = norm ? geocodeResults.get(norm) : null;
      if (!g || !Number.isFinite(g.lat) || !Number.isFinite(g.lon)) continue;

      places.push({
        id: r.id,
        name: String(r.name),
        // keep category optional at top-level or move to extras if you prefer
        category: r.category ? String(r.category) : undefined,
        lat: Number(g.lat),
        lon: Number(g.lon),
        // <-- Always use our formatted original + chosen city, never geocoder's formatted
        address: r.displayAddress,
        callTimeReceived: r.callTimeReceived,
        updatedAt: r.updatedAt,
        extras: {
          incidentTypeCode: r.props.IncidentTypeCode,
          incidentTypeName: formatIncidentTypeName(r.props.IncidentTypeName),
        },
      });
    }

    return {
      city: String(city || "nashville").toLowerCase(),
      source: "nashvilleMNPD",
      fetchedAt: new Date().toISOString(),
      places,
    };
  },
};