const { request } = require("undici");
const { geocode, normalizeAddress } = require("../services/geocode");

// Helpers

function normalizeIntersection(s) {
  let t = String(s || "").trim();
  // Backslashes -> slash, normalize spacing around slash, collapse spaces
  t = t.replace(/\\+/g, "/").replace(/\s*\/\s*/g, " / ").replace(/\s{2,}/g, " ");
  return t;
}
function titleCase(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/\b([a-z])([a-z0-9']*)/g, (_, a, rest) => a.toUpperCase() + rest);
}

// Pick a display city. If the feed gives a directional precinct, fall back to Nashville.
const PRECINCT_OR_DIRECTION = new Set([
  "EAST", "WEST", "NORTH", "SOUTH", "CENTRAL", "MIDTOWN", "DOWNTOWN",
]);
function pickDisplayCity(cityName) {
  const raw = (cityName || "").toString().trim();
  if (!raw) return "Nashville";
  const upper = raw.toUpperCase();
  if (PRECINCT_OR_DIRECTION.has(upper)) return "Nashville";
  return titleCase(raw);
}

// Build the final printable address strictly from the feed + chosen city.
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

// Helper to title-case incident type names nicely
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
    .join("")
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

// Haversine distance in miles
function haversineMiles(a, b) {
  if (
    !a ||
    !b ||
    !Number.isFinite(a.lat) ||
    !Number.isFinite(a.lon) ||
    !Number.isFinite(b.lat) ||
    !Number.isFinite(b.lon)
  )
    return Infinity;

  const R = 3958.7613; // Earth radius in miles
  const dLat = ((b.lat - a.lat) * Math.PI) / 180;
  const dLon = ((b.lon - a.lon) * Math.PI) / 180;
  const lat1 = (a.lat * Math.PI) / 180;
  const lat2 = (b.lat * Math.PI) / 180;

  const sinDLat = Math.sin(dLat / 2);
  const sinDLon = Math.sin(dLon / 2);
  const h =
    sinDLat * sinDLat +
    Math.cos(lat1) * Math.cos(lat2) * sinDLon * sinDLon;

  const c = 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
  return R * c;
}

// Downtown Nashville reference (rough center)
const NASHVILLE_CENTER = { lat: 36.1627, lon: -86.7816 };
const MAX_MILES = 40; // Keep an eye on this, in case we need to expand.

// Build a fallback address string by forcing ", Nashville, TN" at the end
function forceNashville(address) {
  const s = String(address || "");
  // Replace trailing ", <something>, TN" with ", Nashville, TN"; otherwise append.
  if (/, [^,]+, TN$/i.test(s)) {
    return s.replace(/, [^,]+, TN$/i, ", Nashville, TN");
  }
  if (/TN$/i.test(s)) return s; // already TN, keep it
  return `${s}, Nashville, TN`;
}

// Concurrency helper
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

    // Pre-map with props carried through
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
        displayAddress, // ALWAYS returned as the output address
        updatedAt: extractUpdatedAt(props),
        callTimeReceived: extractCallTimeReceived(props),
      };
    });

    // Dedupe addresses to minimize geocoding (for coords only)
    const uniqueAddrMap = new Map();
    for (const row of prelim) {
      if (!row.displayAddress) continue;
      const norm = normalizeAddress(row.displayAddress);
      if (norm && !uniqueAddrMap.has(norm)) uniqueAddrMap.set(norm, row.displayAddress);
    }
    const uniqueNormKeys = Array.from(uniqueAddrMap.keys());

    // Geocode with distance sanity check (retry forced Nashville if > 40 miles)
    const geocodeResults = new Map();
    await mapWithConcurrency(uniqueNormKeys, 5, async (norm) => {
      const original = uniqueAddrMap.get(norm);
      try {
        let g = await geocode(original);
        // If geocode landed too far, try again with ", Nashville, TN"
        if (
          g &&
          Number.isFinite(g.lat) &&
          Number.isFinite(g.lon) &&
          haversineMiles({ lat: g.lat, lon: g.lon }, NASHVILLE_CENTER) > MAX_MILES
        ) {
          const forced = forceNashville(original);
          const g2 = await geocode(forced);
          if (
            g2 &&
            Number.isFinite(g2.lat) &&
            Number.isFinite(g2.lon) &&
            haversineMiles({ lat: g2.lat, lon: g2.lon }, NASHVILLE_CENTER) <= MAX_MILES
          ) {
            g = g2; // prefer the forced-Nashville result if it's within the radius
          }
        }
        geocodeResults.set(norm, g);
      } catch {
        // ignore failures; leave missing
      }
    });

    // Build final places
    const places = [];
    for (const r of prelim) {
      const norm = r.displayAddress ? normalizeAddress(r.displayAddress) : null;
      const g = norm ? geocodeResults.get(norm) : null;
      if (!g || !Number.isFinite(g.lat) || !Number.isFinite(g.lon)) continue;

      places.push({
        id: r.id,
        name: String(r.name),
        // optional at top-level; move to extras if we want a stricter root
        category: r.category ? String(r.category) : undefined,
        lat: Number(g.lat),
        lon: Number(g.lon),
        // Always use our formatted original + chosen city, never geocoder's formatted
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