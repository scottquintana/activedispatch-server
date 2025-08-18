// src/adapters/nashville.js
const { request } = require("undici");
const { geocode, normalizeAddress } = require("../services/geocode");

// ------- helpers -------
function withCityState(address) {
  if (!address) return "Nashville, TN";
  const lower = String(address).toLowerCase();
  if (lower.includes("nashville") || lower.includes(" tn") || lower.includes("tennessee")) {
    return address;
  }
  return `${address}, Nashville, TN`;
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
  const tsRaw =
    props.LastUpdated;

  return toISO(tsRaw);
}

function extractCallTimeReceived(props = {}) {
  const tsRaw =
    props.CallReceivedTime;
  return toISO(tsRaw);
}

// concurrency helper
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

    // step 1: pre-map with props carried through
    const prelim = features.map((f) => {
      const props = f?.properties || {};
      return {
        props,
        id: extractId(props),
        category: extractCategory(props),
        name: extractName(props, extractCategory(props)),
        displayAddress: withCityState(extractAddress(props)),
        updatedAt: extractUpdatedAt(props),
        callTimeReceived: extractCallTimeReceived(props), // ← NEW
      };
    });

    // step 2: dedupe addresses
    const uniqueAddrMap = new Map();
    for (const row of prelim) {
      if (!row.displayAddress) continue;
      const norm = normalizeAddress(row.displayAddress);
      if (norm && !uniqueAddrMap.has(norm)) uniqueAddrMap.set(norm, row.displayAddress);
    }
    const uniqueNormKeys = Array.from(uniqueAddrMap.keys());

    // step 3: geocode uniques
    const geocodeResults = new Map();
    await mapWithConcurrency(uniqueNormKeys, 5, async (norm) => {
      const original = uniqueAddrMap.get(norm);
      try {
        const g = await geocode(original);
        geocodeResults.set(norm, g);
      } catch {
        // leave missing
      }
    });

    // step 4: build final places with extras
    const places = [];
    for (const r of prelim) {
      const norm = r.displayAddress ? normalizeAddress(r.displayAddress) : null;
      const g = norm ? geocodeResults.get(norm) : null;
      if (!g || !Number.isFinite(g.lat) || !Number.isFinite(g.lon)) continue;

      places.push({
        id: r.id,
        name: String(r.name),
        category: r.category ? String(r.category) : undefined,
        lat: Number(g.lat),
        lon: Number(g.lon),
        address: g.formatted || r.displayAddress,
        callTimeReceived: r.callTimeReceived,
        updatedAt: r.updatedAt,

        // preserve provider-specific fields
        extras: {
          incidentTypeCode: r.props.IncidentTypeCode,
          incidentTypeName: r.props.IncidentTypeName,
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
