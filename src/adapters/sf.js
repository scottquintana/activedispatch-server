// src/adapters/sf.js
const { request } = require("undici");
const { geocode, normalizeAddress } = require("../services/geocode");

/**
 * San Francisco – Law Enforcement Dispatched Calls for Service (Real-Time)
 * Default JSON: https://data.sfgov.org/resource/gnap-fj3t.json
 *
 * ENV:
 *   SF_DATASET_URL       optional override of dataset URL
 *   SF_SODA_APP_TOKEN    optional X-App-Token for higher rate limits
 *   SF_LIMIT             optional page size (default 1000)
 *   SF_MIN_LAT           bbox (default 37.55)
 *   SF_MAX_LAT           bbox (default 37.95)
 *   SF_MIN_LON           bbox (default -122.60)
 *   SF_MAX_LON           bbox (default -122.20)
 *   SF_BBOX_ENFORCE      "true"/"false" (default "true")
 *   LOG_LEVEL            set "debug" to print diagnostics
 */

const DATASET = process.env.SF_DATASET_URL || "https://data.sfgov.org/resource/gnap-fj3t.json";
const LIMIT = Number(process.env.SF_LIMIT || 1000);

// BBox (tunable/disable-able)
const SF_BBOX = {
  minLat: Number(process.env.SF_MIN_LAT ?? 37.55),
  maxLat: Number(process.env.SF_MAX_LAT ?? 37.95),
  minLon: Number(process.env.SF_MIN_LON ?? -122.60),
  maxLon: Number(process.env.SF_MAX_LON ?? -122.20),
};
const ENFORCE_BBOX = String(process.env.SF_BBOX_ENFORCE ?? "true") === "true";

function pick(v, ...keys) {
  for (const k of keys) if (v && v[k] != null) return v[k];
  return undefined;
}
function toISO(ts) {
  if (!ts) return undefined;
  const d = new Date(ts);
  return isNaN(d.getTime()) ? undefined : d.toISOString();
}
function s(v) {
  return (v ?? "").toString().trim();
}
function inSF(lat, lon) {
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  if (!ENFORCE_BBOX) return true;
  return (
    lat >= SF_BBOX.minLat && lat <= SF_BBOX.maxLat &&
    lon >= SF_BBOX.minLon && lon <= SF_BBOX.maxLon
  );
}

// STRICT coordinate extraction (only known shapes)
function extractCoordsStrict(row) {
  // 1) Top-level
  let lat = Number(row.latitude);
  let lon = Number(row.longitude);
  if (inSF(lat, lon)) return { lat, lon };

  // 2) Socrata location variants
  const loc = row.location || row.point || row.location_1 || row.geom || row.geometry;
  if (loc && typeof loc === "object") {
    // 2a) { latitude, longitude }
    const lat2 = Number(loc.latitude);
    const lon2 = Number(loc.longitude);
    if (inSF(lat2, lon2)) return { lat: lat2, lon: lon2 };

    // 2b) GeoJSON [lon, lat]
    if (Array.isArray(loc.coordinates) && loc.coordinates.length >= 2) {
      const lon3 = Number(loc.coordinates[0]);
      const lat3 = Number(loc.coordinates[1]);
      if (inSF(lat3, lon3)) return { lat: lat3, lon: lon3 };
    }
  }
  return { lat: undefined, lon: undefined };
}

function buildAddressFromHumanAddress(ha) {
  try {
    const obj = typeof ha === "string" ? JSON.parse(ha) : ha;
    const parts = [obj?.address, obj?.city, obj?.state, obj?.zip].filter(Boolean);
    return parts.join(", ");
  } catch {
    return undefined;
  }
}

// Heuristic: looks like a street/intersection?
function looksStreetSpecific(str = "") {
  const t = String(str).toLowerCase();
  return (
    /\d+\s+[a-z]/i.test(t) || // "123 Main"
    t.includes("&") ||
    t.includes(" / ") ||
    / block\b/i.test(t) ||
    /\b(st|street|ave|avenue|blvd|dr|rd|ln|terr|pl|ct)\b/i.test(t)
  );
}

function withCityState(addr) {
  if (!addr) return "San Francisco, CA";
  const clean = addr.replace(/\s+,/g, ",").replace(/,\s+,/g, ",").trim();
  const lower = clean.toLowerCase();
  if (lower.includes("san francisco") || lower.includes(" ca") || lower.includes("california")) return clean;
  return `${clean}, San Francisco, CA`;
}

// Prefer dataset street fields (no city/state here)
function bestStreetFromDataset(r) {
  return (
    s(pick(r, "address")) ||
    s(pick(r, "intersection")) ||
    (r.location && typeof r.location === "object" ? s(buildAddressFromHumanAddress(r.location.human_address)) : "") ||
    s(pick(r, "location_text")) ||
    ""
  );
}

// Best geocode input (ensures a fallback so we always try)
function bestAddressForGeocode(r) {
  return bestStreetFromDataset(r) || s(pick(r, "neighborhood_district")) || "San Francisco City Hall";
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

module.exports = {
  name: "sf",

  async fetchCity() {
    // Build SODA query
    const url = new URL(DATASET);
    url.searchParams.set("$order", "received_datetime DESC");
    url.searchParams.set("$limit", String(LIMIT));

    const headers = {};
    if (process.env.SF_SODA_APP_TOKEN) headers["X-App-Token"] = process.env.SF_SODA_APP_TOKEN;

    const res = await request(url.toString(), { method: "GET", headers });
    if (res.statusCode >= 400) {
      const txt = await res.body.text();
      throw new Error(`SF adapter HTTP ${res.statusCode}: ${txt.slice(0, 200)}`);
    }

    // Read once as text, then parse
    const text = await res.body.text();
    let rows;
    try {
      rows = JSON.parse(text);
    } catch {
      if (process.env.LOG_LEVEL === "debug") {
        console.log("SF non-JSON head:", text.slice(0, 400));
      }
      return { city: "sf", source: "sf", fetchedAt: new Date().toISOString(), places: [] };
    }

    if (!Array.isArray(rows) || rows.length === 0) {
      if (process.env.LOG_LEVEL === "debug") console.log("SF: empty rows array");
      return { city: "sf", source: "sf", fetchedAt: new Date().toISOString(), places: [] };
    }

    if (process.env.LOG_LEVEL === "debug") {
      console.log("SF sample row keys:", Object.keys(rows[0]));
    }

    // Prelim pass
    const prelim = rows.map((r) => {
      const { lat, lon } = extractCoordsStrict(r);

      const streetFromDataset = bestStreetFromDataset(r);
      const geocodeQuery = bestAddressForGeocode(r);

      // Prefer final desc → original desc → others
      const incidentTypeName = s(
        pick(
          r,
          "call_type_final_desc",
          "call_type_original_desc",
          "call_type",
          "incident_type_description",
          "description",
          "problem_type"
        )
      );

      const updatedAt = toISO(
        pick(r, "received_datetime", "entry_datetime", "updated_datetime", "dispatch_datetime")
      );

      const cad = s(pick(r, "cad_number", "cadnumber", "event_number"));
      const id = cad || `${incidentTypeName || "Incident"}-${updatedAt || ""}`;

      return {
        raw: r,
        id,
        name: incidentTypeName || "Incident",
        lat,
        lon,
        streetFromDataset,
        geocodeQuery,
        updatedAt,
        extras: {
          cadNumber: cad || undefined,
          priority: pick(r, "priority", "priority_level"),
          incidentTypeName: incidentTypeName || undefined,
          callTypeFinal: pick(r, "call_type_final_desc"),
          callTypeOriginal: pick(r, "call_type_original_desc"),
          callTypeOriginalCode: pick(r, "call_type_original"),
          disposition: pick(r, "call_disposition"),
          neighborhood: pick(r, "neighborhood_district"),
          supervisorDistrict: pick(r, "supervisor_district"),
          source: pick(r, "source", "agency"),
        },
      };
    });

    // Dedup + geocode any entries missing coords but having any address candidate
    const needGeo = prelim.filter(p => !(Number.isFinite(p.lat) && Number.isFinite(p.lon)) && p.geocodeQuery);
    const uniqueMap = new Map();
    for (const p of needGeo) {
      const query = withCityState(p.geocodeQuery);
      const norm = normalizeAddress(query);
      if (norm && !uniqueMap.has(norm)) uniqueMap.set(norm, query);
    }
    const uniqueKeys = Array.from(uniqueMap.keys());

    const geoResults = new Map();
    await mapWithConcurrency(uniqueKeys, 5, async (norm) => {
      const original = uniqueMap.get(norm);
      try {
        const g = await geocode(original);
        // only accept points that land inside (or bbox disabled)
        if (inSF(Number(g.lat), Number(g.lon))) {
          geoResults.set(norm, g);
        }
      } catch { /* ignore */ }
    });

    // Build final places
    const places = [];
    for (const p of prelim) {
      let lat = p.lat, lon = p.lon;

      if (!(Number.isFinite(lat) && Number.isFinite(lon)) && p.geocodeQuery) {
        const g = geoResults.get(normalizeAddress(withCityState(p.geocodeQuery)));
        if (g) { lat = Number(g.lat); lon = Number(g.lon); }
      }

      if (!inSF(lat, lon)) continue;

      // Choose display address
      let display = s(p.streetFromDataset);
      if (!display) {
        const g = geoResults.get(normalizeAddress(withCityState(p.geocodeQuery || "")));
        const formatted = g?.formatted;
        display = looksStreetSpecific(formatted) ? formatted : "";
      }
      display = withCityState(display);

      places.push({
        id: p.id,
        name: p.name,
        category: undefined,
        lat,
        lon,
        address: display,
        updatedAt: p.updatedAt,
        extras: p.extras,
      });
    }

    // Debug counters
    if (process.env.LOG_LEVEL === "debug") {
      const total = rows.length;
      const withCoordsStrict = rows.filter(r => {
        const { lat, lon } = extractCoordsStrict(r);
        return Number.isFinite(lat) && Number.isFinite(lon);
      }).length;

      const prelimCount = prelim.length;
      const needGeoCount = needGeo.length;

      let geocodedCount = 0;
      for (const k of geoResults.keys()) geocodedCount++;

      const inBoxAfter = prelim.filter(p => {
        let lt = p.lat, ln = p.lon;
        if (!(Number.isFinite(lt) && Number.isFinite(ln)) && p.geocodeQuery) {
          const g = geoResults.get(normalizeAddress(withCityState(p.geocodeQuery)));
          if (g) { lt = Number(g.lat); ln = Number(g.lon); }
        }
        return inSF(lt, ln);
      }).length;

      console.log("[SF] totals:", {
        totalRows: total,
        strictCoords: withCoordsStrict,
        prelim: prelimCount,
        needGeocode: needGeoCount,
        geocodedOK: geocodedCount,
        inBoxAfter,
        finalPlaces: places.length,
        bbox: SF_BBOX,
        enforceBBox: ENFORCE_BBOX
      });
    }

    return {
      city: "sf",
      source: "sf",
      fetchedAt: new Date().toISOString(),
      places,
    };
  },
};
