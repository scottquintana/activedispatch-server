// src/adapters/sf.js
const { request } = require("undici");

/**
 * San Francisco – Law Enforcement Dispatched Calls for Service (Real-Time)
 * Default JSON: https://data.sfgov.org/resource/gnap-fj3t.json
 *
 * ENV:
 *   SF_DATASET_URL       optional override of dataset URL
 *   SF_SODA_APP_TOKEN    optional X-App-Token for higher rate limits
 *   SF_LIMIT             optional page size (default 1000)
 *   LOG_LEVEL            "debug" to print diagnostics
 */

const DATASET = process.env.SF_DATASET_URL || "https://data.sfgov.org/resource/gnap-fj3t.json";
const LIMIT = Number(process.env.SF_LIMIT || 1000);

/* ------------------------------ utils ------------------------------ */
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

/* --------------------- coordinate extraction ---------------------- */
/** Return {lat, lon} from known SF fields (no bbox, no geocode). */
function extractCoords(row) {
  // 1) Top-level latitude/longitude (some Socrata tables use these)
  let lat = Number(row.latitude);
  let lon = Number(row.longitude);
  if (Number.isFinite(lat) && Number.isFinite(lon)) return { lat, lon };

  // 2) GeoJSON containers: intersection_point, location, geometry, etc.
  const candidates = [row.intersection_point, row.location, row.point, row.location_1, row.geom, row.geometry];
  for (const loc of candidates) {
    if (!loc) continue;
    // GeoJSON { type: "Point", coordinates: [lon, lat] }
    if (typeof loc === "object" && Array.isArray(loc.coordinates) && loc.coordinates.length >= 2) {
      const lon2 = Number(loc.coordinates[0]);
      const lat2 = Number(loc.coordinates[1]);
      if (Number.isFinite(lat2) && Number.isFinite(lon2)) return { lat: lat2, lon: lon2 };
    }
    // { latitude, longitude }
    const lat3 = Number(loc.latitude);
    const lon3 = Number(loc.longitude);
    if (Number.isFinite(lat3) && Number.isFinite(lon3)) return { lat: lat3, lon: lon3 };
    // WKT string "POINT (lon lat)"
    if (typeof loc === "string") {
      const m = loc.match(/POINT\s*\(\s*(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s*\)/i);
      if (m) {
        const lon4 = Number(m[1]);
        const lat4 = Number(m[2]);
        if (Number.isFinite(lat4) && Number.isFinite(lon4)) return { lat: lat4, lon: lon4 };
      }
    }
  }

  return { lat: undefined, lon: undefined };
}

/* ---------------- street/intersection formatting ------------------ */
const UPPER_DIRECTIONS = new Set(["N","S","E","W","NE","NW","SE","SW"]);
const TOKEN_MAP = new Map(Object.entries({
  "st": "St", "street": "Street",
  "ave": "Ave", "avenue": "Avenue",
  "blvd": "Blvd", "boulevard": "Boulevard",
  "rd": "Rd", "road": "Road",
  "dr": "Dr", "drive": "Drive",
  "ct": "Ct", "court": "Court",
  "ln": "Ln", "lane": "Lane",
  "ter": "Ter", "terrace": "Terrace",
  "pl": "Pl", "place": "Place",
  "pkwy": "Pkwy", "parkway": "Parkway",
  "hwy": "Hwy", "highway": "Highway",
  "way": "Way"
}));
const LOWER_SMALL = new Set(["of","and","the","at","de","la","del"]);

function capFirst(w) { return w ? w[0].toUpperCase() + w.slice(1).toLowerCase() : w; }
function titleCaseWord(w, idx) {
  const raw = w, t = w.toLowerCase();
  if (UPPER_DIRECTIONS.has(raw.toUpperCase())) return raw.toUpperCase();
  if (TOKEN_MAP.has(t)) return TOKEN_MAP.get(t);
  if (LOWER_SMALL.has(t) && idx > 0) return t;
  if (/^o'/.test(t)) return "O'" + capFirst(t.slice(2));
  if (/^mc[a-z]/.test(t)) return "Mc" + capFirst(t.slice(2));
  if (raw.includes("-")) return raw.split("-").map((seg,i)=>titleCaseWord(seg,i)).join("-");
  return capFirst(t);
}
function titleCaseStreet(str="") { return str.trim().split(/\s+/).map(titleCaseWord).join(" "); }
function normalizeIntersection(str="") {
  // Replace backslashes (single/multiple) with " / ", normalize slashes, and title-case sides
  const sep = str.replace(/\s*\\+\s*/g, " / ").replace(/\s*\/\s*/g, " / ");
  return sep.split(" / ").map(titleCaseStreet).join(" / ");
}
function prettifyStreet(street="") {
  if (!street) return "";
  return /[\\\/]/.test(street) ? normalizeIntersection(street) : titleCaseStreet(street);
}

/** Prefer SF dataset fields; do NOT append city/state here. */
function bestStreetFromDataset(r) {
  return (
    s(pick(r, "address")) ||
    s(pick(r, "intersection_name")) || // <-- SF uses this
    s(pick(r, "intersection")) ||      // fallback if present
    s(pick(r, "location_text")) ||
    ""
  );
}

function withCityState(addr) {
  if (!addr) return "San Francisco, CA";
  const clean = addr.replace(/\s+,/g, ",").replace(/,\s+,/g, ",").trim();
  const lower = clean.toLowerCase();
  if (lower.includes("san francisco") || lower.includes(" ca") || lower.includes("california")) return clean;
  return `${clean}, San Francisco, CA`;
}

/* ------------------------------ adapter --------------------------- */
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

    const text = await res.body.text();
    let rows;
    try {
      rows = JSON.parse(text);
    } catch {
      if (process.env.LOG_LEVEL === "debug") console.log("SF non-JSON head:", text.slice(0, 400));
      return { city: "sf", source: "sf", fetchedAt: new Date().toISOString(), places: [] };
    }

    if (!Array.isArray(rows) || rows.length === 0) {
      return { city: "sf", source: "sf", fetchedAt: new Date().toISOString(), places: [] };
    }

    const places = [];
    for (const r of rows) {
      const { lat, lon } = extractCoords(r);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
        if (process.env.LOG_LEVEL === "debug") console.log("SF skip: no coords for row", r.cad_number || r.id || "");
        continue; // no coords → skip
      }

      // Display address purely from dataset; pretty-print intersections
      const rawStreet = bestStreetFromDataset(r);
      const prettyStreet = prettifyStreet(rawStreet);
      const address = withCityState(prettyStreet || "San Francisco, CA");

      // Incident type naming preference
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

      const fetchedAt = toISO(
        // prefer the row's own "call_last_updated_at" if present
        pick(r, "call_last_updated_at", "received_datetime", "entry_datetime", "updated_datetime", "dispatch_datetime")
      );

      // ID: prefer CAD number, else stable-ish composite
      const cad = s(pick(r, "cad_number", "cadnumber", "event_number", "id"));
      const id =
        cad ||
        `${incidentTypeName || "Incident"}-${fetchedAt || ""}-${lat.toFixed?.(5)}${lon.toFixed?.(5)}`;

      places.push({
        id,
        name: incidentTypeName || "Incident",
        category: undefined,
        lat,
        lon,
        address,
        callTimeReceived: fetchedAt,
        extras: {
          cadNumber: cad || undefined,
          priority: pick(r, "priority_final", "priority_original", "priority", "priority_level"),
          incidentTypeName: incidentTypeName || undefined,
          callTypeFinal: pick(r, "call_type_final_desc"),
          callTypeOriginal: pick(r, "call_type_original_desc"),
          callTypeOriginalCode: pick(r, "call_type_original"),
          disposition: pick(r, "disposition", "call_disposition"),
          neighborhood: pick(r, "analysis_neighborhood", "neighborhood_district"),
          policeDistrict: pick(r, "police_district"),
          supervisorDistrict: pick(r, "supervisor_district"),
          source: pick(r, "agency", "source"),
        },
      });
    }

    if (process.env.LOG_LEVEL === "debug") {
      console.log(`[SF] rows: ${rows.length}, places: ${places.length}`);
    }

    return {
      city: "sf",
      source: "sf",
      fetchedAt: new Date().toISOString(),
      places,
    };
  },
};
