const { request } = require("undici");
const { XMLParser } = require("fast-xml-parser");
const cheerio = require("cheerio");
const { geocode, normalizeAddress } = require("../services/geocode");

// Helpers
function withCityState(address) {
  if (!address) return "Portland, OR";
  const lower = String(address).toLowerCase();
  if (lower.includes("portland") || lower.includes(" or") || lower.includes("oregon")) return address;
  return `${address}, Portland, OR`;
}

function safeISO(ts) {
  if (!ts) return undefined;
  if (typeof ts === "number") return new Date(ts).toISOString();
  const d = new Date(ts);
  return isNaN(d.getTime()) ? undefined : d.toISOString();
}

function stripHtml(s = "") {
  return String(s).replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
}

function parseKmlDescription(descRaw = "") {
    const desc = stripHtml(descRaw);
  
    // Extract the address substring after " at "
    const atIdx = desc.toLowerCase().indexOf(" at ");
    let addrPart = atIdx >= 0 ? desc.slice(atIdx + 4) : desc;
  
    // Keep only the first line (the next lines usually hold the timestamp)
    addrPart = addrPart.split("\n")[0].trim();
  
    // Remove any inline timestamp if it appears on the same line as the address
    const TS_INLINE =
      /\b(?:Sun|Mon|Tue|Tues|Wed|Thu|Thur|Thurs|Fri|Sat)(?:day)?,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}\s+(?:AM|PM)\b/i;
    addrPart = addrPart.replace(TS_INLINE, "").trim();
  
    // Extract and remove the bracketed incident id, e.g. "[Portland Police #PP25000223544]"
    let incidentId;
    addrPart = addrPart.replace(/\[(?:Portland Police|PPB|Police)[^#]*#([A-Za-z0-9-]+)\]/i, (_, id) => {
      incidentId = id;
      return "";
    }).trim();
  
    // Normalize address punctuation
    addrPart = addrPart
      .replace(/\s{2,}/g, " ")
      .replace(/\s*,\s*/g, ", ")
      .replace(/,\s*PORT(?:LAND)?\b\.?/i, "") // drop trailing ", PORT"
      .replace(/,\s*(?:,)+/g, ",")
      .replace(/^,|,$/g, "")
      .trim();
  
     // Find the timestamp anywhere in the full description and convert to ISO (Pacific time)
  const TS_ANYWHERE =
  /\b(?:Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}\s+(?:AM|PM)\b/i;

const timeMatch = desc.match(TS_ANYWHERE);
let updatedAt;

if (timeMatch) {
  const stamp = timeMatch[0]; // e.g., "Sunday, August 17, 2025 4:20 PM"
  // Try PDT first (-0700), then PST (-0800). One of these will always be correct,
  // and Date will normalize it to UTC for .toISOString().
  const tryParse = (s) => {
    const d = new Date(`${s} GMT-0700`);     // PDT
    if (!isNaN(d.getTime())) return d;
    const d2 = new Date(`${s} GMT-0800`);    // PST
    if (!isNaN(d2.getTime())) return d2;
    return undefined;
  };

  const d = tryParse(stamp);
  if (d) updatedAt = d.toISOString();
}

  
    return { cleanAddress: addrPart, incidentId, updatedAt };
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

// KML parsing (primary for PDX)
function parseKML(text) {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "",
    textNodeName: "text",
  });
  const kml = parser.parse(text);
  const doc = kml?.kml?.Document || kml?.Document || kml?.kml;
  const placemarks = Array.isArray(doc?.Placemark) ? doc.Placemark : (doc?.Placemark ? [doc.Placemark] : []);

  return placemarks.map(pm => {
    const title = pm?.name || pm?.Name || "Incident";
    const description = String(pm?.description || "");

    // Coordinates: <Point><coordinates>lon,lat[,alt]</coordinates></Point>
    const coordText = pm?.Point?.coordinates || pm?.coordinates || "";
    const [lonStr, latStr] = String(coordText).split(",").map(s => s && s.trim());
    const lat = latStr ? Number(latStr) : undefined;
    const lon = lonStr ? Number(lonStr) : undefined;

    // Incident name is before " at " in title (common pattern)
    const name = String(title).split(" at ")[0].trim();
    const { cleanAddress, incidentId, updatedAt } = parseKmlDescription(description);

    return {
      id: String(pm?.id || pm?.name || Math.random().toString(36).slice(2)),
      name: name || "Incident",
      category: undefined,
      lat: Number.isFinite(lat) ? lat : undefined,
      lon: Number.isFinite(lon) ? lon : undefined,
      address: cleanAddress || undefined,
      updatedAt,
      extras: {
        incidentTypeCode: undefined,           // Not provided by KML
        incidentTypeName: name || undefined,   // Keep name here too
        incidentId: incidentId                 // e.g., "PP25000223568"
      }
    };
  });
}

// JSON parsing (fallback)
function parseJSONVariant(json) {
  const rows =
    Array.isArray(json) ? json :
    Array.isArray(json?.incidents) ? json.incidents :
    Array.isArray(json?.features) ? json.features.map(f => ({ ...(f.properties || {}), ...f })) :
    [];

  return rows.map(r => {
    const id = String(
      r.id ?? r.objectid ?? r.OBJECTID ?? r.GlobalID ?? r.GLOBALID ?? r.callid ?? r.CallID ?? Math.random().toString(36).slice(2)
    );
    const typeName = r.IncidentTypeName ?? r.type_name ?? r.type ?? r.CallType ?? r.description ?? r.Description;
    const typeCode = r.IncidentTypeCode ?? r.type_code ?? r.code ?? r.CallTypeCode;
    const addressRaw = r.Address ?? r.address ?? r.Location ?? r.location ?? r.addr_full ?? null;

    const lat = r.lat ?? r.latitude ?? (r.geometry && r.geometry.coordinates && r.geometry.coordinates[1]);
    const lon = r.lon ?? r.longitude ?? (r.geometry && r.geometry.coordinates && r.geometry.coordinates[0]);

    const updatedAt = r.updatedAt ?? r.LastUpdate ?? r.time ?? r.datetime ?? r.LastUpdated ?? r.CreationDate;

    return {
      id,
      name: String(typeName || "Incident"),
      category: r.Category ?? r.category ?? undefined,
      lat: Number(lat),
      lon: Number(lon),
      address: addressRaw ? String(addressRaw) : null,
      updatedAt: safeISO(updatedAt),
      extras: {
        incidentTypeCode: typeCode,
        incidentTypeName: typeName,
        priority: r.Priority ?? r.priority
      },
    };
  });
}

// HTML table parsing (last resort)
function parseHTMLTable(text) {
  const $ = cheerio.load(text);
  const table = $("table").first();
  if (!table.length) return [];

  const headers = table.find("tr").first().find("th,td").map((i, el) => $(el).text().trim().toLowerCase()).get();
  const idxProblem = headers.findIndex(h => /problem|type|incident/.test(h));
  const idxAddress = headers.findIndex(h => /address|location/.test(h));
  const idxTime = headers.findIndex(h => /received|time|datetime|updated/.test(h));

  const rows = [];
  table.find("tr").slice(1).each((_, tr) => {
    const tds = $(tr).find("td");
    if (!tds.length) return;

    const get = (idx) => idx >= 0 && idx < tds.length ? $(tds[idx]).text().trim() : undefined;

    const name = get(idxProblem) || "Incident";
    const addressRaw = get(idxAddress);
    const updatedAt = safeISO(get(idxTime));

    let lat, lon;
    $(tds).find("a[href*='google.'], a[href*='maps']").each((__, a) => {
      const href = $(a).attr("href") || "";
      const qMatch = href.match(/[?&]q=(-?\d+(\.\d+)?),\s*(-?\d+(\.\d+)?)/);
      const d3Match = href.match(/!3d(-?\d+(\.\d+)?)!4d(-?\d+(\.\d+)?)/);
      if (qMatch) { lat = Number(qMatch[1]); lon = Number(qMatch[3]); }
      else if (d3Match) { lat = Number(d3Match[1]); lon = Number(d3Match[3]); }
    });

    rows.push({
      id: Math.random().toString(36).slice(2),
      name,
      category: undefined,
      lat: Number.isFinite(lat) ? lat : undefined,
      lon: Number.isFinite(lon) ? lon : undefined,
      address: addressRaw,
      updatedAt,
      extras: {
        incidentTypeCode: undefined,
        incidentTypeName: name
      }
    });
  });

  return rows;
}

// Main adapter
module.exports = {
  name: "pdx",

  async fetchCity(city) {
    const url = process.env.PORTLAND_URL;
    if (!url) throw new Error("PORTLAND_URL is not set");

    // follow redirects (some hosts 302)
    const res = await request(url, { method: "GET", maxRedirections: 5 });
    if (res.statusCode >= 400) {
      const txt = await res.body.text();
      throw new Error(`PDX adapter HTTP ${res.statusCode}: ${txt.slice(0, 200)}`);
    }

    const text = await res.body.text(); // read once

    let rows = [];
    if (/\<kml[\s>]/i.test(text)) {
      // KML (primary for PDX)
      rows = parseKML(text);
    } else {
      // Try JSON, then HTML as fallback
      try {
        const json = JSON.parse(text);
        rows = parseJSONVariant(json);
      } catch {
        if (/\<html[\s>]/i.test(text)) {
          rows = parseHTMLTable(text);
        } else {
          rows = [];
        }
      }
    }

    // Tidy display address
    for (const r of rows) {
      if (r.address) r.address = withCityState(r.address);
    }

    // Geocode only those missing coords
    const needGeo = rows.filter(r => !(Number.isFinite(r.lat) && Number.isFinite(r.lon)) && r.address);
    const uniqueMap = new Map();
    for (const r of needGeo) {
      const norm = normalizeAddress(r.address);
      if (norm && !uniqueMap.has(norm)) uniqueMap.set(norm, r.address);
    }
    const uniqueKeys = Array.from(uniqueMap.keys());

    const geoResults = new Map();
    await mapWithConcurrency(uniqueKeys, 5, async (norm) => {
      const original = uniqueMap.get(norm);
      try {
        const g = await geocode(original);
        geoResults.set(norm, g);
      } catch { /* ignore geocode errors */ }
    });

    const places = [];
    for (const r of rows) {
      let lat = r.lat, lon = r.lon, address = r.address;
      if (!(Number.isFinite(lat) && Number.isFinite(lon)) && address) {
        const g = geoResults.get(normalizeAddress(address));
        if (g) { lat = g.lat; lon = g.lon; address = g.formatted || address; }
      }
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

      places.push({
        id: r.id,
        name: r.name ? String(r.name) : "Incident",
        category: r.category ? String(r.category) : undefined,
        lat: Number(lat),
        lon: Number(lon),
        address,
        callTimeReceived: r.updatedAt,
        extras: {
          incidentTypeCode: r.extras?.incidentTypeCode,
          incidentTypeName: r.extras?.incidentTypeName || r.name,
          incidentId: r.extras?.incidentId
        }
      });
    }

    return {
      city: String(city || "portland").toLowerCase(),
      source: "pdx",
      fetchedAt: new Date().toISOString(),
      places
    };
  }
};
