# Active Dispatch (Server)

This project fetches **real-time police dispatch data** from multiple US cities, normalizes it into a consistent shape, and exposes it via a lightweight Fastify API to support our mobile apps.

Currently supported cities:
- **Nashville, TN** (Metro Nashville Police Department)
- **Orlando, FL** (Orlando Police Department)
- **Portland, OR** (Portland Police Bureau)
- **San Francisco, CA** (SFPD / SF Open Data)

---

## Project Structure

```
src/
├─ adapters/
│  ├─ nashville.js   # Metro Nashville Police active dispatch (ArcGIS GeoJSON)
│  ├─ orl.js         # Orlando Police Department (XML feed)
│  ├─ pdx.js         # Portland Police Bureau (KML feed)
│  └─ sf.js          # San Francisco incidents (Socrata JSON)
│
├─ providers/
│  └─ index.js       # Maps city slugs → adapters
│
├─ routes/
│  ├─ city.js        # GET /v1/city/:city — main endpoint + in-memory cache
│  └─ health.js      # GET /health
│
├─ services/
│  └─ geocode.js     # OpenCage geocoding + Firestore persistent cache
│
├─ config.js         # Reads env vars, exports config
└─ server.js         # Fastify app entry point

cloudrun/
└─ service.yaml      # Cloud Run service definition (deploy config lives here)
```

---

## API

```
GET /v1/city/:city
```

Supported slugs: `nashville`, `orlando`, `orl`, `pdx`, `sf`

All cities return the same JSON shape:

```json
{
  "city": "nashville",
  "source": "nashvilleMNPD",
  "fetchedAt": "2025-08-18T22:34:10.123Z",
  "places": [
    {
      "id": "abc123",
      "name": "Theft",
      "category": "Property Crime",
      "lat": 36.1627,
      "lon": -86.7816,
      "address": "123 Main St, Nashville, TN",
      "callTimeReceived": "2025-08-18T22:15:00.000Z",
      "extras": {
        "incidentTypeCode": "911",
        "incidentTypeName": "Emergency",
        "neighborhood": "Germantown"
      }
    }
  ]
}
```

The `extras.neighborhood` field is present for Portland, Orlando, and San Francisco. Nashville provides its own neighborhood data natively through its ArcGIS dataset.

---

## City Data Sources & Quirks

### Nashville
- **Source:** Metro Nashville Police Department ArcGIS GeoJSON feature service
- **Coordinates:** Provided directly — no geocoding needed
- **Neighborhood:** Provided natively by the dataset (`neighborhood` field)
- **Notes:** Most complete dataset; includes incident codes, categories, timestamps

### Orlando
- **Source:** City of Orlando XML feed (`ORLANDO_URL`)
- **Format:** `<CALLS><CALL>` XML structure
- **Coordinates:** Not provided — every record is geocoded via OpenCage
- **Neighborhood:** Orange County ArcGIS point-in-polygon lookup (`Neighborhoods Orlando`, layer 85, field `NEIGHBORHOODNAME`). Only returns a value for recognized registered neighborhoods; `null` otherwise.
- **Address formatting:** Raw data arrives in ALL CAPS — title-cased by the adapter with direction and street suffix normalization
- **Timestamps:** `M/D/YYYY HH:MM` format, parsed as Eastern time (EDT/EST)
- **TTL:** 30 seconds (Orlando's feed itself refreshes every ~3 seconds)
- **Notes:** No incident type codes — only plain text descriptions

### Portland
- **Source:** Portland Maps KML feed (`PORTLAND_URL`)
- **Format:** KML with `<Placemark>` elements; coordinates embedded as `<Point>`
- **Coordinates:** Provided in KML — geocoding only used for rows missing coords
- **Neighborhood:** Portland Maps ArcGIS point-in-polygon lookup (layer 1, field `MAPLABEL`). Returns title-cased names like "Pearl District".
- **Timestamps:** Parsed from KML description text (e.g. `"Wednesday, August 17, 2025 4:20 PM"`) and converted to ISO 8601 Pacific time
- **Gresham edge case:** Some incidents belong to Gresham PD. The bracket regex matches `[Gresham Police #...]` and the abbreviation `GRSM` is expanded to `Gresham` in addresses.
- **Notes:** Incident IDs come from KML placemark names; incident type is the text before `" at "` in the placemark title

### San Francisco
- **Source:** Socrata JSON API (SF Open Data, dataset `gnap-fj3t`, `SF_DATASET_URL`)
- **Coordinates:** Provided directly — no geocoding needed
- **Neighborhood:** Provided by the dataset as `analysis_neighborhood`; mapped into `extras.neighborhood`
- **Sensitive calls:** Records with `sensitive_call: true` have location data stripped by the city. These are skipped entirely.
- **Notes:** Fetches up to `SF_LIMIT` records (default 1000) per request. Addresses are formatted as intersections (`"A St / B St"`).

---

## Caching

### In-memory route cache
Responses are cached in a `Map` keyed by city slug. TTL defaults to `CITY_TTL_SECONDS` (default: 900s / 15 min) but can be overridden per-adapter via `adapter.ttl` (in seconds). Orlando uses 30s; other cities use the default.

The `X-Cache` response header indicates `hit`, `miss`, or `stale-refresh`.

### Geocode cache (two layers)
Because OpenCage API calls are rate-limited and billed, geocode results are cached aggressively:

1. **In-memory** — fastest, lives for the process lifetime
2. **Firestore** — persistent, survives Cloud Run restarts and scale-to-zero cold starts

TTL is controlled by `GEOCODE_TTL_SECONDS` (default: 2,592,000s / 30 days).

Set `FIRESTORE_DISABLED=true` in `.env` to skip Firestore during local development (avoids credential errors).

---

## Geocoding APIs

### US Census Geocoder
- Used to resolve street addresses → lat/lon coordinates
- Free, no API key required
- Required for: Orlando (all records), Portland (records missing KML coords), Nashville (all records)
- Handles intersections using `&` separator (adapters use ` / ` which is normalized before the API call)
- Endpoint: `https://geocoding.geo.census.gov/geocoder/locations/onelineaddress`

### ArcGIS point-in-polygon
- Used for neighborhood lookup from coordinates
- No API key required — public endpoints
- Portland: `https://www.portlandmaps.com/arcgis/rest/services/Public/Boundaries/MapServer/1/query` (field: `MAPLABEL`)
- Orlando: `https://ocgis4.ocfl.net/arcgis/rest/services/Public_Dynamic/MapServer/85/query` (field: `NEIGHBORHOODNAME`)
- **Important:** Both endpoints require `inSR=4326` in the query params — omitting it causes empty results

---

## Local Development

### Requirements
- Node.js 20+
- npm

### Install
```bash
npm install
```

### Run
```bash
npm run dev   # nodemon with auto-restart
# or
npm start
```

Server runs on `http://localhost:8080` by default (configurable via `PORT`).

### `.env` file
Copy `.env.example` (or create `.env`) with these variables:

```env
PORT=8081
LOG_LEVEL=debug

NASHVILLE_URL=<Nashville ArcGIS URL>
PORTLAND_URL=https://www.portlandmaps.com/scripts/911incidents-kml_link.cfm
ORLANDO_URL=https://www1.cityoforlando.net/opd/activecalls/activecadpolice.xml
SF_DATASET_URL=https://data.sfgov.org/resource/gnap-fj3t.json
SF_LIMIT=1000

CITY_TTL_SECONDS=60
GEOCODE_TTL_SECONDS=2592000

OPENCAGE_KEY=your_key_here
SF_SODA_APP_TOKEN=your_token_here

FIRESTORE_DISABLED=true   # skip Firestore for local dev
```

---

## Deployment

The app runs on **Google Cloud Run** in project `axiomatic-port-469718-h4`, region `us-east4`.

### Deploy
```bash
# Build and push image
docker build -t us-east4-docker.pkg.dev/axiomatic-port-469718-h4/app-repo/activedispatch:latest .
docker push us-east4-docker.pkg.dev/axiomatic-port-469718-h4/app-repo/activedispatch:latest

# Deploy service
gcloud run services replace cloudrun/service.yaml --region us-east4
```

CI/CD runs automatically via GitHub Actions on push to `main` — see `.github/workflows/deploy.yml`. On a PR merge, two workflow runs trigger: one (from the PR event) runs tests only; the other (from the push event) runs tests and deploys. This is expected behavior, not a bug.

### IMPORTANT: Environment variables in production
**Do not add env vars in the Cloud Run console.** Every `gcloud run services replace` deploy overwrites the service configuration entirely, wiping any console changes.

All production env vars must be defined in `cloudrun/service.yaml`. Plain values go in the `env:` block; secrets (API keys) are referenced from Secret Manager via `secretKeyRef`.

### Firestore
The geocode cache uses Firestore (Native mode, `nam5` multi-region). The Cloud Run service account (`activedispatch-runner@axiomatic-port-469718-h4.iam.gserviceaccount.com`) must have the `roles/datastore.user` IAM role.

---

## Environment Variables Reference

| Variable              | Required | Description                                            |
|-----------------------|----------|--------------------------------------------------------|
| `PORT`                | No       | Server port (default: `8080`)                          |
| `LOG_LEVEL`           | No       | Fastify log level (default: `info`)                    |
| `NASHVILLE_URL`       | Yes      | Metro Nashville ArcGIS GeoJSON endpoint                |
| `PORTLAND_URL`        | Yes      | Portland Police KML feed URL                           |
| `ORLANDO_URL`         | Yes      | Orlando Police XML feed URL                            |
| `SF_DATASET_URL`      | Yes      | SF Open Data Socrata JSON endpoint                     |
| `SF_LIMIT`            | No       | Max records to fetch from SF (default: `1000`)         |
| `CITY_TTL_SECONDS`    | No       | Default route cache TTL in seconds (default: `900`)    |
| `GEOCODE_TTL_SECONDS` | No       | Geocode cache TTL in seconds (default: `2592000`)      |
| `OPENCAGE_KEY`        | No       | Unused — retained in Secret Manager but no longer called |
| `SF_SODA_APP_TOKEN`   | No       | Socrata app token for higher SF API rate limits        |
| `FIRESTORE_DISABLED`  | No       | Set to `true` to skip Firestore (local dev)            |

---

## License

MIT
