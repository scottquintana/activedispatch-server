# Active Dispatch (Server)

This project fetches **real-time police dispatch data** from multiple US cities, normalizes it into a consistent shape, and exposes it via a lightweight Fastify API to support our mobile apps

Currently supported cities:
- **Nashville, TN** (Metro Nashville Police Department)
- **Portland, OR** (Portland Police Bureau)
- **San Francisco, CA** (SFPD / SF Open Data)

---

## Features

- **Adapters per city**  
  Each city has a dedicated adapter (`src/adapters/*.js`) that:
  - Fetches the raw data from the city’s public API.
  - Extracts IDs, categories, names, timestamps, and coordinates.
  - Normalizes addresses and geocodes them if needed.
  - Returns a standardized `place` object.

- **Consistent API Response**  
  All cities return the same JSON structure:

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
        "updatedAt": "2025-08-18T22:20:00.000Z",
        "extras": {
          "callTimeReceived": "2025-08-18T22:15:00.000Z",
          "incidentTypeCode": "911",
          "incidentTypeName": "Emergency"
        }
      }
    ]
  }
  ```

- **Per-city data quirks handled**  
  - Nashville: `callTimeReceived` preserved in `extras`.
  - Portland: incident times parsed from KML descriptions and normalized to ISO8601.
  - San Francisco: uses API-provided coordinates directly, with intersection formatting (`"A St / B St"`).

- **Built on Fastify**  
  Minimal, fast, and simple API server.

---

## Project Structure

```
src/
├─ adapters/
│  ├─ nashville.js   # Metro Nashville Police active dispatch
│  ├─ pdx.js         # Portland Police Bureau incidents
│  ├─ sf.js          # San Francisco incidents
│
├─ services/
│  ├─ geocode.js     # Geocoding + address normalization
│
├─ server.js         # Fastify app entry point
```

---

## API Usage

Start the server, then query:

```
GET /v1/city/:city
```

Example:

```
GET /v1/city/nashville
```

Response:

```json
{
  "city": "nashville",
  "source": "nashvilleMNPD",
  "fetchedAt": "...",
  "places": [...]
}
```

Supported `:city` values:
- `nashville`
- `pdx`
- `sf`

---

## Development

### Requirements
- Node.js 20+
- npm or yarn

### Install
```bash
npm install
```

### Run
```bash
npm start
```

This starts the Fastify server on `http://localhost:3000`.

---

## Environment Variables

| Variable          | Purpose                                |
|-------------------|----------------------------------------|
| `NASHVILLE_URL`   | Metro Nashville API endpoint            |
| `PDX_URL`         | Portland Police KML feed URL           |
| `SF_URL`          | San Francisco incidents dataset URL    |
| `GEOCODE_API_KEY` | (Optional) API key for geocoding       |

---

## Roadmap

- [ ] Add caching (Redis or in-proc LRU).
- [ ] Rate limiting & request logging.
- [ ] Swagger/OpenAPI docs via `@fastify/swagger`.
- [ ] More cities (LA, Chicago, Seattle, etc).
- [ ] CI/CD pipeline for deployment.

---

## License

MIT
