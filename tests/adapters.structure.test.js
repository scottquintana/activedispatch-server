// tests/adapters.structure.test.js
jest.setTimeout(20000);

jest.mock('../src/services/geocode', () => {
  return {
    // Return stable coords and a formatted address for any input
    geocode: async (address) => ({
      lat: 36.246122,               // consistent with your failing sample
      lon: -86.719510,
      formatted: `${address}`,      // let your withCityState logic handle suffixes
    }),
    // Keep normalization predictable
    normalizeAddress: (s) => String(s || '').trim().toLowerCase().replace(/\s+/g, ' '),
  };
});

const fs = require('fs');
const path = require('path');
const { MockAgent, setGlobalDispatcher } = require('undici');
const { PlaceCoreSchema } = require('./schema/placeCore');

// Adapters can be required once; they read the dispatcher at request time
const nashville = require('../src/adapters/nashville');
const pdx       = require('../src/adapters/pdx');
const sf        = require('../src/adapters/sf');

const CORE_KEYS = ['id','name','lat','lon','address','callTimeReceived','extras'];
const toCore = (place) => Object.fromEntries(CORE_KEYS.map(k => [k, place[k]]));


// Helper that intercepts on the *provided* agent (no stale closure!)
function interceptGet(agent, urlStr, body, contentType = 'application/json') {
  const u = new URL(urlStr);
  const origin = `${u.protocol}//${u.host}`;
  const pool = agent.get(origin);
  pool
    .intercept({ method: 'GET', path: u.pathname + (u.search || '') })
    .reply(200, body, { headers: { 'content-type': contentType } });
}

// add next to interceptGet()
function interceptRegex(agent, origin, pathRegex, body, contentType = 'application/json') {
  const pool = agent.get(origin);
  pool
    .intercept({ method: 'GET', path: pathRegex })
    .reply(200, body, { headers: { 'content-type': contentType } });
}

beforeEach(() => {
  agent = new (require('undici').MockAgent)();
  agent.disableNetConnect();
  require('undici').setGlobalDispatcher(agent);

  const fs = require('fs');
  const path = require('path');
  const read = (p) => fs.readFileSync(path.join(__dirname, 'fixtures', p), 'utf8');

  // Nashville JSON (dummy URL)
  interceptGet(agent, process.env.NASHVILLE_URL, read('nashville.json'), 'application/json');

  // Portland KML (dummy URL)
  interceptGet(agent, process.env.PORTLAND_URL, read('pdx.kml'), 'application/vnd.google-earth.kml+xml');

  // --- SF: intercept BOTH cases ---

  // 1) Your dummy URL with *any* query string
  //    Matches: /sf, /sf?, /sf?$limit=..., etc.
  interceptRegex(
    agent,
    'https://example.test',
    /^\/sf(?:\?.*)?$/i,
    read('sf.json'),
    'application/json'
  );

  // 2) The real default SODA endpoint with *any* query string
  interceptRegex(
    agent,
    'https://data.sfgov.org',
    /^\/resource\/gnap-fj3t\.json(?:\?.*)?$/i,
    read('sf.json'),
    'application/json'
  );
});

afterEach(async () => {
  await agent.close();
});

async function fetchPlaces(adapter, cityKey) {
  const payload = await adapter.fetchCity(cityKey);
  expect(Array.isArray(payload.places)).toBe(true);
  return payload.places;
}

describe('All adapters produce the same core structure (extras is free-form)', () => {
  it.each([
    ['nashville', nashville],
    ['pdx',       pdx],
    ['sf',        sf],
  ])('%s core shape', async (cityKey, adapter) => {
    const places = await fetchPlaces(adapter, cityKey);
    expect(places.length).toBeGreaterThan(0);
    for (const place of places) {
      for (const k of CORE_KEYS) {
        expect(Object.prototype.hasOwnProperty.call(place, k)).toBe(true);
      }
      const res = PlaceCoreSchema.safeParse(toCore(place));
      if (!res.success) {
        console.error(cityKey, JSON.stringify(res.error.format(), null, 2), place);
      }
      expect(res.success).toBe(true);
    }
  });
});
