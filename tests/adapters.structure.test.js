// tests/adapters.structure.test.js
jest.setTimeout(20000);

const fs = require('fs');
const path = require('path');
const nock = require('nock');

const { PlaceCoreSchema } = require('./schema/placeCore');

// IMPORTANT: require adapters *after* setupEnv.js has set URLs
const nashville = require('../src/adapters/nashville');
const pdx       = require('../src/adapters/pdx');
const sf        = require('../src/adapters/sf');

const CORE_KEYS = ['id','name','lat','lon','address','callTimeReceived','extras'];
const toCore = (place) => Object.fromEntries(CORE_KEYS.map(k => [k, place[k]]));

function mockGet(url, body, contentType = 'application/json') {
  const u = new URL(url);
  nock(`${u.protocol}//${u.host}`)
    .get(u.pathname + (u.search || ''))
    .reply(200, body, { 'Content-Type': contentType });
}

beforeEach(() => {
  nock.cleanAll();

  mockGet(
    process.env.NASHVILLE_URL,
    fs.readFileSync(path.join(__dirname, 'fixtures/nashville.json'), 'utf8')
  );
  mockGet(
    process.env.PORTLAND_URL,
    fs.readFileSync(path.join(__dirname, 'fixtures/pdx.kml'), 'utf8'),
    'application/vnd.google-earth.kml+xml'
  );
  mockGet(
    process.env.SF_DATASET_URL,
    fs.readFileSync(path.join(__dirname, 'fixtures/sf.json'), 'utf8')
  );
});

afterAll(() => nock.restore());

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
      // Must have all core keys present
      for (const k of CORE_KEYS) {
        expect(Object.prototype.hasOwnProperty.call(place, k)).toBe(true);
      }
      // Validate only the core subset
      const res = PlaceCoreSchema.safeParse(toCore(place));
      if (!res.success) {
        console.error(cityKey, JSON.stringify(res.error.format(), null, 2), place);
      }
      expect(res.success).toBe(true);
    }
  });
});
