// tests/setupEnv.js
// If you still like using .env locally, load it first (won't override existing):
require('dotenv').config({ override: false });

// Provide dummy URLs if not set (tests will mock them with nock)
process.env.NASHVILLE_URL   ||= 'https://example.test/nashville';
process.env.PORTLAND_URL    ||= 'https://example.test/pdx';
process.env.SF_DATASET_URL  ||= 'https://example.test/sf';

// If your code reads a geocoding key during tests, give it a harmless dummy:
process.env.GEOCODE_API_KEY ||= 'test-dummy-key';
process.env.LOG_LEVEL       ||= 'warn';
process.env.NODE_ENV        ||= 'test';
