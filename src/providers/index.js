const nashvilleMNPD = require("../adapters/nashville");
const pdx = require("../adapters/pdx");
const sf = require("../adapters/sf");

const registry = { nashvilleMNPD, pdx, sf };

const CITY_PROVIDER = {
  nashville: "nashvilleMNPD",
  portland: "pdx",
  pdx: "pdx",
  sf: "sf",
  "san-francisco": "sf",
  sanfrancisco: "sf"
};

function resolveProvider(city) {
  const name = CITY_PROVIDER[String(city).toLowerCase()] || "nashvilleMNPD";
  const p = registry[name];
  if (!p) throw new Error(`No provider registered: ${name}`);
  return p;
}
module.exports = { resolveProvider };
