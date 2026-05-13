const nashvilleMNPD = require("../adapters/nashville");
const pdx = require("../adapters/pdx");
const sf = require("../adapters/sf");
const orl = require("../adapters/orl");

const registry = { nashvilleMNPD, pdx, sf, orl };

const CITY_PROVIDER = {
  nashville: "nashvilleMNPD",
  portland: "pdx",
  pdx: "pdx",
  sf: "sf",
  "san-francisco": "sf",
  sanfrancisco: "sf",
  orlando: "orl",
  orl: "orl",
};

function resolveProvider(city) {
  const name = CITY_PROVIDER[String(city).toLowerCase()] || "nashvilleMNPD";
  const p = registry[name];
  if (!p) throw new Error(`No provider registered: ${name}`);
  return p;
}
module.exports = { resolveProvider };
