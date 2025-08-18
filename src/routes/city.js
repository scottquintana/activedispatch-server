const { cityTTL } = require("../config");
const { resolveProvider } = require("../providers");

const cache = new Map();

async function cityRoutes(fastify) {
  fastify.get("/v1/city/:city", async (req, reply) => {
    const city = req.params.city.toLowerCase();
    const key = `city:${city}`;
    const now = Date.now();

    const cached = cache.get(key);
    if (cached && cached.expiresAt > now) {
      reply.header("X-Cache", "hit");
      return cached.data;
    }

    const provider = resolveProvider(city);
    fastify.log.info({ city, provider: provider.name }, "fetching city");
    const data = await provider.fetchCity(city);

    cache.set(key, { data, expiresAt: now + cityTTL * 1000 });
    reply.header("X-Cache", cached ? "stale-refresh" : "miss");
    return data;
  });
}

module.exports = { cityRoutes };
