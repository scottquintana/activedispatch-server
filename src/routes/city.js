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

    let data;
    try {
      data = await provider.fetchCity(city, fastify.log);
    } catch (err) {
      fastify.log.error({ city, provider: provider.name, err }, "adapter fetch failed");
      throw err;
    }

    if (data.places.length === 0) {
      fastify.log.warn({ city, provider: provider.name }, "adapter returned zero places");
    }

    const ttl = (provider.ttl ?? cityTTL) * 1000;
    cache.set(key, { data, expiresAt: now + ttl });
    reply.header("X-Cache", cached ? "stale-refresh" : "miss");
    return data;
  });
}

module.exports = { cityRoutes };
