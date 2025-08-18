async function healthRoutes(fastify) {
    fastify.get("/healthz", async () => ({ ok: true }));
  }
  module.exports = { healthRoutes };
  