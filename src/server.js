const Fastify = require("fastify");
const { port, logLevel } = require("./config");
const { healthRoutes } = require("./routes/health");
const { cityRoutes } = require("./routes/city");  

const isDev = process.env.NODE_ENV !== "production";
const app = Fastify({ logger: { level: logLevel } });

app.register(healthRoutes);
app.register(cityRoutes);                       

app.ready().then(() => app.log.info(app.printRoutes())); 

app.listen({ port, host: "0.0.0.0" })
  .then(() => app.log.info(`listening on :${port}`))
  .catch((err) => { app.log.error(err); process.exit(1); });
