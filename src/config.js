require("dotenv").config();

module.exports = {
  port: Number(process.env.PORT || 8080),
  logLevel: process.env.LOG_LEVEL || "info",
};
