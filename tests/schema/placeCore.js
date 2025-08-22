const { z } = require("zod");

// Strict root: only these keys at the top level; extras is free-form
const PlaceCoreSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  lat: z.number().finite(),
  lon: z.number().finite(),
  address: z.string().min(1),
  callTimeReceived: z.string().datetime(), // REQUIRED
  extras: z.record(z.any()).default({}),
})
// .strict()
.refine(p => p.lat >= -90 && p.lat <= 90 && p.lon >= -180 && p.lon <= 180, {
  message: "lat/lon out of range",
});

module.exports = { PlaceCoreSchema };
