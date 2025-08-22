
import express from "express";
import cors from "cors";
import path from "path";
import { fileURLToPath } from "url";
import extractionRouter from "./routes/extraction.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

// CORS (adjust origin for your client)
app.use(cors({ origin: true, credentials: true }));
app.use(express.json());

// Routes
app.use("/api", extractionRouter);

// Health
app.get("/health", (_req, res) => res.json({ ok: true }));

// Global error handler
app.use((err, req, res, _next) => {
  console.error("🔥 Global Error Handler:", err.stack || err.message || err);
  res.status(err.status || 500).json({
    success: false,
    message: err.message || "Internal Server Error",
  });
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`✅ API listening on http://localhost:${PORT}`);
});
