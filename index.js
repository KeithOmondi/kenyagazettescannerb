import express from "express";
import cors from "cors";
import morgan from "morgan";
import path from "path";
import { fileURLToPath } from "url";
import extractionRouter from "./routes/extraction.js";


const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);


const app = express();
app.use(cors({ origin: "https://kenyagazettescanner.vercel.app", credentials: true }));
app.use(express.json({ limit: "25mb" }));
app.use(morgan("dev"));


app.use("/api", extractionRouter);


app.get("/health", (_req, res) => res.json({ ok: true }));


// Global error handler
app.use((err, req, res, _next) => {
console.error("🔥 Global Error Handler:", err.stack || err.message || err);
res.status(err.status || 500).json({ success: false, message: err.message || "Internal Server Error" });
});


const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`✅ API listening on http://localhost:${PORT}`));