// ==========================
// Gazette Matcher Backend
// ==========================
import { createRequire } from "module";
const require = createRequire(import.meta.url);

import express from "express";
import multer from "multer";
import xlsx from "xlsx";
import fs from "fs";
import cors from "cors";
import path from "path";
import { fileURLToPath } from "url";
import Database from "better-sqlite3";
const pdfParse = require("pdf-parse");
import pkg from "fastest-levenshtein";
const { similarity } = pkg;

// ---------- Helpers for ES Modules ----------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ---------- Express App ----------
const app = express();
app.use(cors({ origin: "https://kenyagazettescanner.vercel.app/", methods: ["GET", "POST"] }));
app.use(express.json());

// ---------- Database Setup ----------
const DB_PATH = path.join(__dirname, "gazette.db");
const db = new Database(DB_PATH);

// Enable WAL for concurrency
db.pragma("journal_mode = WAL");

// Create matches table (with normalized name for uniqueness)
db.prepare(`
  CREATE TABLE IF NOT EXISTS gazette_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    gazette_date   TEXT,           -- optional mirror of date_published if needed
    volume_no      TEXT,
    court_station  TEXT,
    cause_no       TEXT,
    name_of_deceased TEXT,
    name_norm      TEXT,           -- lowercase normalized version for unique index
    status_at_gp   TEXT,
    date_published TEXT,
    matched_alias  TEXT,
    UNIQUE(name_norm, date_published, volume_no)
  )
`).run();

// ---------- File Upload Setup ----------
const uploadDir = path.join(__dirname, "uploads");
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });

const storage = multer.diskStorage({
  destination: (_, __, cb) => cb(null, uploadDir),
  filename: (_, file, cb) => cb(null, `${Date.now()}-${file.originalname}`),
});
const upload = multer({ storage });

// ---------- Helpers ----------
function normalizeName(name) {
  if (!name && name !== "") return "";
  return String(name)
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[^\p{L}\p{N}\s]/gu, "")
    .trim();
}

function getAliases(rawName) {
  if (!rawName) return [];
  return String(rawName)
    .split(/\s+(?:alias|aka|a\.k\.a\.|otherwise known as)\s+/i)
    .map((n) => normalizeName(n))
    .filter(Boolean);
}

function nameTokens(name) {
  return normalizeName(name)
    .split(" ")
    .filter((t) => t && t.length > 1);
}

function nameSignature(name) {
  return nameTokens(name).sort().join("|");
}

/**
 * Robust extractor for Gazette records:
 * Finds "(the) estate of <NAME>" patterns and global volume/date.
 * Returns rows with keys matching DB/processing pipeline.
 */
function extractGazetteRecords(pdfText) {
  const results = [];
  if (!pdfText || !pdfText.trim()) return results;

  const normalized = pdfText
    .replace(/\r\n/g, "\n")
    .replace(/\t/g, " ")
    .replace(/\s+/g, " ");

  const volumeMatch = normalized.match(
    /Vol\.?\s*([A-Z0-9IVXLCDM]+)\s*[-–—]\s*No\.?\s*(\d+)/i
  );
  const volumeNo = volumeMatch ? `Vol. ${volumeMatch[1]} — No. ${volumeMatch[2]}` : "";

  const datePubMatch = normalized.match(
    /\b(\d{1,2})(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s*,?\s+(\d{4})\b/i
  );
  const datePublished = datePubMatch ? `${datePubMatch[1]} ${datePubMatch[2]} ${datePubMatch[3]}` : "";

  const estateRegex =
    /\b(?:estate of|in the estate of|the estate of)\s+([A-Z][A-Z ,.'\-\(\)\/&0-9]+?)(?=[\.,;:\n]| who\b|$)/gi;

  let m;
  const seenSignatures = new Set();

  while ((m = estateRegex.exec(normalized)) !== null) {
    let rawName = m[1].trim();
    rawName = rawName.replace(/(,?\s*(herein|hereinafter).*)$/i, "").trim();
    rawName = rawName.replace(/,$/, "").trim();

    const clean = rawName.replace(/\s{2,}/g, " ").trim();
    if (!clean) continue;

    const sig = nameSignature(clean);
    if (seenSignatures.has(sig)) continue;
    seenSignatures.add(sig);

    results.push({
      "Court Station": "",
      "Cause No.": "",
      "Name of Deceased": clean,
      "Status at G.P.": "Published",
      "Date Published": datePublished || "",
      "Volume No.": volumeNo || "",
    });
  }

  if (!results.length) {
    const fallbackRegex = /\b([A-Z][A-Z ,.'\-\(\)\/&0-9]{3,}?)\s+deceased\b/gi;
    const seen2 = new Set();
    while ((m = fallbackRegex.exec(normalized)) !== null) {
      const rawName = m[1].trim();
      const sig = nameSignature(rawName);
      if (seen2.has(sig)) continue;
      seen2.add(sig);
      results.push({
        "Court Station": "",
        "Cause No.": "",
        "Name of Deceased": rawName,
        "Status at G.P.": "Published",
        "Date Published": datePublished || "",
        "Volume No.": volumeNo || "",
      });
    }
  }

  return results;
}

// Insert-or-ignore bulk saver
function saveMatchesToDB(matches) {
  if (!matches || !matches.length) return 0;

  const insert = db.prepare(`
    INSERT OR IGNORE INTO gazette_matches (
      gazette_date, volume_no, court_station, cause_no, name_of_deceased,
      name_norm, status_at_gp, date_published, matched_alias
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const tx = db.transaction((rows) => {
    let inserted = 0;
    for (const r of rows) {
      const info = insert.run(
        r["Date Published"] || null,                  // gazette_date (mirror)
        r["Volume No."] || null,
        r["Court Station"] || null,
        r["Cause No."] || null,
        r["Name of Deceased"] || null,
        normalizeName(r["Name of Deceased"] || ""),   // name_norm
        r["Status at G.P."] || null,
        r["Date Published"] || null,
        r["MatchedAlias"] || ""
      );
      if (info.changes === 1) inserted++;
    }
    return inserted;
  });

  return tx(matches);
}

// ================== Routes ==================

// Upload & Match (supports mode & threshold like the UI)
app.post(
  "/match",
  upload.fields([{ name: "pdfFile" }, { name: "excelFile" }]),
  async (req, res) => {
    let pdfPath, excelPath;
    try {
      const mode = String(req.query.mode || "tokens").toLowerCase(); // exact | tokens | fuzzy
      const thresholdParam = Number(req.query.threshold ?? 0.85);
      const threshold = Number.isFinite(thresholdParam)
        ? Math.max(0, Math.min(1, thresholdParam))
        : 0.85;

      if (!req.files?.pdfFile?.[0] || !req.files?.excelFile?.[0]) {
        return res.status(400).json({ error: "Both PDF and Excel files are required." });
      }

      // Parse PDF
      pdfPath = req.files.pdfFile[0].path;
      const pdfData = await pdfParse(fs.readFileSync(pdfPath));
      const gazetteRecords = extractGazetteRecords(pdfData.text || "");

      // Parse Excel (all sheets, defval for blank cells)
      excelPath = req.files.excelFile[0].path;
      const workbook = xlsx.readFile(excelPath);
      const excelRows = workbook.SheetNames.flatMap((sheet) =>
        xlsx.utils.sheet_to_json(workbook.Sheets[sheet], { defval: "" })
      );

      if (!excelRows.length) {
        try { if (pdfPath && fs.existsSync(pdfPath)) fs.unlinkSync(pdfPath); } catch {}
        try { if (excelPath && fs.existsSync(excelPath)) fs.unlinkSync(excelPath); } catch {}
        return res.status(400).json({ error: "Excel file has no rows." });
      }

      // Find name column heuristically
      const headerKeys = Object.keys(excelRows[0] || {});
      const nameColKey =
        headerKeys.find((c) => c.toLowerCase().includes("name") || c.toLowerCase().includes("deceased")) ||
        "Name of Deceased";

      // Build structured Excel records
      const excelRecords = excelRows.map((row) => ({
        "Court Station": row["Court Station"] || row["Station"] || "",
        "Cause No.": row["Cause No."] || row["Case Number"] || "",
        "Name of Deceased": String(row?.[nameColKey] ?? row["Name"] ?? row["Name of Deceased"] ?? "").trim(),
        "Date Published": row["Date Published"] || "",
      }));

      // Build indices from Gazette for matching
      const aliasIndex = new Map(); // alias -> [gr]
      const sigIndex = new Map();   // signature -> [gr]
      const tokenIndex = new Map(); // token -> [gr]

      for (const gr of gazetteRecords) {
        const aliases = getAliases(gr["Name of Deceased"]);
        const useAliases = aliases.length ? aliases : [normalizeName(gr["Name of Deceased"])];
        for (const a of useAliases) {
          if (!a) continue;

          if (!aliasIndex.has(a)) aliasIndex.set(a, []);
          aliasIndex.get(a).push(gr);

          const sig = nameSignature(a);
          if (!sigIndex.has(sig)) sigIndex.set(sig, []);
          sigIndex.get(sig).push(gr);

          for (const t of nameTokens(a)) {
            if (!tokenIndex.has(t)) tokenIndex.set(t, []);
            tokenIndex.get(t).push(gr);
          }
        }
      }

      // Match Excel rows to Gazette records
      const matchedRecords = [];
      const seenPairKeys = new Set(); // to avoid duplicates within this run

      for (const er of excelRecords) {
        const erNorm = normalizeName(er["Name of Deceased"]);
        if (!erNorm) continue;

        let candidates = [];

        if (mode === "exact") {
          candidates = aliasIndex.get(erNorm) || [];
        } else if (mode === "tokens") {
          const sig = nameSignature(erNorm);
          candidates = sigIndex.get(sig) || aliasIndex.get(erNorm) || [];
        } else if (mode === "fuzzy") {
          const seenCands = new Set();
          for (const token of nameTokens(erNorm)) {
            for (const cand of tokenIndex.get(token) || []) seenCands.add(cand);
          }
          candidates = [...seenCands].filter(
            (cand) => similarity(erNorm, normalizeName(cand["Name of Deceased"])) >= threshold
          );
        } else {
          // default to tokens
          const sig = nameSignature(erNorm);
          candidates = sigIndex.get(sig) || aliasIndex.get(erNorm) || [];
        }

        // Remove dup candidates by identity
        candidates = Array.from(new Set(candidates));

        for (const g of candidates) {
          const pairKey = `${normalizeName(g["Name of Deceased"])}|${(g["Date Published"] || "").trim()}|${(g["Volume No."] || "").trim()}`;
          if (seenPairKeys.has(pairKey)) continue;
          seenPairKeys.add(pairKey);

          matchedRecords.push({
            "Court Station": er["Court Station"] || g["Court Station"] || "",
            "Cause No.": er["Cause No."] || g["Cause No."] || "",
            "Name of Deceased": g["Name of Deceased"] || er["Name of Deceased"],
            "Status at G.P.": g["Status at G.P."] || "Published",
            "Date Published": g["Date Published"] || er["Date Published"] || "",
            "Volume No.": g["Volume No."] || "",
            MatchedAlias: erNorm,
          });
        }
      }

      const insertedCount = saveMatchesToDB(matchedRecords);

      // Cleanup uploads
      try { if (pdfPath && fs.existsSync(pdfPath)) fs.unlinkSync(pdfPath); } catch {}
      try { if (excelPath && fs.existsSync(excelPath)) fs.unlinkSync(excelPath); } catch {}

      return res.json({
        mergedRecords: matchedRecords,
        insertedCount,         // <-- what your frontend reads
        mode,
        threshold,
      });
    } catch (err) {
      console.error("Error in /match:", err);
      try { if (pdfPath && fs.existsSync(pdfPath)) fs.unlinkSync(pdfPath); } catch {}
      try { if (excelPath && fs.existsSync(excelPath)) fs.unlinkSync(excelPath); } catch {}
      return res.status(500).json({ error: "Failed to process files", details: err.message });
    }
  }
);

// Get stored matches (distinct by (name_norm, date_published, volume_no))
app.get("/records", (req, res) => {
  try {
    const rows = db.prepare(`
      SELECT gm.*
      FROM gazette_matches gm
      JOIN (
        SELECT name_norm, date_published AS dp, volume_no AS vol, MIN(id) AS min_id
        FROM gazette_matches
        GROUP BY name_norm, dp, vol
      ) d ON gm.id = d.min_id
      ORDER BY
        CASE WHEN gm.date_published IS NULL OR gm.date_published = '' THEN 1 ELSE 0 END,
        gm.date_published DESC,
        gm.name_norm ASC
    `).all();

    res.json(rows || []);
  } catch (err) {
    console.error("Error fetching records:", err);
    res.status(500).json({ error: "Failed to fetch records." });
  }
});

// Clear DB (frontend calls this)
app.post("/clear-records", (req, res) => {
  try {
    db.prepare("DELETE FROM gazette_matches").run();
    res.json({ ok: true });
  } catch (err) {
    console.error("Error clearing records:", err);
    res.status(500).json({ error: "Failed to clear records." });
  }
});

// ---------- Start Server ----------
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`✅ Gazette matcher server running at http://localhost:${PORT}`);
});




