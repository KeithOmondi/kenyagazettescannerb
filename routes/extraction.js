import express from "express";
import multer from "multer";
import pdfParse from "pdf-parse";
import XLSX from "xlsx";

import { initDB, clearMatches, saveMatchesToDB } from "../helpers/db.js";
import { extractGazetteRecords } from "../helpers/parse.js";
import { normalizeName, tokenizeName, jaroWinkler, jaccard, bestExcelNameKey } from "../helpers/match.js";

const router = express.Router();

// Multer in-memory — cleaner & faster than temp files
const upload = multer({ storage: multer.memoryStorage() });

// ---------- Utilities ----------
function parseExcelBuffer(buf) {
  const wb = XLSX.read(buf, { type: "buffer" });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { defval: "" });
  return rows;
}

// Normalize an Excel row into a canonical form used for matching
function normalizeExcelRows(rows) {
  return rows.map((row, idx) => {
    const rawName = bestExcelNameKey(row) || "";
    return {
      _rowIndex: idx,
      _name_raw: rawName,
      _name_norm: normalizeName(rawName),
      _name_tokens: tokenizeName(rawName),
      // Keep all original columns (spread last)
      ...row,
    };
  });
}

// ------------- Endpoints -------------

// POST /api/match  (multipart form: pdfFile, excelFile; query: mode, threshold)
router.post(
  "/match",
  upload.fields([
    { name: "pdfFile", maxCount: 1 },
    { name: "excelFile", maxCount: 1 },
  ]),
  async (req, res, next) => {
    try {
      const mode = (req.query.mode || "tokens").toLowerCase();
      const threshold = Math.max(0, Math.min(0.999, Number(req.query.threshold || 0.85)));

      const pdfFile = req.files?.pdfFile?.[0];
      const excelFile = req.files?.excelFile?.[0];

      if (!pdfFile || !excelFile) {
        return res.status(400).json({ error: "Both pdfFile and excelFile are required." });
      }

      // Extract PDF text
      const pdfData = await pdfParse(pdfFile.buffer);
      const pdfText = pdfData.text || "";

      // Gazette records
      const gazetteRecords = extractGazetteRecords(pdfText);
      const totalGazette = gazetteRecords.length;

      // Excel rows
      const excelRowsRaw = parseExcelBuffer(excelFile.buffer);
      const excelRows = normalizeExcelRows(excelRowsRaw);
      const totalExcel = excelRows.length;

      // Build a quick lookup for exact/tokens
      const excelByNorm = new Map();     // exact
      const excelByTokens = new Map();   // tokens

      for (const r of excelRows) {
        if (r._name_norm) {
          const arr = excelByNorm.get(r._name_norm) || [];
          arr.push(r);
          excelByNorm.set(r._name_norm, arr);
        }
        if (r._name_tokens) {
          const arr = excelByTokens.get(r._name_tokens) || [];
          arr.push(r);
          excelByTokens.set(r._name_tokens, arr);
        }
      }

      // Perform matching
      const matchedRows = [];
      for (const g of gazetteRecords) {
        const gNorm = normalizeName(g.name_of_deceased);
        const gTokens = tokenizeName(g.name_of_deceased);

        let candidates = [];

        if (mode === "exact") {
          candidates = excelByNorm.get(gNorm) || [];
        } else if (mode === "tokens") {
          candidates = excelByTokens.get(gTokens) || [];
        } else if (mode === "fuzzy") {
          // Fuzzy over ALL excel rows (optimize if needed)
          // Score by Jaro-Winkler & token Jaccard; combine
          candidates = excelRows
            .map((ex) => {
              const s1 = jaroWinkler(gNorm, ex._name_norm);
              const s2 = jaccard(gTokens.split(" "), ex._name_tokens.split(" "));
              // Weighted combo; tweak weights if needed
              const score = (s1 * 0.7) + (s2 * 0.3);
              return { ex, score };
            })
            .filter(({ score }) => score >= threshold)
            .sort((a, b) => b.score - a.score)
            .slice(0, 5)
            .map(({ ex }) => ex); // take top few
        } else {
          return res.status(400).json({ error: `Unknown mode: ${mode}` });
        }

        // If candidates found, create match rows
        for (const ex of candidates) {
          matchedRows.push({
            court_station: g.court_station,
            cause_no: g.cause_no,
            name_of_deceased: g.name_of_deceased,
            status_at_gp: g.status_at_gp || "Published",
            volume_no: g.volume_no || "",
            date_published: g.date_published || "",
            // Optionally include excel context columns for export
            excel_name: ex._name_raw,
          });
        }
      }

      // Save to DB (upsert w/ status upgrade Published -> Approved)
      const inserted = await saveMatchesToDB(matchedRows);

      res.json({
        success: true,
        mode,
        threshold,
        totalGazette,
        totalExcel,
        matchedCount: matchedRows.length,
        insertedCount: inserted.length,
        matchedRows,
      });
    } catch (err) {
      next(err);
    }
  }
);

// POST /api/clear-records
router.post("/clear-records", async (_req, res, next) => {
  try {
    const n = await clearMatches();
    res.json({ success: true, deleted: n });
  } catch (err) {
    next(err);
  }
});

// GET /api/matches  (optional: quick fetch)
router.get("/matches", async (_req, res, next) => {
  try {
    const db = await initDB();
    const rows = await db.all(`SELECT * FROM gazette_matches ORDER BY date_published DESC, id DESC`);
    await db.close();
    res.json({ success: true, count: rows.length, rows });
  } catch (err) {
    next(err);
  }
});

export default router;
