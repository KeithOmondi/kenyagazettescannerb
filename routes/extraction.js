import express from "express";
import multer from "multer";
import {
  initDB,
  saveMatchesToDB,
  clearMatches,
} from "../helpers/db.js";
// import { buildReport } from "../utils/report.js"; // optional if you have it
// import { parseGazette } from "../helpers/parseGazette.js";
// import { parseRegistry } from "../helpers/parseRegistry.js";

const upload = multer({ dest: "uploads/" });
const router = express.Router();

/**
 * Core process function (used by both /process and /match)
 */
async function processMatches({
  mode,
  candidates = [],
  gazetteRecords = [],
  excelRows = [],
  totalGazette = 0,
  totalExcel = 0,
  acceptThreshold = 0.8,
  reviewThreshold = 0.5,
}) {
  if (!mode) throw new Error("Mode is required");
  if (!["exact", "fuzzy", "tokens"].includes(mode)) {
    throw new Error(`Unknown mode: ${mode}`);
  }

  if (!Array.isArray(candidates)) {
    throw new Error("Candidates must be an array");
  }

  const accepted = [];
  const review = [];

  for (const { g, ex, score } of candidates) {
    if (!g || !ex) continue;

    const publicRow = {
      court_station: g.court_station,
      cause_no: g.cause_no,
      name_of_deceased: g.name_of_deceased,
      status_at_gp: score >= acceptThreshold ? "Approved" : "Published",
      volume_no: g.volume_no || "",
      date_published: g.date_published || "",
      excel_name: ex._name_raw,
    };

    const enriched = { public: publicRow, _score: score, _g: g, _e: ex };

    if (score >= acceptThreshold) {
      accepted.push(enriched);
    } else if (score >= reviewThreshold) {
      review.push(enriched);
    }
  }

  // Save only accepted matches
  const rowsToSave = accepted.map((m) => m.public);
  const inserted = await saveMatchesToDB(rowsToSave);

  // Optional report builder
  let report = null;
  // if (typeof buildReport === "function") {
  //   report = buildReport(totalGazette, totalExcel, accepted, review, gazetteRecords, excelRows);
  // }

  return {
    success: true,
    mode,
    acceptThreshold,
    reviewThreshold,
    totalGazette,
    totalExcel,
    matchedCount: accepted.length + review.length,
    insertedCount: inserted.length,
    accepted,
    review,
    report,
  };
}

/**
 * POST /process — process structured candidates
 */
router.post("/process", async (req, res, next) => {
  try {
    const result = await processMatches(req.body);
    res.json(result);
  } catch (err) {
    next(err);
  }
});

/**
 * POST /clear-records — wipe DB
 */
router.post("/clear-records", async (_req, res, next) => {
  try {
    const n = await clearMatches();
    res.json({ success: true, deleted: n });
  } catch (err) {
    next(err);
  }
});

/**
 * GET /matches — fetch saved matches
 */
router.get("/matches", async (_req, res, next) => {
  let db;
  try {
    db = await initDB();
    const rows = await db.all(
      `SELECT * FROM gazette_matches ORDER BY date_published DESC, id DESC`
    );
    res.json({ success: true, count: rows.length, rows });
  } catch (err) {
    next(err);
  } finally {
    if (db) await db.close();
  }
});

/**
 * POST /match — upload PDF + Excel, parse, then process
 */
router.post(
  "/match",
  upload.fields([{ name: "pdfFile" }, { name: "excelFile" }]),
  async (req, res, next) => {
    try {
      const { mode = "tokens", threshold = 0.8 } = req.query;
      const pdfFile = req.files.pdfFile?.[0];
      const excelFile = req.files.excelFile?.[0];

      if (!pdfFile || !excelFile) {
        return res.status(400).json({ error: "Missing files" });
      }

      // TODO: implement these
      // const gazetteRecords = await parseGazette(pdfFile.path);
      // const excelRows = await parseRegistry(excelFile.path);
      // const candidates = buildCandidates(gazetteRecords, excelRows, mode, threshold);

      const gazetteRecords = [];
      const excelRows = [];
      const candidates = [];

      const result = await processMatches({
        mode,
        candidates,
        gazetteRecords,
        excelRows,
        totalGazette: gazetteRecords.length,
        totalExcel: excelRows.length,
        acceptThreshold: Number(threshold) || 0.8,
      });

      res.json(result);
    } catch (err) {
      next(err);
    }
  }
);

export default router;
