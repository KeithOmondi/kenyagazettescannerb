// helpers/db.js
import sqlite3 from "sqlite3";
import { open } from "sqlite";

/**
 * Open DB connection
 */
export async function initDB() {
  const db = await open({
    filename: "./gazette.db", // change path if needed
    driver: sqlite3.Database,
  });

  await db.exec("PRAGMA journal_mode = WAL;");
  await ensureSchema(db);
  return db;
}

/**
 * Create tables & indexes once
 */
async function ensureSchema(db) {
  await db.exec(`
    CREATE TABLE IF NOT EXISTS gazette_matches (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      court_station TEXT NOT NULL,
      cause_no TEXT NOT NULL,
      name_norm TEXT NOT NULL,
      name_of_deceased TEXT NOT NULL,
      excel_name TEXT,
      match_type TEXT,                -- "exact" | "fuzzy" | etc
      score REAL DEFAULT 0,
      duplicate INTEGER DEFAULT 0,    -- 0/1
      status_at_gp TEXT DEFAULT 'Published',
      volume_no TEXT,
      date_published TEXT,

      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    -- Uniqueness key for upsert
    CREATE UNIQUE INDEX IF NOT EXISTS ux_gazette_match
      ON gazette_matches (court_station, cause_no, name_norm, date_published, volume_no);

    CREATE INDEX IF NOT EXISTS ix_gazette_station_date
      ON gazette_matches (court_station, date_published);

    CREATE INDEX IF NOT EXISTS ix_gazette_excel_name
      ON gazette_matches (excel_name);
  `);
}

/**
 * Mark duplicates where the same excel_name maps to multiple matches
 */
async function flagDuplicates(db) {
  const dupes = await db.all(`
    SELECT excel_name
    FROM gazette_matches
    WHERE IFNULL(excel_name, '') <> ''
    GROUP BY excel_name
    HAVING COUNT(*) > 1
  `);

  if (dupes.length === 0) return;

  const names = dupes.map(d => d.excel_name);
  const placeholders = names.map(() => "?").join(",");
  await db.run(
    `UPDATE gazette_matches SET duplicate = 1 WHERE excel_name IN (${placeholders})`,
    names
  );
}

/**
 * Batch insert with UPSERT; only escalate status to "Approved", never downgrade.
 * Keeps best (max) score seen so far.
 */
export async function saveMatchesToDB(matches, options = {}) {
  const batchSize = Number(options.batchSize || 500);
  if (!Array.isArray(matches) || matches.length === 0) return [];

  const db = await initDB();
  const inserted = [];

  const insertSQL = `
    INSERT INTO gazette_matches
      (court_station, cause_no, name_norm, name_of_deceased, excel_name,
       match_type, score, duplicate, status_at_gp, volume_no, date_published, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    ON CONFLICT(court_station, cause_no, name_norm, date_published, volume_no)
    DO UPDATE SET
      status_at_gp = CASE
        WHEN excluded.status_at_gp = 'Approved' THEN 'Approved'
        ELSE gazette_matches.status_at_gp
      END,
      excel_name = COALESCE(excluded.excel_name, gazette_matches.excel_name),
      match_type = COALESCE(excluded.match_type, gazette_matches.match_type),
      score = MAX(gazette_matches.score, excluded.score),
      updated_at = datetime('now')
  `;

  const stmt = await db.prepare(insertSQL);

  try {
    for (let i = 0; i < matches.length; i += batchSize) {
      const batch = matches.slice(i, i + batchSize);

      await db.exec("BEGIN TRANSACTION;");
      try {
        for (const m of batch) {
          // name_norm must already be set by caller; fallback defensively
          const nameNorm = m.name_norm || normalizeForDB(m.name_of_deceased || "");

          const result = await stmt.run(
            m.court_station || "",
            m.cause_no || "",
            nameNorm,
            m.name_of_deceased || "",
            m.excel_name || null,
            m.match_type || null,
            Number(m.score ?? 0),
            Number(m.duplicate ? 1 : 0),
            m.status_at_gp || "Published",
            m.volume_no || "",
            m.date_published || ""
          );

          // sqlite run() .changes is 1 for insert or update;
          // we push the row metadata for observability
          if (result.changes) {
            inserted.push({ id: result.lastID, ...m });
          }
        }
        await db.exec("COMMIT;");
      } catch (err) {
        console.error("❌ Batch insert failed:", err);
        await db.exec("ROLLBACK;");
      }
    }
  } finally {
    await stmt.finalize();
  }

  await flagDuplicates(db);
  await db.close();
  return inserted;
}

/**
 * Delete all matches
 */
export async function clearMatches() {
  const db = await initDB();
  try {
    const res = await db.run("DELETE FROM gazette_matches;");
    await db.exec("VACUUM;");
    return res.changes || 0;
  } finally {
    await db.close();
  }
}

/**
 * Basic in-file normalization for DB safety when caller forgot to pass name_norm.
 * (Prefer using utils/normalize.normalizeNameDB)
 */
function normalizeForDB(s = "") {
  return String(s)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
