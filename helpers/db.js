import sqlite3 from "sqlite3";
import { open } from "sqlite";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export const DB_PATH = path.resolve(__dirname, "../../gazette.db");

const REQUIRED_COLUMNS = [
  { name: "court_station", type: "TEXT" },
  { name: "cause_no", type: "TEXT" },
  { name: "name_norm", type: "TEXT" },
  { name: "name_of_deceased", type: "TEXT" },
  { name: "status_at_gp", type: "TEXT" },
  { name: "volume_no", type: "TEXT" },
  { name: "date_published", type: "TEXT" }
];

export async function initDB() {
  const db = await open({ filename: DB_PATH, driver: sqlite3.Database });
  await db.exec("PRAGMA journal_mode = WAL;");
  await db.exec(`CREATE TABLE IF NOT EXISTS gazette_matches ( id INTEGER PRIMARY KEY AUTOINCREMENT );`);
  // Add columns if missing
  const pragma = await db.all(`PRAGMA table_info(gazette_matches);`);
  const existing = new Set(pragma.map((c) => c.name));
  for (const col of REQUIRED_COLUMNS) {
    if (!existing.has(col.name)) {
      await db.exec(`ALTER TABLE gazette_matches ADD COLUMN ${col.name} ${col.type};`);
    }
  }
  await db.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS ux_gazette_unique
    ON gazette_matches (court_station, cause_no, name_norm, date_published, volume_no);
  `);
  return db;
}

export function normalizeName(name = "") {
  return String(name)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export async function clearMatches() {
  const db = await initDB();
  const info = await db.run(`DELETE FROM gazette_matches;`);
  await db.close();
  return info.changes ?? 0;
}

export async function saveMatchesToDB(matches, options = {}) {
  const batchSize = options.batchSize || 500;
  if (!Array.isArray(matches) || matches.length === 0) return [];

  const db = await initDB();
  const inserted = [];

  const insertSQL = `
    INSERT INTO gazette_matches
      (court_station, cause_no, name_norm, name_of_deceased, status_at_gp, volume_no, date_published)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(court_station, cause_no, name_norm, date_published, volume_no)
    DO UPDATE SET 
      status_at_gp = CASE 
        WHEN excluded.status_at_gp = 'Approved' THEN 'Approved'
        ELSE gazette_matches.status_at_gp
      END
  `;

  const stmt = await db.prepare(insertSQL);

  for (let i = 0; i < matches.length; i += batchSize) {
    const batch = matches.slice(i, i + batchSize);
    await db.exec("BEGIN TRANSACTION;");
    try {
      for (const m of batch) {
        const nameOriginal = m.name_of_deceased || "";
        const nameNorm = normalizeName(nameOriginal);
        const result = await stmt.run(
          m.court_station || "",
          m.cause_no || "",
          nameNorm,
          nameOriginal,
          m.status_at_gp || "Published",
          m.volume_no || "",
          m.date_published || ""
        );
        if (result.changes) inserted.push({ id: result.lastID, ...m });
      }
      await db.exec("COMMIT;");
    } catch (e) {
      console.error("❌ Batch insert failed:", e);
      await db.exec("ROLLBACK;");
    }
  }

  await stmt.finalize();
  await db.close();
  return inserted;
}
