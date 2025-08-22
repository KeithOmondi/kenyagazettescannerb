// routes/reports.js
import express from "express";
import multer from "multer";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import xlsx from "xlsx";
import PDFDocument from "pdfkit";
import pdfParse from "pdf-parse"; // ✅ direct import
import { extractGazetteRecords } from "../helpers.js";

const router = express.Router();

// Setup upload dir
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const UPLOAD_DIR = path.join(__dirname, "../uploads");
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });
const upload = multer({ dest: UPLOAD_DIR });

/**
 * POST /reports/upload-multi
 * Accepts multiple gazette PDFs, extracts, and aggregates results
 */
router.post("/upload-multi", upload.array("pdfFiles", 20), async (req, res) => {
  if (!req.files || req.files.length === 0) {
    return res.status(400).json({ error: "No PDF files uploaded." });
  }

  try {
    let allRecords = [];

    for (const file of req.files) {
      const pdfBuffer = fs.readFileSync(file.path);
      const data = await pdfParse(pdfBuffer); // ✅ buffer only
      const records = extractGazetteRecords(data.text || "");
      allRecords = allRecords.concat(records);

      fs.unlinkSync(file.path); // cleanup
    }

    // Aggregate by Court
    const courtSummary = {};
    for (const rec of allRecords) {
      const court = rec.court_station || "Unknown Court";
      if (!courtSummary[court]) {
        courtSummary[court] = {
          totalCases: 0,
          dates: new Set(),
          volumes: new Set(),
        };
      }
      courtSummary[court].totalCases++;
      if (rec.date_published) courtSummary[court].dates.add(rec.date_published);
      if (rec.volume_no) courtSummary[court].volumes.add(rec.volume_no);
    }

    // Transform into clean array
    const results = Object.entries(courtSummary).map(([court, val]) => {
      const datesArr = Array.from(val.dates).sort(
        (a, b) => new Date(a) - new Date(b)
      );
      const dateRange =
        datesArr.length > 1
          ? `${datesArr[0]} – ${datesArr[datesArr.length - 1]}`
          : datesArr[0] || "N/A";

      return {
        court,
        totalCases: val.totalCases,
        dateRange,
        volumes: Array.from(val.volumes).join(", "),
      };
    });

    res.json({ totalRecords: allRecords.length, courts: results });
  } catch (err) {
    console.error("❌ Multi-PDF processing error:", err);
    res.status(500).json({ error: "Failed to process multiple PDFs" });
  }
});

/**
 * GET /reports/excel
 */
router.get("/excel", (req, res) => {
  try {
    const sample = [
      {
        court: "High Court Nairobi",
        totalCases: 10000,
        dateRange: "Jan – Feb 2025",
        volumes: "Vol. CXXVII—No.146, Vol. CXXVIII—No.147",
      },
    ];

    const worksheet = xlsx.utils.json_to_sheet(sample);
    const workbook = xlsx.utils.book_new();
    xlsx.utils.book_append_sheet(workbook, worksheet, "Report");

    const buffer = xlsx.write(workbook, { type: "buffer", bookType: "xlsx" });
    res.setHeader("Content-Disposition", "attachment; filename=report.xlsx");
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.send(buffer);
  } catch (err) {
    console.error("❌ Error generating Excel:", err);
    res.status(500).json({ error: "Failed to generate Excel" });
  }
});

/**
 * GET /reports/pdf
 */
router.get("/pdf", (req, res) => {
  try {
    const sample = [
      {
        court: "High Court Nairobi",
        totalCases: 10000,
        dateRange: "Jan – Feb 2025",
        volumes: "Vol. CXXVII—No.146, Vol. CXXVIII—No.147",
      },
    ];

    res.setHeader("Content-Disposition", "attachment; filename=report.pdf");
    res.setHeader("Content-Type", "application/pdf");

    const doc = new PDFDocument();
    doc.pipe(res);
    doc.fontSize(18).text("Court Report", { align: "center" }).moveDown();

    sample.forEach((row) => {
      doc
        .fontSize(14)
        .text(`Court: ${row.court}`, { underline: true })
        .moveDown(0.3);
      doc.fontSize(12).text(` Total Cases: ${row.totalCases}`);
      doc.fontSize(12).text(` Dates: ${row.dateRange}`);
      doc.fontSize(12).text(` Volumes: ${row.volumes}`);
      doc.moveDown();
    });

    doc.end();
  } catch (err) {
    console.error("❌ Error generating PDF:", err);
    res.status(500).json({ error: "Failed to generate PDF" });
  }
});

export default router;
