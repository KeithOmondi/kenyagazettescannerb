// Gazette parsing helpers
const toTitle = (s = "") =>
  s
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase())
    .trim();

function extractHeaderInfo(lines) {
  let volumeNo = "";
  let datePublished = "";

  const header = lines.slice(0, Math.min(1000, lines.length));
  const volRegex = /Vol\.?\s*[IVXLCDM0-9]+\s*[-–—]?\s*No\.?\s*\d+/i;
  const dateRegex = /(\d{1,2}(?:st|nd|rd|th)?\s+[A-Z][a-z]+\s*,?\s+\d{4})/i;

  let volIdx = -1;
  for (let i = 0; i < header.length; i++) {
    const m = header[i].match(volRegex);
    if (m) {
      volumeNo = m[0].replace(/\s+/g, " ").trim();
      volIdx = i;
      break;
    }
  }

  if (volIdx >= 0) {
    const start = Math.max(0, volIdx - 5);
    const end = Math.min(header.length - 1, volIdx + 8);
    for (let i = start; i <= end; i++) {
      const dm = header[i].match(dateRegex);
      if (dm) {
        datePublished = dm[1].replace(",", "").trim();
        break;
      }
    }
  }

  if (!datePublished) {
    for (let i = 0; i < header.length; i++) {
      const dm = header[i].match(dateRegex);
      if (dm) {
        datePublished = dm[1].replace(",", "").trim();
        break;
      }
    }
  }

  if (volumeNo) volumeNo = volumeNo.replace(/\s+/g, " ").trim();
  return { volumeNo, datePublished };
}

export function extractGazetteRecords(pdfText) {
  const raw = String(pdfText || "").replace(/\u00a0/g, " ");
  const lines = raw.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);

  const { volumeNo, datePublished } = extractHeaderInfo(lines);

  const records = [];
  let currentStation = "";

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Court station
    const st = line.match(
      /IN THE\s+(HIGH COURT|MAGISTRATES? COURT|CHIEF MAGISTRATE[’']?S COURT)\s+(?:OF KENYA\s+)?AT\s+([A-Z][A-Za-z\s-]+)/i
    );
    if (st) {
      const typeRaw = st[1].toUpperCase();
      const type = typeRaw.includes("HIGH") ? "High Court" : "Magistrates Court";
      const loc = toTitle(st[2].replace(/[^A-Za-z\s-]/g, " "));
      currentStation = `${loc} ${type}`;
      continue;
    }

    // Cause number
    const cause = line.match(
      /\bCAUSE\s+NO\.?\s*([A-Za-z-]*\s*\d+(?:\s*OF\s*)?\s*\d{4}|[A-Za-z0-9]+\/\d{4}|[A-Za-z0-9-]+)/i
    );
    if (!cause) continue;

    // Estate name block
    let estateBlock = "";
    for (let j = 0; j <= 5; j++) {
      const ln = lines[i + j] || "";
      if (/(ESTATE\s+OF)/i.test(ln)) {
        estateBlock = [lines[i + j], lines[i + j + 1], lines[i + j + 2]]
          .filter(Boolean)
          .join(" ");
        break;
      }
    }
    if (!estateBlock) continue;

    let name = estateBlock
      .replace(/.*ESTATE\s+OF\s*/i, "")
      .replace(/\(?\s*DECEASED\s*\)?/gi, "")
      .replace(/\b(THE|LATE)\b/gi, "")
      .trim();

    // Cleanup
    name = name.split(/who\s+died/i)[0];
    name = name.split(",")[0];
    name = name.replace(/\bof\s+[A-Z][A-Za-z\s]+$/i, "").trim();

    if (!name) continue;

    records.push({
      court_station: currentStation || "Unknown Court",
      cause_no: cause[1].replace(/\s+/g, " ").trim(),
      name_of_deceased: name,
      status_at_gp: "Published",
      volume_no: volumeNo || "",
      date_published: datePublished || ""
    });
  }

  // console.log(`✅ Extracted ${records.length} Gazette records`);
  return records;
}
