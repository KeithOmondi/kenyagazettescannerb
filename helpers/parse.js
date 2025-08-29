// Gazette parsing helpers
let currentStation = "";


for (let i = 0; i < lines.length; i++) {
const line = lines[i];


const st = line.match(/IN THE\s+(HIGH COURT|MAGISTRATES? COURT|CHIEF MAGISTRATE[’']?S COURT)\s+(?:OF KENYA\s+)?AT\s+([A-Z][A-Za-z\s-]+)/i);
if (st) {
const typeRaw = st[1].toUpperCase();
const type = typeRaw.includes("HIGH") ? "High Court" : "Magistrates Court";
const loc = toTitle(st[2].replace(/[^A-Za-z\s-]/g, " "));
currentStation = `${loc} ${type}`;
continue;
}


const cause = line.match(/\bCAUSE\s+NO\.?\s*([A-Za-z-]*\s*\d+(?:\s*OF\s*)?\s*\d{4}|[A-Za-z0-9]+\/\d{4}|[A-Za-z0-9-]+)/i);
if (!cause) continue;


let estateBlock = "";
for (let j = 0; j <= 5; j++) {
const ln = lines[i + j] || "";
if (/(ESTATE\s+OF)/i.test(ln)) {
estateBlock = [lines[i + j], lines[i + j + 1], lines[i + j + 2]].filter(Boolean).join(" ");
break;
}
}
if (!estateBlock) continue;


let name = estateBlock
.replace(/.*ESTATE\s+OF\s*/i, "")
.replace(/\(?\s*DECEASED\s*\)?/gi, "")
.replace(/\b(THE|LATE)\b/gi, "")
.trim();


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


return records;
