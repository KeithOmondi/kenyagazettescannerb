// utils/normalize.js

// Base normalizer
export const normalizeName = (name = "") =>
  String(name)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

// Strip titles / estate phrases / trailing "of <place>"
export const normalizeCanonical = (name = "") => {
  let s = normalizeName(name);
  s = s.replace(/\b(dr|mr|mrs|ms|miss|rev|prof|eng)\b/g, " ");
  s = s.replace(/\b(the\s+late|late|estate\s+of|of\s+the\s+estate\s+of)\b/g, " ");
  s = s.replace(/\bof\s+[a-z][a-z\s-]*$/g, " ");
  return s.replace(/\s+/g, " ").trim();
};

// Canonical tokens in alpha order (deterministic)
export const tokenizeName = (name = "") =>
  normalizeCanonical(name).split(" ").filter(Boolean).sort().join(" ");

// DB key — same as canonical tokens (stable match key)
export const normalizeNameDB = (name = "") => tokenizeName(name);

// For set operations
export const tokensAsSet = (name = "") =>
  new Set(tokenizeName(name).split(" ").filter(Boolean));

// Flexible header normalization for Excel-like columns
const normKey = (k = "") =>
  String(k)
    .toLowerCase()
    .replace(/’/g, "'")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

// Try to find the "deceased name" column in arbitrary Excel headers
export function bestExcelNameKey(row) {
  if (!row || typeof row !== "object") return "";
  const map = {};
  for (const [k, v] of Object.entries(row)) map[normKey(k)] = v;

  const candidates = [
    "name of the deceased",
    "name of deceased",
    "name deceased",
    "deceased name",
    "deceased s name",
    "name deceased s",
    "name (deceased)",
    "full name",
    "fullname",
    "deceased",
    "name",
  ];
  for (const key of candidates) if (map[key]) return String(map[key]).trim();
  for (const [k, v] of Object.entries(map))
    if (k.includes("deceased") && v) return String(v).trim();
  return "";
}

// Proper Jaccard similarity
export function jaccard(tokensA = [], tokensB = []) {
  const setA = new Set(tokensA.filter(Boolean));
  const setB = new Set(tokensB.filter(Boolean));
  const inter = [...setA].filter((x) => setB.has(x)).length;
  const union = new Set([...setA, ...setB]).size || 1;
  return inter / union;
}
