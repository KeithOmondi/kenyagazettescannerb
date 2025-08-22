// Normalization & tokenization
export const normalizeName = (name = "") =>
  String(name)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

export const tokenizeName = (name = "") =>
  normalizeName(name).split(" ").filter(Boolean).sort().join(" ");

// Excel header normalization and best key finder
const normKey = (k = "") =>
  String(k)
    .toLowerCase()
    .replace(/’/g, "'")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

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
  ];
  for (const key of candidates) if (map[key]) return String(map[key]).trim();

  for (const [k, v] of Object.entries(map)) {
    if (k.includes("deceased") && v) return String(v).trim();
  }
  return "";
}

// Jaccard similarity for token sets
export function jaccard(tokensA, tokensB) {
  const setA = new Set(tokensA.filter(Boolean));
  const setB = new Set(tokensB.filter(Boolean));
  const inter = [...setA].filter((x) => setB.has(x)).length;
  const union = new Set([...setA, ...setB]).size || 1;
  return inter / union;
}

// Jaro-Winkler similarity (0..1)
export function jaroWinkler(s1 = "", s2 = "") {
  if (!s1 && !s2) return 1;
  if (!s1 || !s2) return 0;

  const m = matchingCharacters(s1, s2);
  if (m === 0) return 0;

  const t = transpositions(s1, s2, m);
  const jaro = (m / s1.length + m / s2.length + (m - t) / m) / 3;

  // Winkler boost for common prefix up to 4 chars
  const prefixLen = commonPrefixLen(s1, s2, 4);
  const p = 0.1; // standard scaling factor
  return jaro + prefixLen * p * (1 - jaro);
}

function matchingCharacters(s1, s2) {
  const matchWindow = Math.max(
    0,
    Math.floor(Math.max(s1.length, s2.length) / 2) - 1
  );
  const s2Matches = new Array(s2.length).fill(false);
  let matches = 0;

  for (let i = 0; i < s1.length; i++) {
    const start = Math.max(0, i - matchWindow);
    const end = Math.min(i + matchWindow + 1, s2.length);
    for (let j = start; j < end; j++) {
      if (!s2Matches[j] && s1[i] === s2[j]) {
        s2Matches[j] = true;
        matches++;
        break;
      }
    }
  }
  return matches;
}

function transpositions(s1, s2, matches) {
  const matchWindow = Math.max(
    0,
    Math.floor(Math.max(s1.length, s2.length) / 2) - 1
  );
  const s2Matches = new Array(s2.length).fill(false);
  const s1Matched = [];
  const s2Matched = [];

  for (let i = 0; i < s1.length; i++) {
    const start = Math.max(0, i - matchWindow);
    const end = Math.min(i + matchWindow + 1, s2.length);
    for (let j = start; j < end; j++) {
      if (!s2Matches[j] && s1[i] === s2[j]) {
        s2Matches[j] = true;
        s1Matched.push(s1[i]);
        break;
      }
    }
  }

  for (let j = 0; j < s2.length; j++) if (s2Matches[j]) s2Matched.push(s2[j]);

  let transpositions = 0;
  for (let i = 0; i < s1Matched.length; i++)
    if (s1Matched[i] !== s2Matched[i]) transpositions++;
  return transpositions / 2;
}

function commonPrefixLen(s1, s2, max = 4) {
  let n = 0;
  while (n < max && n < s1.length && n < s2.length && s1[n] === s2[n]) n++;
  return n;
}
