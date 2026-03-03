const fs = require("fs/promises");
const path = require("path");

const DATA_FILE = path.resolve(__dirname, "../../data/jobs.json");

function buildDedupeKey(job) {
  return `${job.club_id || ""}::${job.source_id || ""}`;
}

async function ensureStoreFile() {
  await fs.mkdir(path.dirname(DATA_FILE), { recursive: true });
  try {
    await fs.access(DATA_FILE);
  } catch {
    await fs.writeFile(DATA_FILE, "[]\n", "utf8");
  }
}

async function loadJobs() {
  await ensureStoreFile();

  try {
    const raw = await fs.readFile(DATA_FILE, "utf8");
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

async function saveJobs(jobs) {
  await ensureStoreFile();
  await fs.writeFile(DATA_FILE, `${JSON.stringify(jobs, null, 2)}\n`, "utf8");
}

async function upsertJobs(incomingJobs) {
  const existingJobs = await loadJobs();
  const map = new Map();

  for (const job of existingJobs) {
    if (job && job.club_id && job.source_id) {
      map.set(buildDedupeKey(job), job);
    }
  }

  for (const job of incomingJobs) {
    if (job && job.club_id && job.source_id) {
      map.set(buildDedupeKey(job), job);
    }
  }

  const merged = Array.from(map.values()).sort((a, b) => {
    const keyA = buildDedupeKey(a);
    const keyB = buildDedupeKey(b);
    return keyA.localeCompare(keyB);
  });

  await saveJobs(merged);
  return merged;
}

module.exports = {
  DATA_FILE,
  loadJobs,
  saveJobs,
  upsertJobs,
};
