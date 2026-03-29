const fs = require("fs/promises");
const path = require("path");
const { loadJobs } = require("./storage/fileStore");
const { exportJobsToXml } = require("./export/xmlExport");
const {
  isDisabledSourceEntry,
  findDisabledSourcePolicy,
  describeDisabledSource,
} = require("./config/disabledSources");

const CLUBS_FILE = path.resolve(__dirname, "./config/clubs.json");

function installEpipeGuard(stream) {
  if (!stream || typeof stream.on !== "function") {
    return;
  }

  stream.on("error", (error) => {
    if (error && error.code === "EPIPE") {
      process.exit(0);
    }
  });
}

installEpipeGuard(process.stdout);
installEpipeGuard(process.stderr);

async function loadClubs() {
  const raw = await fs.readFile(CLUBS_FILE, "utf8");
  const parsed = JSON.parse(raw);
  const clubs = Array.isArray(parsed) ? parsed : [];

  return clubs.filter((club) => {
    if (!isDisabledSourceEntry(club)) {
      return true;
    }

    const policy = findDisabledSourcePolicy(club);
    const label = club.club_id || club.name || "unknown";
    console.warn(`[skip] ${label}: ${describeDisabledSource(policy)}`);
    return false;
  });
}

async function runCrawl() {
  const { crawlClubs } = require("./core/crawler");
  const clubs = await loadClubs();
  const { scrapedJobs, storedJobs } = await crawlClubs(clubs);
  console.log(
    `[done] crawl completo. scraped=${scrapedJobs.length} | almacenados=${storedJobs.length}`
  );
}

async function runXmlExport() {
  const jobs = await loadJobs();
  const outputPath = await exportJobsToXml(jobs);
  console.log(`[done] XML exportado: ${outputPath} (${jobs.length} jobs)`);
}

async function runAll() {
  await runCrawl();
  await runXmlExport();
}

async function main() {
  const command = String(process.argv[2] || "run").toLowerCase();

  if (command === "crawl") {
    await runCrawl();
    return;
  }

  if (command === "export:xml") {
    await runXmlExport();
    return;
  }

  if (command === "run") {
    await runAll();
    return;
  }

  throw new Error(`Comando no soportado: ${command}`);
}

main().catch((error) => {
  console.error(`[error] ${error.message}`);
  process.exit(1);
});
