const fs = require("fs/promises");
const path = require("path");
const { loadJobs } = require("./storage/fileStore");
const { exportJobsToXml } = require("./export/xmlExport");

const CLUBS_FILE = path.resolve(__dirname, "./config/clubs.json");

async function loadClubs() {
  const raw = await fs.readFile(CLUBS_FILE, "utf8");
  const parsed = JSON.parse(raw);
  return Array.isArray(parsed) ? parsed : [];
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
