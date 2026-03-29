const fs = require("fs/promises");
const path = require("path");
const {
  loadRawJobs,
  saveJobs,
  cleanupHistoricalJobs,
  filterDisabledSourceJobs,
} = require("../src/storage/fileStore");
const { exportJobsToXml } = require("../src/export/xmlExport");

const CLUBS_FILE = path.resolve(__dirname, "../src/config/clubs.json");
const ROOT_XML_FILE = path.resolve(__dirname, "../jobs.xml");

async function loadClubs() {
  const raw = await fs.readFile(CLUBS_FILE, "utf8");
  const parsed = JSON.parse(raw);
  return Array.isArray(parsed) ? parsed : [];
}

function parseArgs(argv) {
  const args = new Set(argv);
  return {
    write: args.has("--write"),
    check: args.has("--check") || !args.has("--write"),
  };
}

async function syncRootXml() {
  try {
    const publicXml = path.resolve(__dirname, "../public/jobs.xml");
    const xml = await fs.readFile(publicXml, "utf8");
    await fs.writeFile(ROOT_XML_FILE, xml, "utf8");
  } catch {
    // Ignore if the root XML is not part of the current workflow.
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const clubs = await loadClubs();
  const rawJobs = await loadRawJobs();
  const filtered = filterDisabledSourceJobs(rawJobs);
  const cleaned = cleanupHistoricalJobs(filtered.jobs, clubs);
  const removed = rawJobs.length - cleaned.jobs.length;
  const removedDisabled = filtered.removed_jobs.length;

  console.log(
    `[cleanup] mode=${options.write ? "write" : "check"} before=${rawJobs.length} after=${cleaned.jobs.length} removed=${removed}`
  );

  for (const report of filtered.reports) {
    if (!report || !report.removed_jobs) {
      continue;
    }

    console.log(
      `[cleanup] ${report.policy_key}: removed_disabled_jobs=${report.removed_jobs}`
    );
  }

  for (const report of cleaned.reports) {
    if (!report) {
      continue;
    }

    if (!report.removed_duplicates && !report.rewritten_ownership) {
      continue;
    }

    console.log(
      `[cleanup] ${report.club_id}: removed_duplicates=${report.removed_duplicates} rewritten_ownership=${report.rewritten_ownership}`
    );
  }

  if (!options.write) {
    return;
  }

  await saveJobs(cleaned.jobs);
  await exportJobsToXml(cleaned.jobs);
  await syncRootXml();

  console.log(
    `[cleanup] store and XML updated (removed_disabled_jobs=${removedDisabled})`
  );
}

main().catch((error) => {
  console.error(`[cleanup:error] ${error.message}`);
  process.exit(1);
});
