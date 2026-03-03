const fs = require("fs/promises");
const path = require("path");
const { loadJobs } = require("../storage/fileStore");

const OUTPUT_FILE = path.resolve(__dirname, "../../public/jobs.xml");

function normalizeText(value) {
  return String(value || "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function sanitizeTitle(value) {
  const title = normalizeText(value);
  if (!title) {
    return "";
  }

  return title.replace(/^vacancies\b[\s:\-–—]*/i, "").trim();
}

function slugify(value) {
  return normalizeText(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function escapeXml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function escapeForCdata(value) {
  return String(value || "").replace(/]]>/g, "]]]]><![CDATA[>");
}

function buildHtmlDescription(job) {
  if (job.html_description && job.html_description.trim()) {
    return job.html_description.trim();
  }

  const plain = escapeXml(job.plain_text_description || "");
  return `<p>${plain}</p>`;
}

function serializeJob(job) {
  const htmlDescription = escapeForCdata(buildHtmlDescription(job));
  const id = normalizeText(job.id || job.source_id || "");
  const title = sanitizeTitle(job.title || "") || normalizeText(job.title || "");
  const guid = `${id}-${slugify(title || id)}`;

  return [
    " <job>",
    `  <guid isPermaLink="false">${escapeXml(guid)}</guid>`,
    `  <id>${escapeXml(id)}</id>`,
    `  <url>${escapeXml(job.url)}</url>`,
    `  <application_link>${escapeXml(job.application_link)}</application_link>`,
    `  <title>${escapeXml(title)}</title>`,
    `  <arrangement>${escapeXml(job.arrangement || "fulltime")}</arrangement>`,
    `  <location_type>${escapeXml(job.location_type || "onsite")}</location_type>`,
    `  <location>${escapeXml(job.location)}</location>`,
    "  <location_limits><location>Worldwide</location></location_limits>",
    `  <published_at>${escapeXml(job.published_at)}</published_at>`,
    `  <expires_at>${escapeXml(job.expires_at)}</expires_at>`,
    "  <highlighted>false</highlighted>",
    "  <sticky>false</sticky>",
    `  <html_description><![CDATA[ ${htmlDescription} ]]></html_description>`,
    `  <plain_text_description>${escapeXml(job.plain_text_description)}</plain_text_description>`,
    `  <company_name>${escapeXml(job.company_name)}</company_name>`,
    `  <company_url>${escapeXml(job.company_url)}</company_url>`,
    `  <company_logo_url>${escapeXml(job.company_logo_url)}</company_logo_url>`,
    "  <salary_minimum/>",
    "  <salary_maximum/>",
    "  <salary_schedule/>",
    "  <salary_currency/>",
    " </job>",
  ].join("\n");
}

async function exportJobsToXml(jobsInput) {
  const jobs = Array.isArray(jobsInput) ? jobsInput : await loadJobs();
  const xmlJobs = jobs.map((job) => serializeJob(job)).join("\n");
  const xml = `<jobs>\n${xmlJobs ? `${xmlJobs}\n` : ""}</jobs>\n`;

  await fs.mkdir(path.dirname(OUTPUT_FILE), { recursive: true });
  await fs.writeFile(OUTPUT_FILE, xml, "utf8");

  return OUTPUT_FILE;
}

module.exports = {
  OUTPUT_FILE,
  exportJobsToXml,
};
