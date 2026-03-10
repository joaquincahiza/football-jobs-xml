const { chromium } = require("playwright");
const cheerio = require("cheerio");

const RATE_LIMIT_MS = 1000;
const RETRIES = 2;
const MAX_DISCOVERY_PAGES = 10;

const BLOCK_TAGS = new Set([
  "p",
  "div",
  "section",
  "article",
  "blockquote",
  "pre",
  "h1",
  "h2",
  "h3",
  "h4",
  "h5",
  "h6",
]);

const MONTHS = {
  january: 1,
  february: 2,
  march: 3,
  april: 4,
  may: 5,
  june: 6,
  july: 7,
  august: 8,
  september: 9,
  october: 10,
  november: 11,
  december: 12,
};

function normalizeText(value) {
  return String(value || "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeMultilineText(value) {
  const lines = String(value || "")
    .replace(/\u00a0/g, " ")
    .replace(/\r/g, "")
    .split("\n")
    .map((line) => line.replace(/[ \t]+/g, " ").trim());

  while (lines.length && !lines[0]) {
    lines.shift();
  }

  while (lines.length && !lines[lines.length - 1]) {
    lines.pop();
  }

  const compact = [];
  let previousEmpty = false;

  for (const line of lines) {
    if (!line) {
      if (!previousEmpty) {
        compact.push("");
      }
      previousEmpty = true;
      continue;
    }

    compact.push(line);
    previousEmpty = false;
  }

  return compact.join("\n");
}

function toSingleLine(value) {
  return normalizeMultilineText(value).replace(/\n+/g, " ").trim();
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

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function escapeRegExp(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function canonicalizeUrl(baseUrl, value, options = {}) {
  if (!value) {
    return "";
  }

  try {
    const url = new URL(value, baseUrl);

    if (options.dropHash !== false) {
      url.hash = "";
    }

    if (options.dropSearch) {
      url.search = "";
    }

    return url.href;
  } catch {
    return "";
  }
}

function normalizeIdentifier(value) {
  const text = normalizeText(value);
  if (!text) {
    return "";
  }

  const match = text.match(/(\d{3,})/);
  if (match) {
    return match[1];
  }

  return text
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function extractSourceIdFromUrl(jobUrl) {
  try {
    const parsed = new URL(jobUrl);
    const segments = parsed.pathname
      .split("/")
      .filter(Boolean)
      .map((segment) => decodeURIComponent(segment));

    for (let i = segments.length - 1; i >= 0; i -= 1) {
      const cleaned = segments[i].replace(/\.[a-z0-9]+$/i, "");
      if (
        !cleaned ||
        /^jobs?$/i.test(cleaned) ||
        /^careers?$/i.test(cleaned) ||
        /^positions?$/i.test(cleaned) ||
        /^vacanc(?:y|ies)$/i.test(cleaned)
      ) {
        continue;
      }

      const normalized = normalizeIdentifier(cleaned);
      if (normalized) {
        return normalized;
      }
    }
  } catch {
    // Ignore URL parsing errors.
  }

  return "";
}

function parseDayMonthYear(text) {
  const match = String(text || "").match(
    /(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+),?\s+(\d{4})/
  );

  if (!match) {
    return null;
  }

  const day = Number(match[1]);
  const month = MONTHS[String(match[2] || "").toLowerCase()];
  const year = Number(match[3]);

  if (!day || !month || !year) {
    return null;
  }

  return { day, month, year };
}

function formatIsoDateAtMidnightUtc(year, month, day) {
  const yyyy = String(year).padStart(4, "0");
  const mm = String(month).padStart(2, "0");
  const dd = String(day).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}T00:00:00Z`;
}

function parseDateToIso(value) {
  const cleaned = normalizeText(value)
    .replace(/^posted on\s*/i, "")
    .replace(/^closing date\s*/i, "");

  if (!cleaned) {
    return "";
  }

  const dayMonthYear = parseDayMonthYear(cleaned);
  if (dayMonthYear) {
    return formatIsoDateAtMidnightUtc(
      dayMonthYear.year,
      dayMonthYear.month,
      dayMonthYear.day
    );
  }

  const slashDate = cleaned.match(
    /^(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})(?:\D.*)?$/
  );
  if (slashDate) {
    const day = Number(slashDate[1]);
    const month = Number(slashDate[2]);
    let year = Number(slashDate[3]);
    if (year < 100) {
      year += 2000;
    }
    return formatIsoDateAtMidnightUtc(year, month, day);
  }

  const parsed = new Date(cleaned);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }

  const hasTime =
    /t\d{1,2}:\d{2}/i.test(cleaned) || /\b\d{1,2}:\d{2}\b/.test(cleaned);
  const hasZone = /z$|[+\-]\d{2}:?\d{2}$/i.test(cleaned);

  if (hasTime || hasZone) {
    return parsed.toISOString();
  }

  return formatIsoDateAtMidnightUtc(
    parsed.getUTCFullYear(),
    parsed.getUTCMonth() + 1,
    parsed.getUTCDate()
  );
}

function extractInlineText($, node) {
  let output = "";

  $(node)
    .contents()
    .each((_, child) => {
      if (child.type === "text") {
        output += child.data || "";
        return;
      }

      if (child.type !== "tag") {
        return;
      }

      const tag = String(child.name || "").toLowerCase();
      if (tag === "script" || tag === "style" || tag === "noscript") {
        return;
      }

      if (tag === "br") {
        output += "\n";
        return;
      }

      output += extractInlineText($, child);
    });

  return normalizeMultilineText(output);
}

function extractListBlock($, listNode) {
  const lines = [];

  $(listNode)
    .children("li")
    .each((_, liNode) => {
      const line = toSingleLine(extractInlineText($, liNode));
      if (line) {
        lines.push(`- ${line}`);
      }
    });

  return lines.join("\n");
}

function pushBlock(blocks, value) {
  const cleaned = normalizeMultilineText(value);
  if (cleaned) {
    blocks.push(cleaned);
  }
}

function extractBlocksFromNode($, node, blocks) {
  if (node.type === "text") {
    pushBlock(blocks, node.data || "");
    return;
  }

  if (node.type !== "tag") {
    return;
  }

  const tag = String(node.name || "").toLowerCase();

  if (tag === "script" || tag === "style" || tag === "noscript") {
    return;
  }

  if (tag === "ul" || tag === "ol") {
    pushBlock(blocks, extractListBlock($, node));
    return;
  }

  if (tag === "li") {
    const line = toSingleLine(extractInlineText($, node));
    if (line) {
      blocks.push(`- ${line}`);
    }
    return;
  }

  if (BLOCK_TAGS.has(tag)) {
    const hasBlockChildren =
      $(node)
        .children()
        .filter((_, child) => {
          const childTag = String(child.name || "").toLowerCase();
          return BLOCK_TAGS.has(childTag) || childTag === "ul" || childTag === "ol";
        }).length > 0;

    if (hasBlockChildren) {
      $(node)
        .contents()
        .each((_, child) => extractBlocksFromNode($, child, blocks));
      return;
    }

    pushBlock(blocks, extractInlineText($, node));
    return;
  }

  $(node)
    .contents()
    .each((_, child) => extractBlocksFromNode($, child, blocks));
}

function htmlToStructuredPlainText(html) {
  const fragment = String(html || "").trim();
  if (!fragment) {
    return "";
  }

  const $ = cheerio.load(`<div id="__plain_root__">${fragment}</div>`);
  const blocks = [];

  $("#__plain_root__")
    .contents()
    .each((_, node) => extractBlocksFromNode($, node, blocks));

  return blocks.filter(Boolean).join("\n\n");
}

function plainTextToHtml(text) {
  const cleaned = normalizeMultilineText(text);
  if (!cleaned) {
    return "";
  }

  return cleaned
    .split(/\n{2,}/)
    .map((paragraph) => paragraph.trim())
    .filter(Boolean)
    .map((paragraph) => `<p>${escapeHtml(paragraph).replace(/\n/g, "<br/>")}</p>`)
    .join("\n");
}

function normalizeHtmlFragment(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }

  const hasHtmlTags = /<([a-z][a-z0-9]*)\b[^>]*>/i.test(raw);
  const hasEscapedTags = /&lt;\/?[a-z][^&]*&gt;/i.test(raw);

  if (!hasHtmlTags && hasEscapedTags) {
    const $ = cheerio.load(`<div id="__decode__">${raw}</div>`);
    return String($("#__decode__").text() || "").trim();
  }

  return raw;
}

function parseJsonLdPayload(raw) {
  const source = String(raw || "")
    .replace(/^\s*<!--/, "")
    .replace(/-->\s*$/, "")
    .trim();

  if (!source) {
    return null;
  }

  try {
    return JSON.parse(source);
  } catch {
    return null;
  }
}

function flattenJsonLd(value, items) {
  if (!value) {
    return;
  }

  if (Array.isArray(value)) {
    for (const entry of value) {
      flattenJsonLd(entry, items);
    }
    return;
  }

  if (typeof value !== "object") {
    return;
  }

  items.push(value);

  if (value["@graph"]) {
    flattenJsonLd(value["@graph"], items);
  }
}

function hasJobPostingType(typeValue) {
  if (!typeValue) {
    return false;
  }

  if (Array.isArray(typeValue)) {
    return typeValue.some((value) =>
      String(value || "").toLowerCase().includes("jobposting")
    );
  }

  return String(typeValue).toLowerCase().includes("jobposting");
}

function extractJobPostingJsonLd($) {
  const scripts = $('script[type*="ld+json"]');
  const items = [];

  scripts.each((_, el) => {
    const payload = parseJsonLdPayload($(el).contents().text());
    if (payload) {
      flattenJsonLd(payload, items);
    }
  });

  return (
    items.find((item) => item && hasJobPostingType(item["@type"])) || null
  );
}

function extractIdentifierFromJsonLd(jobPosting) {
  if (!jobPosting || typeof jobPosting !== "object") {
    return "";
  }

  const { identifier, jobId } = jobPosting;
  const value = identifier || jobId;

  if (Array.isArray(value)) {
    for (const entry of value) {
      const extracted = extractIdentifierFromJsonLd({ identifier: entry });
      if (extracted) {
        return extracted;
      }
    }
    return "";
  }

  if (typeof value === "string" || typeof value === "number") {
    return normalizeIdentifier(value);
  }

  if (value && typeof value === "object") {
    return normalizeIdentifier(
      value.value || value.name || value["@id"] || value.identifier
    );
  }

  return "";
}

function extractLocationFromJsonLd(jobPosting) {
  if (!jobPosting || typeof jobPosting !== "object") {
    return "";
  }

  const source = jobPosting.jobLocation || jobPosting.location || "";
  const locations = [];

  const visit = (value) => {
    if (!value) {
      return;
    }

    if (Array.isArray(value)) {
      for (const entry of value) {
        visit(entry);
      }
      return;
    }

    if (typeof value === "string") {
      const normalized = normalizeText(value);
      if (normalized) {
        locations.push(normalized);
      }
      return;
    }

    if (typeof value === "object") {
      if (value.address) {
        visit(value.address);
      }

      const parts = [
        value.name,
        value.streetAddress,
        value.addressLocality,
        value.addressRegion,
        value.postalCode,
        value.addressCountry,
      ]
        .map((part) => normalizeText(part))
        .filter(Boolean);

      if (parts.length) {
        locations.push(parts.join(", "));
      }
    }
  };

  visit(source);

  return Array.from(new Set(locations)).join(" | ");
}

function extractEmploymentTypeFromJsonLd(jobPosting) {
  if (!jobPosting || typeof jobPosting !== "object") {
    return "";
  }

  const source = jobPosting.employmentType || jobPosting.jobType || "";
  if (Array.isArray(source)) {
    return source.map((value) => normalizeText(value)).filter(Boolean).join(", ");
  }

  return normalizeText(source);
}

function extractMetaDescription($) {
  return normalizeText(
    $('meta[name="description"]').attr("content") ||
      $('meta[property="og:description"]').attr("content") ||
      ""
  );
}

function selectDescriptionHtml($) {
  const selectors = [
    '[itemprop="description"]',
    '[data-testid*="description"]',
    ".job-description",
    ".description",
    '[class*="job-description"]',
    "main article",
    "article",
    "main",
    "#main-content",
  ];

  for (const selector of selectors) {
    const node = $(selector).first();
    if (!node.length) {
      continue;
    }

    const text = normalizeText(node.text());
    if (!text) {
      continue;
    }

    const html = String(node.html() || "").trim();
    if (html) {
      return html;
    }
  }

  const metaDescription = extractMetaDescription($);
  if (metaDescription) {
    return `<p>${escapeHtml(metaDescription)}</p>`;
  }

  return "";
}

function findValueByLabels($, labels) {
  const normalizedLabels = Array.isArray(labels)
    ? labels.map((label) => normalizeText(label).toLowerCase()).filter(Boolean)
    : [];

  if (!normalizedLabels.length) {
    return "";
  }

  let value = "";

  $("dt,th").each((_, el) => {
    if (value) {
      return;
    }

    const label = normalizeText($(el).text()).toLowerCase().replace(/:$/, "");
    if (!normalizedLabels.includes(label)) {
      return;
    }

    const neighbor =
      $(el).next("dd").text() || $(el).next("td").text() || $(el).parent().find("td").eq(0).text();
    value = normalizeText(neighbor);
  });

  if (value) {
    return value;
  }

  for (const label of normalizedLabels) {
    const pattern = new RegExp(`^${escapeRegExp(label)}\\s*:?\\s*(.+)$`, "i");

    $("li,p,div,span").each((_, el) => {
      if (value) {
        return;
      }

      const text = normalizeText($(el).text());
      const match = text.match(pattern);
      if (match) {
        value = normalizeText(match[1]);
      }
    });

    if (value) {
      return value;
    }
  }

  return "";
}

function mapArrangementFromEmploymentType(employmentType) {
  const normalized = normalizeText(employmentType).toLowerCase();

  if (
    normalized.includes("contract") ||
    normalized.includes("fixed") ||
    normalized.includes("temporary") ||
    normalized.includes("temp")
  ) {
    return "contract";
  }

  if (
    normalized.includes("casual") ||
    normalized.includes("part-time") ||
    normalized.includes("part time")
  ) {
    return "parttime";
  }

  return "fulltime";
}

function normalizeJobRecord(club, rawJob) {
  const input = rawJob || {};
  const url = normalizeText(input.url || input.job_url || input.application_link);
  const sourceId = normalizeText(
    input.source_id ||
      input.id ||
      extractIdentifierFromJsonLd(input._jobPosting) ||
      extractSourceIdFromUrl(url)
  );

  if (!sourceId) {
    return null;
  }

  const titleCandidate = normalizeText(input.title || input.position || "");
  const title = sanitizeTitle(titleCandidate) || titleCandidate || `job-${sourceId}`;
  const plainTextDescription =
    normalizeMultilineText(input.plain_text_description) ||
    htmlToStructuredPlainText(input.html_description);
  const htmlDescription =
    String(input.html_description || "").trim() || plainTextToHtml(plainTextDescription);
  const employmentType = normalizeText(
    input.employment_type || input.contract_type || ""
  );
  const arrangementCandidate =
    normalizeText(input.arrangement).toLowerCase() ||
    mapArrangementFromEmploymentType(employmentType);
  const arrangement = ["fulltime", "parttime", "casual", "contract"].includes(
    arrangementCandidate
  )
    ? arrangementCandidate
    : "fulltime";
  const id = normalizeText(input.id || sourceId) || sourceId;

  return {
    club_id: club.club_id,
    club: normalizeText(input.club || club.name || ""),
    source_id: sourceId,
    id,
    guid: `${id}-${slugify(title)}`,
    url,
    application_link: normalizeText(input.application_link || url),
    title,
    arrangement,
    location_type: normalizeText(input.location_type || "onsite") || "onsite",
    location: normalizeText(input.location || ""),
    published_at: parseDateToIso(input.published_at || input.posted_on || ""),
    expires_at: parseDateToIso(input.expires_at || input.closing_date || ""),
    highlighted: false,
    sticky: false,
    html_description: htmlDescription,
    plain_text_description: plainTextDescription,
    department: normalizeText(input.department || input.team || ""),
    company_name: normalizeText(input.company_name || club.name || ""),
    company_url: normalizeText(input.company_url || club.company_url || club.source_url || ""),
    company_logo_url: String(
      input.company_logo_url !== undefined
        ? input.company_logo_url
        : club.company_logo_url || ""
    ),
    source_url: normalizeText(input.source_url || club.source_url || ""),
    ats: normalizeText(input.ats || club.source_type || ""),
    _meta: input._meta || undefined,
  };
}

async function gotoWithRetry(page, url) {
  for (let attempt = 0; attempt <= RETRIES; attempt += 1) {
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 });
      await page.waitForTimeout(RATE_LIMIT_MS);
      return;
    } catch (error) {
      if (attempt === RETRIES) {
        throw error;
      }
      await page.waitForTimeout(RATE_LIMIT_MS);
    }
  }
}

async function createSession() {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  return {
    browser,
    context,
    page,
    close: async () => {
      await context.close();
      await browser.close();
    },
  };
}

async function withPage(options, runner) {
  if (options && options.page) {
    return runner(options.page);
  }

  const session = await createSession();
  try {
    return await runner(session.page);
  } finally {
    await session.close();
  }
}

module.exports = {
  RATE_LIMIT_MS,
  RETRIES,
  MAX_DISCOVERY_PAGES,
  normalizeText,
  normalizeMultilineText,
  toSingleLine,
  sanitizeTitle,
  slugify,
  escapeHtml,
  canonicalizeUrl,
  extractSourceIdFromUrl,
  parseDateToIso,
  htmlToStructuredPlainText,
  plainTextToHtml,
  normalizeHtmlFragment,
  extractJobPostingJsonLd,
  extractIdentifierFromJsonLd,
  extractLocationFromJsonLd,
  extractEmploymentTypeFromJsonLd,
  extractMetaDescription,
  selectDescriptionHtml,
  findValueByLabels,
  mapArrangementFromEmploymentType,
  normalizeJobRecord,
  gotoWithRetry,
  createSession,
  withPage,
};
