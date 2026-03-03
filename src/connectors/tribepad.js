const { chromium } = require("playwright");
const cheerio = require("cheerio");

const RATE_LIMIT_MS = 1000;
const RETRIES = 2;
const MAX_PAGES = 10;
const JOB_PATH_REGEX = /\/jobs\/job\/.+\/\d+\/?$/i;
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

function normalizeText(value) {
  return String(value || "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePlainLines(value) {
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
  return normalizePlainLines(value).replace(/\n+/g, " ").replace(/\s+/g, " ").trim();
}

function slugify(value) {
  return normalizeText(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function escapeRegExp(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
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

  return normalizePlainLines(output);
}

function extractListBlock($, listNode) {
  const lines = [];

  $(listNode)
    .children("li")
    .each((_, liNode) => {
      const itemText = toSingleLine(extractInlineText($, liNode));
      if (itemText) {
        lines.push(`- ${itemText}`);
      }
    });

  return lines.join("\n");
}

function pushBlock(blocks, value) {
  const cleaned = normalizePlainLines(value);
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

function isJobUrl(url) {
  try {
    return JOB_PATH_REGEX.test(new URL(url).pathname);
  } catch {
    return false;
  }
}

function canonicalizeUrl(baseUrl, value) {
  if (!value) {
    return "";
  }

  try {
    const url = new URL(value, baseUrl);
    url.hash = "";
    return url.href;
  } catch {
    return "";
  }
}

function extractJobIdFromUrl(jobUrl) {
  try {
    const match = new URL(jobUrl).pathname.match(/\/(\d+)\/?$/);
    return match ? match[1] : "";
  } catch {
    return "";
  }
}

function mapArrangement(contractType) {
  const normalized = normalizeText(contractType).toLowerCase();

  if (normalized.includes("casual")) {
    return "parttime";
  }

  if (normalized.includes("permanent")) {
    return "fulltime";
  }

  return "fulltime";
}

function parseTribepadDate(value) {
  const cleaned = normalizeText(value)
    .replace(/^posted on\s*/i, "")
    .replace(/^closing date\s*/i, "");

  const match = cleaned.match(/(\d{1,2})\s+([A-Za-z]+),?\s+(\d{4})/);
  if (!match) {
    return "";
  }

  const day = Number(match[1]);
  const monthName = match[2].toLowerCase();
  const year = Number(match[3]);
  const months = {
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

  const month = months[monthName];
  if (!month || !day || !year) {
    return "";
  }

  const yyyy = String(year);
  const mm = String(month).padStart(2, "0");
  const dd = String(day).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}T00:00:00Z`;
}

function extractValueByLabel($, label) {
  const labelPattern = new RegExp(`^${escapeRegExp(label)}\\s*:?\\s*(.+)$`, "i");

  let value = "";

  $("dt").each((_, el) => {
    if (value) {
      return;
    }

    const dtText = normalizeText($(el).text()).toLowerCase();
    if (dtText === label.toLowerCase()) {
      value = normalizeText($(el).next("dd").text());
    }
  });

  if (value) {
    return value;
  }

  $("li,p,div,span").each((_, el) => {
    if (value) {
      return;
    }

    const text = normalizeText($(el).text());
    const match = text.match(labelPattern);
    if (match) {
      value = normalizeText(match[1]);
    }
  });

  return value;
}

function extractSectionByHeading($, headingText) {
  const normalizedTarget = normalizeText(headingText).toLowerCase();
  const heading = $("h1,h2,h3,h4,h5,h6")
    .filter((_, el) => normalizeText($(el).text()).toLowerCase() === normalizedTarget)
    .first();

  if (!heading.length) {
    return { html: "", plain: "" };
  }

  const sectionNodes = heading.nextUntil("h1,h2,h3,h4,h5,h6");
  const htmlParts = [];

  sectionNodes.each((_, el) => {
    const html = $.html(el);
    if (html && normalizeText($(el).text())) {
      htmlParts.push(html.trim());
    }
  });

  const html = htmlParts.join("\n").trim();
  const plain = htmlToStructuredPlainText(html);

  return { html, plain };
}

function extractFallbackDescription($) {
  const candidates = [
    "main",
    "article",
    ".job-description",
    ".job-details",
    "#main-content",
  ];

  for (const selector of candidates) {
    const candidate = $(selector).first();
    if (!candidate.length) {
      continue;
    }

    const text = htmlToStructuredPlainText(candidate.html() || "");
    if (text) {
      return text;
    }
  }

  return htmlToStructuredPlainText($("body").html() || "");
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

async function discoverJobUrls(club, options = {}) {
  return withPage(options, async (page) => {
    const queue = [club.source_url];
    const queued = new Set(queue);
    const visited = new Set();
    const jobUrls = new Set();
    let crawledPages = 0;

    while (queue.length && crawledPages < MAX_PAGES) {
      const currentUrl = queue.shift();
      queued.delete(currentUrl);

      if (visited.has(currentUrl)) {
        continue;
      }

      await gotoWithRetry(page, currentUrl);
      visited.add(currentUrl);
      crawledPages += 1;

      const $ = cheerio.load(await page.content());

      $("a[href]").each((_, el) => {
        const href = $(el).attr("href");
        const absoluteUrl = canonicalizeUrl(currentUrl, href);
        if (!absoluteUrl) {
          return;
        }

        if (isJobUrl(absoluteUrl)) {
          jobUrls.add(absoluteUrl);
        }

        const linkText = normalizeText($(el).text()).toLowerCase();
        const isPagination =
          linkText.includes("next results page") || linkText.includes("page number");

        if (
          isPagination &&
          !isJobUrl(absoluteUrl) &&
          !visited.has(absoluteUrl) &&
          !queued.has(absoluteUrl)
        ) {
          queue.push(absoluteUrl);
          queued.add(absoluteUrl);
        }
      });
    }

    return Array.from(jobUrls);
  });
}

async function fetchJob(club, jobUrl, options = {}) {
  return withPage(options, async (page) => {
    await gotoWithRetry(page, jobUrl);

    const $ = cheerio.load(await page.content());

    let sourceId = extractJobIdFromUrl(jobUrl);
    const title = normalizeText($("h1").first().text());
    const location = extractValueByLabel($, "Location");
    const contractType = extractValueByLabel($, "Contract Type");
    const postedOn = extractValueByLabel($, "Posted on");
    const closingDate = extractValueByLabel($, "Closing Date");

    if (!sourceId) {
      const jobReference = extractValueByLabel($, "Job Reference");
      const referenceMatch = jobReference.match(/(\d+)$/);
      sourceId = referenceMatch ? referenceMatch[1] : "";
    }

    if (!sourceId) {
      throw new Error(`No se pudo extraer source_id desde ${jobUrl}`);
    }

    const aboutRole = extractSectionByHeading($, "About the role");
    const plainTextDescription = aboutRole.plain || extractFallbackDescription($);
    const htmlDescription =
      aboutRole.html || `<p>${escapeHtml(plainTextDescription)}</p>`;

    const safeTitle = title || `job-${sourceId}`;
    const id = sourceId;

    return {
      club_id: club.club_id,
      source_id: sourceId,
      id,
      guid: `${id}-${slugify(safeTitle)}`,
      url: jobUrl,
      application_link: `https://careers.liverpoolfc.com/members/?j=${sourceId}&lang=en`,
      title: safeTitle,
      arrangement: mapArrangement(contractType),
      location_type: "onsite",
      location,
      published_at: parseTribepadDate(postedOn),
      expires_at: parseTribepadDate(closingDate),
      highlighted: false,
      sticky: false,
      html_description: htmlDescription,
      plain_text_description: plainTextDescription,
      company_name: club.name,
      company_url: club.company_url,
      company_logo_url: club.company_logo_url || "",
      _meta: {
        contract_type: contractType,
        posted_on_raw: postedOn,
        closing_date_raw: closingDate,
      },
    };
  });
}

module.exports = {
  RATE_LIMIT_MS,
  RETRIES,
  discoverJobUrls,
  fetchJob,
  createSession,
};
