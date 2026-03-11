const cheerio = require("cheerio");
const {
  MAX_DISCOVERY_PAGES,
  withPage,
  gotoWithRetry,
  canonicalizeUrl,
  normalizeText,
  slugify,
  extractJobPostingJsonLd,
  extractIdentifierFromJsonLd,
  extractLocationFromJsonLd,
  extractEmploymentTypeFromJsonLd,
  parseDateToIso,
  htmlToStructuredPlainText,
  selectDescriptionHtml,
  findValueByLabels,
  extractMetaDescription,
  escapeHtml,
  extractSourceIdFromUrl,
  mapArrangementFromEmploymentType,
  normalizeHtmlFragment,
  createSession,
} = require("./utils");

const JOB_KEYWORD_REGEX = /(\/|^)(jobs?|careers?|position|vacanc(?:y|ies))(\/|$)/i;
const LISTING_PATH_REGEX = /^\/(jobs?|careers?|positions?|vacanc(?:y|ies))\/?$/i;

const MANUTD_CLUB_ID = "manutd";
const MANUTD_LISTING_URL_REGEX = /candidatemanager\.net\/cm\/p\/pjobs\.aspx/i;
const MANUTD_DETAIL_URL_REGEX = /candidatemanager\.net\/cm\/p\/pjobdetails\.aspx/i;
const MANUTD_TABLE_HEADING_REGEX = /current vacancies/i;
const MANUTD_CACHE = new Map();
const LEEDS_CLUB_ID = "leeds";
const WOLVES_CLUB_ID = "wolves";
const LEEDS_APPLY_URL_REGEX = /^https:\/\/forms\.office\.com\//i;
const LEEDS_CACHE = new Map();
const LEEDS_URL_CACHE = new Map();
const WOLVES_CACHE = new Map();
const WOLVES_URL_CACHE = new Map();
const SHARED_CAREERS_CLUB_IDS = new Set(["newcastle", "astonvilla"]);
const SHARED_CAREERS_CACHE = new Map();
const SHARED_CAREERS_LISTING_PATH_REGEX = /^\/jobs\/?$/i;
const SHARED_CAREERS_DETAIL_PATH_REGEX = /^\/job\/([^/?#]+)\/?$/i;
const SHARED_CAREERS_APPLY_PATH_REGEX = /^\/job\/([^/?#]+)\/apply\/?$/i;

function logManUtdHttp(message) {
  console.log(`[http] manutd ${message}`);
}

function hasJobKeyword(pathname) {
  return JOB_KEYWORD_REGEX.test(String(pathname || ""));
}

function isListingPath(pathname) {
  return LISTING_PATH_REGEX.test(String(pathname || ""));
}

function isLikelyJobDetailUrl(url) {
  try {
    const parsed = new URL(url);
    if (!hasJobKeyword(parsed.pathname)) {
      return false;
    }

    if (isListingPath(parsed.pathname)) {
      return false;
    }

    return true;
  } catch {
    return false;
  }
}

function isPaginationLink(url, linkText) {
  const text = normalizeText(linkText).toLowerCase();

  if (
    text.includes("next") ||
    text.includes("older") ||
    text.includes("more") ||
    text.includes("page")
  ) {
    return true;
  }

  try {
    const parsed = new URL(url);
    return parsed.searchParams.has("page");
  } catch {
    return false;
  }
}

function isManUtdClub(club) {
  return normalizeText(club && club.club_id).toLowerCase() === MANUTD_CLUB_ID;
}

function isLeedsClub(club) {
  return normalizeText(club && club.club_id).toLowerCase() === LEEDS_CLUB_ID;
}

function isWolvesClub(club) {
  return normalizeText(club && club.club_id).toLowerCase() === WOLVES_CLUB_ID;
}

function isSharedCareersClub(club) {
  return SHARED_CAREERS_CLUB_IDS.has(
    normalizeText(club && club.club_id).toLowerCase()
  );
}

function buildManUtdCacheKey(club, sourceId) {
  return `${normalizeText(club && club.club_id)}::${normalizeText(sourceId)}`;
}

function buildSharedCareersCacheKey(club, sourceId) {
  return `${normalizeText(club && club.club_id)}::${normalizeText(sourceId)}`;
}

function buildLeedsCacheKey(club, sourceId) {
  return `${normalizeText(club && club.club_id)}::${normalizeText(sourceId)}`;
}

function buildWolvesCacheKey(club, sourceId) {
  return `${normalizeText(club && club.club_id)}::${normalizeText(sourceId)}`;
}

function extractSharedSourceIdFromSlug(slug) {
  const match = normalizeText(slug).match(/-(\d+)$/);
  return match ? match[1] : "";
}

function parseSharedCareersLink(url) {
  try {
    const parsed = new URL(url);
    const pathname = decodeURIComponent(parsed.pathname || "");
    const applyMatch = pathname.match(SHARED_CAREERS_APPLY_PATH_REGEX);

    if (applyMatch) {
      const slug = normalizeText(applyMatch[1]);
      const sourceId = extractSharedSourceIdFromSlug(slug);
      return sourceId
        ? {
            type: "apply",
            slug,
            source_id: sourceId,
          }
        : null;
    }

    const detailMatch = pathname.match(SHARED_CAREERS_DETAIL_PATH_REGEX);
    if (detailMatch) {
      const slug = normalizeText(detailMatch[1]);
      const sourceId = extractSharedSourceIdFromSlug(slug);
      return sourceId
        ? {
            type: "detail",
            slug,
            source_id: sourceId,
          }
        : null;
    }
  } catch {
    // Ignore URL parsing failures.
  }

  return null;
}

function buildSharedCareersDefaultApplyUrl(jobUrl, slug) {
  if (slug) {
    return canonicalizeUrl(jobUrl, `/job/${slug}/apply`);
  }

  const normalized = normalizeText(jobUrl);
  if (!normalized) {
    return "";
  }

  return `${normalized.replace(/\/+$/, "")}/apply`;
}

function extractTitleFromSharedSlug(slug) {
  const cleaned = normalizeText(slug).replace(/-\d+$/, "");
  if (!cleaned) {
    return "";
  }

  return cleaned.replace(/-/g, " ").trim();
}

function isSharedActionText(value) {
  const text = normalizeText(value).toLowerCase();

  return (
    !text ||
    text === "read more" ||
    text === "apply" ||
    text === "apply now" ||
    text === "view details" ||
    text === "details"
  );
}

function findSharedCareersCard($, anchorNode) {
  const selectors = [
    "article",
    "li",
    '[class*="job"]',
    '[class*="vacan"]',
    '[class*="card"]',
    '[data-testid*="job"]',
  ];

  for (const selector of selectors) {
    const candidate = $(anchorNode).closest(selector).first();
    if (candidate.length) {
      return candidate;
    }
  }

  return $(anchorNode).parent();
}

function findValueInSharedCardByLabels($, cardNode, labels) {
  if (!cardNode || !cardNode.length) {
    return "";
  }

  const snippet = String(cardNode.html() || "").trim();
  if (!snippet) {
    return "";
  }

  const local$ = cheerio.load(`<section id="__card__">${snippet}</section>`);
  return findValueByLabels(local$, labels);
}

function extractSharedListingMeta($, cardNode, anchorNode, slug) {
  const heading = cardNode
    .find("h1,h2,h3,h4,h5,h6,[class*='title'],[data-testid*='title']")
    .first();
  const headingText = normalizeText(heading.text());
  const anchorText = normalizeText($(anchorNode).text());
  const fallbackFromSlug = extractTitleFromSharedSlug(slug);

  const title = !isSharedActionText(headingText)
    ? headingText
    : !isSharedActionText(anchorText)
      ? anchorText
      : fallbackFromSlug;

  const location =
    findValueInSharedCardByLabels($, cardNode, [
      "location",
      "job location",
      "base location",
    ]) || "";

  const arrangement =
    findValueInSharedCardByLabels($, cardNode, [
      "job type",
      "employment type",
      "contract type",
      "type",
    ]) || "";

  const publishedAt =
    findValueInSharedCardByLabels($, cardNode, [
      "posted on",
      "posted",
      "date posted",
      "published",
    ]) || "";

  const locationTypeRaw =
    findValueInSharedCardByLabels($, cardNode, [
      "location type",
      "workplace",
      "working model",
    ]) || "";

  const locationTypeText = normalizeText(locationTypeRaw).toLowerCase();
  const locationType = locationTypeText.includes("hybrid")
    ? "hybrid"
    : locationTypeText.includes("remote")
      ? "remote"
      : "onsite";

  return {
    title,
    location,
    arrangement,
    published_at: publishedAt,
    location_type: locationType,
  };
}

function resolveSharedLocationType(value, detailText) {
  const normalized = normalizeText(value).toLowerCase();

  if (normalized.includes("hybrid")) {
    return "hybrid";
  }

  if (normalized.includes("remote")) {
    return "remote";
  }

  const detailNormalized = normalizeText(detailText).toLowerCase();
  if (detailNormalized.includes("hybrid")) {
    return "hybrid";
  }

  if (detailNormalized.includes("remote")) {
    return "remote";
  }

  return "onsite";
}

function extractManUtdSourceId(url) {
  try {
    const parsed = new URL(url);
    const jid = normalizeText(
      parsed.searchParams.get("jid") ||
        parsed.searchParams.get("JID") ||
        parsed.searchParams.get("jobid") ||
        ""
    );

    if (jid) {
      return jid;
    }
  } catch {
    // Ignore URL parsing errors.
  }

  const match = String(url || "").match(/[?&]jid=([^&#]+)/i);
  if (!match) {
    return "";
  }

  try {
    return normalizeText(decodeURIComponent(match[1]));
  } catch {
    return normalizeText(match[1]);
  }
}

function mapManUtdArrangement(jobType) {
  const normalized = normalizeText(jobType).toLowerCase();

  if (normalized.includes("part")) {
    return "parttime";
  }

  if (normalized.includes("casual") || normalized.includes("temporary")) {
    return "casual";
  }

  return "fulltime";
}

function parseUsDateToIso(value) {
  const match = normalizeText(value).match(/^(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})$/);

  if (!match) {
    return "";
  }

  const month = Number(match[1]);
  const day = Number(match[2]);
  let year = Number(match[3]);

  if (!month || !day || !year || month > 12 || day > 31) {
    return "";
  }

  if (year < 100) {
    year += 2000;
  }

  const yyyy = String(year).padStart(4, "0");
  const mm = String(month).padStart(2, "0");
  const dd = String(day).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}T00:00:00Z`;
}

async function fetchHtmlWithFallback(page, url) {
  const targetUrl = normalizeText(url);
  if (!targetUrl) {
    return "";
  }

  try {
    logManUtdHttp(`GET ${targetUrl}`);
    const response = await page.request.get(targetUrl, {
      timeout: 45000,
    });
    logManUtdHttp(`${response.status()} ${targetUrl}`);

    const text = await response.text();
    await page.waitForTimeout(1000);

    if (text && text.trim()) {
      return text;
    }
  } catch (error) {
    logManUtdHttp(`ERROR ${targetUrl} -> ${error.message}`);
    // Fallback to browser navigation.
  }

  logManUtdHttp(`GET ${targetUrl} (browser)`);
  const response = await page.goto(targetUrl, {
    waitUntil: "domcontentloaded",
    timeout: 45000,
  });
  logManUtdHttp(
    `${response ? response.status() : "NO_RESPONSE"} ${targetUrl} (browser)`
  );
  await page.waitForTimeout(1000);
  return page.content();
}

function extractFallbackLocation($) {
  const byLabel = findValueByLabels($, [
    "location",
    "job location",
    "base location",
    "office location",
  ]);
  if (byLabel) {
    return byLabel;
  }

  const selectors = [
    '[itemprop="jobLocation"]',
    '[class*="location"]',
    '[data-testid*="location"]',
  ];

  for (const selector of selectors) {
    const node = $(selector).first();
    const text = normalizeText(node.text());
    if (text) {
      return text;
    }
  }

  return "";
}

function extractFallbackEmploymentType($) {
  return findValueByLabels($, [
    "employment type",
    "contract type",
    "job type",
    "type",
  ]);
}

function extractFallbackDescriptionHtml($) {
  const selected = selectDescriptionHtml($);
  if (selected) {
    return selected;
  }

  const metaDescription = extractMetaDescription($);
  if (metaDescription) {
    return `<p>${escapeHtml(metaDescription)}</p>`;
  }

  return "";
}

function findManUtdVacanciesTable($) {
  let table = $("table")
    .filter((_, el) => $(el).find('a[href*="pJobDetails.aspx"]').length > 0)
    .first();

  if (!table.length) {
    return table;
  }

  const heading = $("h1,h2,h3,h4,h5,h6,strong,caption")
    .filter((_, el) => MANUTD_TABLE_HEADING_REGEX.test(normalizeText($(el).text())))
    .first();

  if (!heading.length) {
    return table;
  }

  const tableNearHeading = heading
    .nextAll("table")
    .filter((_, el) => $(el).find('a[href*="pJobDetails.aspx"]').length > 0)
    .first();

  if (tableNearHeading.length) {
    table = tableNearHeading;
  }

  return table;
}

function resolveHeaderIndices($, table) {
  const headerCells = table.find("thead tr").first().children("th,td");
  const fallbackHeaderCells = table.find("tr").first().children("th,td");
  const cells = headerCells.length ? headerCells : fallbackHeaderCells;

  const headers = cells
    .toArray()
    .map((cell) => normalizeText($(cell).text()).toLowerCase());

  const findIndex = (predicates) =>
    headers.findIndex((header) => predicates.some((value) => header.includes(value)));

  return {
    location: findIndex(["location"]),
    jobType: findIndex(["job type", "type"]),
    expiryDate: findIndex(["expiry", "closing", "deadline"]),
  };
}

function getCellText($, cells, index) {
  if (index < 0 || index >= cells.length) {
    return "";
  }

  return normalizeText($(cells[index]).text());
}

function parseManUtdRows(baseUrl, html, club) {
  const $ = cheerio.load(String(html || ""));
  const table = findManUtdVacanciesTable($);

  if (!table.length) {
    return [];
  }

  const indices = resolveHeaderIndices($, table);
  const jobs = [];

  table.find("tr").each((_, row) => {
    const link = $(row).find('a[href*="pJobDetails.aspx"]').first();
    if (!link.length) {
      return;
    }

    const url = canonicalizeUrl(baseUrl, link.attr("href"));
    if (!url) {
      return;
    }

    const sourceId = extractManUtdSourceId(url);
    const cells = $(row).children("th,td").toArray();
    const title = normalizeText(link.text()) || getCellText($, cells, 0);
    const location = getCellText($, cells, indices.location);
    const jobType = getCellText($, cells, indices.jobType);
    const rawExpiryDate = getCellText($, cells, indices.expiryDate);

    const rowJob = {
      source_id: sourceId,
      id: sourceId,
      url,
      application_link: url,
      title,
      arrangement: mapManUtdArrangement(jobType),
      location_type: "onsite",
      location,
      published_at: "",
      expires_at: parseUsDateToIso(rawExpiryDate) || parseDateToIso(rawExpiryDate),
      html_description: "",
      plain_text_description: "",
      company_name: club.name,
      company_url: "https://www.manutd.com/",
      company_logo_url: club.company_logo_url || "",
    };

    if (sourceId) {
      MANUTD_CACHE.set(buildManUtdCacheKey(club, sourceId), rowJob);
    }

    jobs.push(rowJob);
  });

  return jobs;
}

function collectManUtdUrls(baseUrl, html) {
  const listingUrls = new Set();
  const detailUrls = new Set();

  const $ = cheerio.load(String(html || ""));

  $("iframe[src],a[href]").each((_, el) => {
    const href =
      $(el).attr("href") ||
      $(el).attr("src") ||
      "";

    const absoluteUrl = canonicalizeUrl(baseUrl, href);

    if (!absoluteUrl) {
      return;
    }

    if (MANUTD_LISTING_URL_REGEX.test(absoluteUrl)) {
      listingUrls.add(absoluteUrl);
    }

    if (MANUTD_DETAIL_URL_REGEX.test(absoluteUrl)) {
      detailUrls.add(absoluteUrl);
    }
  });

  return { listingUrls, detailUrls };
}

async function discoverManUtdJobUrls(club, options = {}) {
  return withPage(options, async (page) => {
    const discoveredJobUrls = new Set();
    const listingUrls = new Set();

    const seedHtml = await fetchHtmlWithFallback(page, club.source_url);
    const seedDiscovery = collectManUtdUrls(club.source_url, seedHtml);

    for (const url of seedDiscovery.detailUrls) {
      discoveredJobUrls.add(url);
    }

    for (const url of seedDiscovery.listingUrls) {
      listingUrls.add(url);
    }

    if (!listingUrls.size) {
      listingUrls.add(club.source_url);
    }

    for (const listingUrl of listingUrls) {
      const html =
        listingUrl === club.source_url
          ? seedHtml
          : await fetchHtmlWithFallback(page, listingUrl);

      const rows = parseManUtdRows(listingUrl, html, club);
      for (const row of rows) {
        if (row.url) {
          discoveredJobUrls.add(row.url);
        }
      }
    }

    return Array.from(discoveredJobUrls).filter(Boolean);
  });
}

async function fetchManUtdJob(club, jobUrl, options = {}) {
  return withPage(options, async (page) => {
    const sourceId = extractManUtdSourceId(jobUrl);
    let cachedJob = sourceId
      ? MANUTD_CACHE.get(buildManUtdCacheKey(club, sourceId))
      : null;

    if (!cachedJob) {
      const html = await fetchHtmlWithFallback(page, jobUrl);
      const discovered = parseManUtdRows(jobUrl, html, club);
      cachedJob = discovered.find((job) => job.url === jobUrl) || null;
    }

    let title = normalizeText(cachedJob && cachedJob.title);

    if (!title) {
      const html = await fetchHtmlWithFallback(page, jobUrl);
      const $ = cheerio.load(html);
      title =
        normalizeText($("h1").first().text()) ||
        normalizeText($("title").first().text());
    }

    return {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      url: jobUrl,
      application_link: jobUrl,
      title,
      arrangement: cachedJob && cachedJob.arrangement ? cachedJob.arrangement : "",
      location_type: "onsite",
      location: normalizeText(cachedJob && cachedJob.location),
      published_at: "",
      expires_at: normalizeText(cachedJob && cachedJob.expires_at),
      highlighted: false,
      sticky: false,
      html_description: "",
      plain_text_description: "",
      company_name: club.name,
      company_url: "https://www.manutd.com/",
      company_logo_url: club.company_logo_url || "",
    };
  });
}

function isSharedListingPath(pathname) {
  return SHARED_CAREERS_LISTING_PATH_REGEX.test(String(pathname || ""));
}

function isSharedPaginationLink(url, linkText) {
  const text = normalizeText(linkText).toLowerCase();
  if (text.includes("next") || text.includes("more") || text.includes("page")) {
    return true;
  }

  try {
    const parsed = new URL(url);
    if (!isSharedListingPath(parsed.pathname)) {
      return false;
    }

    return parsed.searchParams.has("page");
  } catch {
    return false;
  }
}

async function discoverSharedCareersJobUrls(club, options = {}) {
  return withPage(options, async (page) => {
    const queue = [club.source_url];
    const queued = new Set(queue);
    const visited = new Set();
    const listings = new Map();
    let crawledPages = 0;
    let seedHost = "";

    try {
      seedHost = new URL(club.source_url).hostname;
    } catch {
      seedHost = "";
    }

    while (queue.length && crawledPages < MAX_DISCOVERY_PAGES) {
      const currentUrl = queue.shift();
      queued.delete(currentUrl);

      if (visited.has(currentUrl)) {
        continue;
      }

      await gotoWithRetry(page, currentUrl);
      visited.add(currentUrl);
      crawledPages += 1;

      const $ = cheerio.load(await page.content());

      $("a[href]").each((_, anchorNode) => {
        const href = $(anchorNode).attr("href");
        const absoluteUrl = canonicalizeUrl(currentUrl, href);
        if (!absoluteUrl) {
          return;
        }

        let parsed;
        try {
          parsed = new URL(absoluteUrl);
        } catch {
          return;
        }

        if (seedHost && parsed.hostname !== seedHost) {
          return;
        }

        const sharedLink = parseSharedCareersLink(absoluteUrl);
        if (sharedLink && sharedLink.source_id) {
          const key = sharedLink.source_id;
          const existing = listings.get(key) || {
            source_id: key,
            slug: sharedLink.slug,
            url: "",
            application_link: "",
            title: "",
            location: "",
            arrangement: "",
            published_at: "",
            location_type: "onsite",
          };

          const card = findSharedCareersCard($, anchorNode);
          const metadata = extractSharedListingMeta($, card, anchorNode, sharedLink.slug);
          if (metadata.title && isSharedActionText(existing.title)) {
            existing.title = metadata.title;
          } else if (!existing.title && metadata.title) {
            existing.title = metadata.title;
          }
          if (!existing.location && metadata.location) {
            existing.location = metadata.location;
          }
          if (!existing.arrangement && metadata.arrangement) {
            existing.arrangement = metadata.arrangement;
          }
          if (!existing.published_at && metadata.published_at) {
            existing.published_at = metadata.published_at;
          }
          if (metadata.location_type && existing.location_type === "onsite") {
            existing.location_type = metadata.location_type;
          }

          if (sharedLink.type === "detail") {
            existing.url = absoluteUrl;
          } else if (sharedLink.type === "apply") {
            existing.application_link = absoluteUrl;
            if (!existing.url) {
              existing.url = canonicalizeUrl(absoluteUrl, `/job/${sharedLink.slug}`);
            }
          }

          listings.set(key, existing);
          return;
        }

        if (
          isSharedListingPath(parsed.pathname) ||
          isSharedPaginationLink(absoluteUrl, $(anchorNode).text())
        ) {
          if (!visited.has(absoluteUrl) && !queued.has(absoluteUrl)) {
            queue.push(absoluteUrl);
            queued.add(absoluteUrl);
          }
        }
      });
    }

    const jobUrls = [];
    for (const entry of listings.values()) {
      if (!entry.source_id || !entry.url || !parseSharedCareersLink(entry.url)) {
        continue;
      }

      if (!entry.application_link) {
        entry.application_link = buildSharedCareersDefaultApplyUrl(
          entry.url,
          entry.slug
        );
      }

      SHARED_CAREERS_CACHE.set(
        buildSharedCareersCacheKey(club, entry.source_id),
        entry
      );
      jobUrls.push(entry.url);
    }

    return Array.from(new Set(jobUrls));
  });
}

function extractSharedSourceIdFromUrl(jobUrl) {
  const sharedLink = parseSharedCareersLink(jobUrl);
  if (sharedLink && sharedLink.source_id) {
    return sharedLink.source_id;
  }

  return extractSourceIdFromUrl(jobUrl);
}

async function fetchSharedCareersJob(club, jobUrl, options = {}) {
  return withPage(options, async (page) => {
    const sharedLink = parseSharedCareersLink(jobUrl);
    const sourceId = extractSharedSourceIdFromUrl(jobUrl);
    const listingCache = sourceId
      ? SHARED_CAREERS_CACHE.get(buildSharedCareersCacheKey(club, sourceId))
      : null;

    await gotoWithRetry(page, jobUrl);
    const $ = cheerio.load(await page.content());

    const detailText = normalizeText($("body").text());
    let title =
      normalizeText(listingCache && listingCache.title) ||
      normalizeText($("h1").first().text()) ||
      extractTitleFromSharedSlug(sharedLink && sharedLink.slug);

    if (isSharedActionText(title)) {
      title = normalizeText($("h1").first().text()) || title;
    }

    const location =
      normalizeText(listingCache && listingCache.location) ||
      extractFallbackLocation($);
    const employmentType =
      normalizeText(listingCache && listingCache.arrangement) ||
      extractFallbackEmploymentType($);
    const htmlDescription = extractFallbackDescriptionHtml($);
    const plainTextDescription = htmlToStructuredPlainText(htmlDescription);
    const publishedAt = parseDateToIso(
      normalizeText(listingCache && listingCache.published_at) ||
        findValueByLabels($, ["posted on", "published", "date posted", "posted"])
    );
    const expiresAt = parseDateToIso(
      findValueByLabels($, [
        "closing date",
        "application deadline",
        "expires",
        "valid through",
      ])
    );

    let applicationLink = normalizeText(listingCache && listingCache.application_link);
    if (!applicationLink) {
      $("a[href]").each((_, node) => {
        if (applicationLink) {
          return;
        }

        const href = $(node).attr("href");
        const absoluteUrl = canonicalizeUrl(jobUrl, href);
        if (!absoluteUrl) {
          return;
        }

        const parsed = parseSharedCareersLink(absoluteUrl);
        if (parsed && parsed.type === "apply") {
          applicationLink = absoluteUrl;
          return;
        }

        const text = normalizeText($(node).text()).toLowerCase();
        if (text.includes("apply")) {
          applicationLink = absoluteUrl;
        }
      });
    }

    if (!applicationLink) {
      applicationLink = buildSharedCareersDefaultApplyUrl(
        jobUrl,
        sharedLink && sharedLink.slug
      );
    }

    return {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      url: jobUrl,
      application_link: applicationLink,
      title,
      arrangement: mapArrangementFromEmploymentType(employmentType),
      employment_type: employmentType,
      location_type: resolveSharedLocationType(
        normalizeText(listingCache && listingCache.location_type),
        detailText
      ),
      location,
      published_at: publishedAt,
      expires_at: expiresAt,
      highlighted: false,
      sticky: false,
      html_description: htmlDescription,
      plain_text_description: plainTextDescription,
      company_name: club.name,
      company_url: club.company_url || club.source_url || "",
      company_logo_url: club.company_logo_url || "",
    };
  });
}

function extractLeedsSourceIdFromApplyUrl(url) {
  try {
    const parsed = new URL(url);
    const segments = parsed.pathname.split("/").filter(Boolean);

    if (segments.length >= 2 && normalizeText(segments[0]).toLowerCase() === "e") {
      return normalizeText(segments[1]).toLowerCase();
    }

    const queryId = normalizeText(
      parsed.searchParams.get("id") || parsed.searchParams.get("ID") || ""
    );
    if (queryId) {
      return slugify(queryId);
    }
  } catch {
    // Ignore URL parsing failures.
  }

  return normalizeText(extractSourceIdFromUrl(url)).toLowerCase();
}

function cleanLeedsTitle(value) {
  const normalized = normalizeText(value)
    .replace(/\s*[–-]\s*fill\s*out\s*form\s*$/i, "")
    .replace(/^apply(?:\s+here)?\s*:\s*/i, "")
    .trim();

  if (!normalized || /^https?:\/\//i.test(normalized)) {
    return "";
  }

  return normalized;
}

function extractLeedsTitleFromAccordion($, articleNode) {
  const heading = $(articleNode)
    .closest(".v3-accordion__expandableSection")
    .children("h1,h2,h3,h4,h5,h6")
    .first();

  return cleanLeedsTitle(heading.text());
}

function extractLeedsEntry($, club, anchorNode) {
  const applyUrl = canonicalizeUrl(club.source_url, $(anchorNode).attr("href"));
  if (!applyUrl || !LEEDS_APPLY_URL_REGEX.test(applyUrl)) {
    return null;
  }

  const sourceId = extractLeedsSourceIdFromApplyUrl(applyUrl);
  if (!sourceId) {
    return null;
  }

  const article = $(anchorNode).closest("article").first();
  if (!article.length) {
    return null;
  }

  const articleHtml = String(article.html() || "").trim();
  if (!articleHtml) {
    return null;
  }

  const local$ = cheerio.load(`<section id="__leeds_card__">${articleHtml}</section>`);
  const anchorTitle = cleanLeedsTitle($(anchorNode).text());
  const labelTitle = cleanLeedsTitle(
    findValueByLabels(local$, ["job title", "title", "position", "role"])
  );
  const accordionTitle = extractLeedsTitleFromAccordion($, article);
  const title = anchorTitle || labelTitle || accordionTitle || `Leeds role ${sourceId}`;
  const location = findValueByLabels(local$, ["location", "base location"]);
  const department = findValueByLabels(local$, ["department", "team"]);
  const employmentType = findValueByLabels(local$, [
    "hours of work",
    "hours",
    "employment type",
    "contract type",
    "job type",
  ]);
  const expiresAt = parseDateToIso(
    findValueByLabels(local$, [
      "closing",
      "closing date",
      "application deadline",
      "deadline",
    ])
  );

  const htmlDescription = articleHtml;
  const plainTextDescription = htmlToStructuredPlainText(htmlDescription);

  return {
    source_id: sourceId,
    id: sourceId,
    url: applyUrl,
    application_link: applyUrl,
    title,
    department,
    arrangement: mapArrangementFromEmploymentType(employmentType),
    employment_type: employmentType,
    location_type: "onsite",
    location,
    published_at: "",
    expires_at: expiresAt,
    highlighted: false,
    sticky: false,
    html_description: htmlDescription,
    plain_text_description: plainTextDescription,
    company_name: club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url: club.company_logo_url || "",
  };
}

function mergeLeedsEntries(existing, incoming) {
  if (!existing) {
    return incoming;
  }

  const merged = { ...existing };
  const fields = [
    "title",
    "department",
    "arrangement",
    "employment_type",
    "location",
    "published_at",
    "expires_at",
    "html_description",
    "plain_text_description",
  ];

  for (const field of fields) {
    if (!normalizeText(merged[field]) && normalizeText(incoming[field])) {
      merged[field] = incoming[field];
    }
  }

  return merged;
}

function parseLeedsEntries(club, html) {
  const $ = cheerio.load(String(html || ""));
  const bySourceId = new Map();

  $("a[href]").each((_, anchorNode) => {
    const entry = extractLeedsEntry($, club, anchorNode);
    if (!entry || !entry.source_id) {
      return;
    }

    const existing = bySourceId.get(entry.source_id);
    bySourceId.set(entry.source_id, mergeLeedsEntries(existing, entry));
  });

  return Array.from(bySourceId.values());
}

async function discoverLeedsJobUrls(club, options = {}) {
  return withPage(options, async (page) => {
    await gotoWithRetry(page, club.source_url);
    const entries = parseLeedsEntries(club, await page.content());
    const urls = [];

    for (const entry of entries) {
      if (!entry.source_id || !entry.url) {
        continue;
      }

      LEEDS_CACHE.set(buildLeedsCacheKey(club, entry.source_id), entry);
      LEEDS_URL_CACHE.set(
        `${normalizeText(club && club.club_id)}::${normalizeText(entry.url)}`,
        entry.source_id
      );
      urls.push(entry.url);
    }

    return Array.from(new Set(urls));
  });
}

function findLeedsSourceIdByUrl(club, jobUrl) {
  const direct = extractLeedsSourceIdFromApplyUrl(jobUrl);
  if (direct) {
    return direct;
  }

  return normalizeText(
    LEEDS_URL_CACHE.get(
      `${normalizeText(club && club.club_id)}::${normalizeText(jobUrl)}`
    ) || ""
  );
}

async function fetchLeedsJob(club, jobUrl, options = {}) {
  const sourceId = findLeedsSourceIdByUrl(club, jobUrl);
  let cachedJob = sourceId
    ? LEEDS_CACHE.get(buildLeedsCacheKey(club, sourceId))
    : null;

  if (!cachedJob) {
    await discoverLeedsJobUrls(club, options);
    cachedJob = sourceId
      ? LEEDS_CACHE.get(buildLeedsCacheKey(club, sourceId))
      : null;
  }

  if (!cachedJob) {
    return {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      url: jobUrl,
      application_link: jobUrl,
      title: "",
      arrangement: "",
      location_type: "onsite",
      location: "",
      published_at: "",
      expires_at: "",
      highlighted: false,
      sticky: false,
      html_description: "",
      plain_text_description: "",
      company_name: club.name,
      company_url: club.company_url || club.source_url || "",
      company_logo_url: club.company_logo_url || "",
    };
  }

  return {
    club_id: club.club_id,
    ...cachedJob,
  };
}

function extractEmailAddress(text) {
  const match = normalizeText(text).match(
    /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i
  );
  return match ? String(match[0]).toLowerCase() : "";
}

function buildWolvesFallbackUrl(club, sourceId) {
  const base = normalizeText(club && club.source_url);
  if (!base) {
    return "";
  }

  const separator = base.includes("?") ? "&" : "?";
  return `${base}${separator}job=${encodeURIComponent(sourceId)}`;
}

function extractWolvesClosingDate(text) {
  const match = String(text || "").match(
    /closing\s*date\s*:\s*([^\n\r]+)/i
  );
  if (!match) {
    return "";
  }

  return parseDateToIso(match[1]);
}

function extractWolvesEntry($, club, cardNode) {
  const title = normalizeText($(cardNode).find("h1,h2,h3,h4,h5,h6").first().text());
  const sourceId = slugify(title);
  if (!sourceId) {
    return null;
  }

  const cardHtml = String($(cardNode).html() || "").trim();
  if (!cardHtml) {
    return null;
  }

  const local$ = cheerio.load(`<section id="__wolves_card__">${cardHtml}</section>`);
  const rawText = normalizeText($(cardNode).text());
  const links = [];

  $(cardNode)
    .find("a[href]")
    .each((_, node) => {
      const absolute = canonicalizeUrl(club.source_url, $(node).attr("href"));
      if (!absolute) {
        return;
      }

      links.push({
        href: absolute,
        text: normalizeText($(node).text()),
      });
    });

  const pdfLink = normalizeText(
    (links.find((item) => /\.pdf(?:$|\?)/i.test(item.href)) || {}).href
  );
  const explicitApplyLink = normalizeText(
    (
      links.find(
        (item) =>
          item.text.toLowerCase().includes("apply") &&
          !/\.pdf(?:$|\?)/i.test(item.href)
      ) || {}
    ).href
  );
  const mailtoLink = normalizeText(
    (links.find((item) => item.href.toLowerCase().startsWith("mailto:")) || {}).href
  );
  const emailAddress = extractEmailAddress(rawText);
  const inferredMailto = emailAddress ? `mailto:${emailAddress}` : "";
  const fallbackUrl = buildWolvesFallbackUrl(club, sourceId);
  const url = pdfLink || explicitApplyLink || fallbackUrl;
  const applicationLink =
    explicitApplyLink || mailtoLink || inferredMailto || pdfLink || fallbackUrl;

  const employmentType =
    findValueByLabels(local$, [
      "employment type",
      "job type",
      "contract type",
      "hours",
      "hours of work",
      "position type",
    ]) || title;
  const location = findValueByLabels(local$, [
    "location",
    "base location",
    "site",
  ]);

  return {
    source_id: sourceId,
    id: sourceId,
    url,
    application_link: applicationLink,
    title,
    arrangement: mapArrangementFromEmploymentType(employmentType),
    employment_type: employmentType,
    location_type: "onsite",
    location,
    published_at: "",
    expires_at: extractWolvesClosingDate(rawText),
    highlighted: false,
    sticky: false,
    html_description: cardHtml,
    plain_text_description: htmlToStructuredPlainText(cardHtml),
    company_name: club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url: club.company_logo_url || "",
  };
}

function parseWolvesEntries(club, html) {
  const $ = cheerio.load(String(html || ""));
  const entries = [];

  $(".module.vacancies .vacancies__cards article").each((_, cardNode) => {
    const entry = extractWolvesEntry($, club, cardNode);
    if (entry) {
      entries.push(entry);
    }
  });

  return entries;
}

async function discoverWolvesJobUrls(club, options = {}) {
  return withPage(options, async (page) => {
    await gotoWithRetry(page, club.source_url);
    const entries = parseWolvesEntries(club, await page.content());
    const urls = [];

    for (const entry of entries) {
      if (!entry.source_id || !entry.url) {
        continue;
      }

      WOLVES_CACHE.set(buildWolvesCacheKey(club, entry.source_id), entry);
      WOLVES_URL_CACHE.set(
        `${normalizeText(club && club.club_id)}::${normalizeText(entry.url)}`,
        entry.source_id
      );
      urls.push(entry.url);
    }

    return Array.from(new Set(urls));
  });
}

function findWolvesSourceIdByUrl(club, jobUrl) {
  const fromCache = normalizeText(
    WOLVES_URL_CACHE.get(
      `${normalizeText(club && club.club_id)}::${normalizeText(jobUrl)}`
    ) || ""
  );
  if (fromCache) {
    return fromCache;
  }

  try {
    const parsed = new URL(jobUrl);
    const byQuery = normalizeText(parsed.searchParams.get("job"));
    if (byQuery) {
      return byQuery;
    }
  } catch {
    // Ignore URL parsing errors.
  }

  return "";
}

async function fetchWolvesJob(club, jobUrl, options = {}) {
  const sourceId = findWolvesSourceIdByUrl(club, jobUrl);
  let cachedJob = sourceId
    ? WOLVES_CACHE.get(buildWolvesCacheKey(club, sourceId))
    : null;

  if (!cachedJob) {
    await discoverWolvesJobUrls(club, options);
    cachedJob = sourceId
      ? WOLVES_CACHE.get(buildWolvesCacheKey(club, sourceId))
      : null;
  }

  if (!cachedJob) {
    return {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      url: jobUrl,
      application_link: jobUrl,
      title: "",
      arrangement: "",
      location_type: "onsite",
      location: "",
      published_at: "",
      expires_at: "",
      highlighted: false,
      sticky: false,
      html_description: "",
      plain_text_description: "",
      company_name: club.name,
      company_url: club.company_url || club.source_url || "",
      company_logo_url: club.company_logo_url || "",
    };
  }

  return {
    club_id: club.club_id,
    ...cachedJob,
  };
}

async function discoverJobUrls(club, options = {}) {
  if (isManUtdClub(club)) {
    return discoverManUtdJobUrls(club, options);
  }

  if (isLeedsClub(club)) {
    return discoverLeedsJobUrls(club, options);
  }

  if (isWolvesClub(club)) {
    return discoverWolvesJobUrls(club, options);
  }

  if (isSharedCareersClub(club)) {
    return discoverSharedCareersJobUrls(club, options);
  }

  return withPage(options, async (page) => {
    const seedUrl = club.source_url;
    const queue = [seedUrl];
    const queued = new Set(queue);
    const visited = new Set();
    const jobUrls = new Set();
    let crawledPages = 0;
    let seedHost = "";

    try {
      seedHost = new URL(seedUrl).hostname;
    } catch {
      seedHost = "";
    }

    while (queue.length && crawledPages < MAX_DISCOVERY_PAGES) {
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

        if (isLikelyJobDetailUrl(absoluteUrl)) {
          jobUrls.add(canonicalizeUrl(currentUrl, absoluteUrl, { dropHash: true }));
        }

        try {
          const parsed = new URL(absoluteUrl);
          const sameHost = !seedHost || parsed.hostname === seedHost;
          const listingPath = isListingPath(parsed.pathname);
          const pagination = isPaginationLink(absoluteUrl, $(el).text());

          if (
            sameHost &&
            (listingPath || pagination) &&
            !visited.has(absoluteUrl) &&
            !queued.has(absoluteUrl)
          ) {
            queue.push(absoluteUrl);
            queued.add(absoluteUrl);
          }
        } catch {
          // Ignore URL parsing failures.
        }
      });
    }

    return Array.from(jobUrls).filter(Boolean);
  });
}

async function fetchJob(club, jobUrl, options = {}) {
  if (isManUtdClub(club)) {
    return fetchManUtdJob(club, jobUrl, options);
  }

  if (isLeedsClub(club)) {
    return fetchLeedsJob(club, jobUrl, options);
  }

  if (isWolvesClub(club)) {
    return fetchWolvesJob(club, jobUrl, options);
  }

  if (isSharedCareersClub(club)) {
    return fetchSharedCareersJob(club, jobUrl, options);
  }

  return withPage(options, async (page) => {
    await gotoWithRetry(page, jobUrl);
    const $ = cheerio.load(await page.content());
    const jobPosting = extractJobPostingJsonLd($);

    let sourceId = "";
    let title = "";
    let location = "";
    let employmentType = "";
    let htmlDescription = "";
    let plainTextDescription = "";
    let publishedAt = "";
    let expiresAt = "";
    let applicationLink = "";

    if (jobPosting) {
      sourceId =
        extractIdentifierFromJsonLd(jobPosting) || extractSourceIdFromUrl(jobUrl);
      title = normalizeText(jobPosting.title || jobPosting.name);
      location = extractLocationFromJsonLd(jobPosting);
      employmentType = extractEmploymentTypeFromJsonLd(jobPosting);
      htmlDescription = normalizeHtmlFragment(jobPosting.description);
      plainTextDescription = htmlToStructuredPlainText(htmlDescription);
      publishedAt = parseDateToIso(jobPosting.datePosted || jobPosting.dateCreated);
      expiresAt = parseDateToIso(jobPosting.validThrough);
      applicationLink = normalizeText(jobPosting.url || "");
    }

    sourceId = sourceId || extractSourceIdFromUrl(jobUrl);
    title = title || normalizeText($("h1").first().text());
    location = location || extractFallbackLocation($);
    employmentType = employmentType || extractFallbackEmploymentType($);
    htmlDescription = htmlDescription || extractFallbackDescriptionHtml($);
    plainTextDescription =
      plainTextDescription || htmlToStructuredPlainText(htmlDescription);
    publishedAt =
      publishedAt ||
      parseDateToIso(
        findValueByLabels($, ["posted on", "published", "date posted", "posted"])
      );
    expiresAt =
      expiresAt ||
      parseDateToIso(
        findValueByLabels($, [
          "closing date",
          "application deadline",
          "expires",
          "valid through",
        ])
      );
    applicationLink = applicationLink || jobUrl;

    return {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      url: jobUrl,
      application_link: applicationLink,
      title,
      arrangement: mapArrangementFromEmploymentType(employmentType),
      employment_type: employmentType,
      location_type: "onsite",
      location,
      published_at: publishedAt,
      expires_at: expiresAt,
      highlighted: false,
      sticky: false,
      html_description: htmlDescription,
      plain_text_description: plainTextDescription,
      company_name: club.name,
      company_url: club.company_url || club.source_url || "",
      company_logo_url: club.company_logo_url || "",
      _jobPosting: jobPosting || undefined,
    };
  });
}

module.exports = {
  discoverJobUrls,
  fetchJob,
  createSession,
};
