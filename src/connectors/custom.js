const cheerio = require("cheerio");
const {
  MAX_DISCOVERY_PAGES,
  withPage,
  gotoWithRetry,
  canonicalizeUrl,
  normalizeText,
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

function buildManUtdCacheKey(club, sourceId) {
  return `${normalizeText(club && club.club_id)}::${normalizeText(sourceId)}`;
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

async function discoverJobUrls(club, options = {}) {
  if (isManUtdClub(club)) {
    return discoverManUtdJobUrls(club, options);
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
