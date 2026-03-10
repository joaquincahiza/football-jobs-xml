const cheerio = require("cheerio");
const {
  RATE_LIMIT_MS,
  RETRIES,
  MAX_DISCOVERY_PAGES,
  normalizeText,
  parseDateToIso,
  htmlToStructuredPlainText,
  normalizeHtmlFragment,
  canonicalizeUrl,
  extractJobPostingJsonLd,
  extractIdentifierFromJsonLd,
  extractLocationFromJsonLd,
  extractEmploymentTypeFromJsonLd,
  findValueByLabels,
  selectDescriptionHtml,
  extractSourceIdFromUrl,
  mapArrangementFromEmploymentType,
  createSession,
} = require("./utils");

const LISTING_CACHE = new Map();
const JOB_CACHE = new Map();
const REQUEST_TIMEOUT_MS = 45000;
const DISCOVERY_REQUEST_TIMEOUT_MS = 25000;
const DETAIL_REQUEST_TIMEOUT_MS = 30000;
const DISCOVERY_MAX_RETRIES = 1;
const DETAIL_MAX_RETRIES = 1;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildCacheKey(club, sourceId) {
  return `${normalizeText(club && club.club_id)}::${normalizeText(sourceId)}`;
}

function logHttp(club, message) {
  const clubId = normalizeText(club && club.club_id) || "unknown";
  console.log(`[http] ${clubId} ${message}`);
}

function formatNetworkError(error) {
  if (!error) {
    return "unknown error";
  }

  const message = normalizeText(error.message || String(error));
  const cause = error.cause && typeof error.cause === "object" ? error.cause : null;
  const causeCode = normalizeText(cause && cause.code);
  const causeMessage = normalizeText(cause && cause.message);

  if (causeCode && causeMessage) {
    return `${message} (${causeCode}: ${causeMessage})`;
  }

  if (causeCode) {
    return `${message} (${causeCode})`;
  }

  if (causeMessage) {
    return `${message} (${causeMessage})`;
  }

  return message || "unknown error";
}

function normalizeHost(url) {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return "";
  }
}

function normalizePath(url) {
  try {
    return decodeURIComponent(new URL(url).pathname || "")
      .replace(/\/+$/, "")
      .toLowerCase();
  } catch {
    return "";
  }
}

function normalizeMaybeUrl(baseUrl, value) {
  const text = normalizeText(value);
  if (!text || /^javascript:/i.test(text)) {
    return "";
  }

  if (text.startsWith("//")) {
    return `https:${text}`;
  }

  return canonicalizeUrl(baseUrl, text) || text;
}

async function requestTextWithRetry(club, url, options = {}) {
  let lastError;
  const maxRetries =
    Number.isFinite(Number(options.maxRetries)) && Number(options.maxRetries) >= 0
      ? Number(options.maxRetries)
      : RETRIES;

  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    const timeoutMs =
      Number.isFinite(Number(options.timeoutMs)) && Number(options.timeoutMs) > 0
        ? Number(options.timeoutMs)
        : REQUEST_TIMEOUT_MS;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

    try {
      logHttp(club, `${options.method || "GET"} ${url}`);
      const response = await fetch(url, {
        method: options.method || "GET",
        headers: {
          Accept:
            options.accept ||
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "en-US,en;q=0.9,de-DE;q=0.8,de;q=0.7",
          "User-Agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
          Referer: normalizeText(club && club.source_url),
          ...(options.headers || {}),
        },
        body: options.body,
        signal: controller.signal,
      });
      logHttp(club, `${response.status} ${url}`);

      if (!response.ok) {
        const error = new Error(`HTTP ${response.status}`);
        error.status = response.status;
        throw error;
      }

      const text = await response.text();
      await sleep(RATE_LIMIT_MS);
      return text;
    } catch (error) {
      const normalizedError =
        error && error.name === "AbortError"
          ? new Error(`Request timeout after ${timeoutMs}ms`)
          : error;

      lastError = normalizedError;
      logHttp(club, `ERROR ${url} -> ${formatNetworkError(normalizedError)}`);
      if (attempt < maxRetries) {
        await sleep(RATE_LIMIT_MS);
      }
    } finally {
      clearTimeout(timeoutId);
    }
  }

  if (options.page) {
    try {
      logHttp(club, `GET ${url} (browser fallback)`);
      const response = await options.page.goto(url, {
        waitUntil: "domcontentloaded",
        timeout: options.timeoutMs || REQUEST_TIMEOUT_MS,
      });
      logHttp(
        club,
        `${response ? response.status() : "NO_RESPONSE"} ${url} (browser fallback)`
      );

      if (!response || response.status() >= 400) {
        throw new Error(
          `HTTP ${response ? response.status() : "NO_RESPONSE"} in browser fallback`
        );
      }

      const html = await options.page.content();
      await sleep(RATE_LIMIT_MS);
      return html;
    } catch (browserError) {
      logHttp(club, `ERROR ${url} (browser fallback) -> ${formatNetworkError(browserError)}`);
    }
  }

  throw lastError || new Error("error de request text");
}

function extractSourceIdFromSuccessFactorsUrl(jobUrl) {
  try {
    const parsed = new URL(jobUrl);
    const queryId = normalizeText(
      parsed.searchParams.get("jobPipeline") ||
        parsed.searchParams.get("jobReqId") ||
        parsed.searchParams.get("jobId") ||
        ""
    );

    if (queryId) {
      return queryId;
    }

    const pathname = decodeURIComponent(parsed.pathname || "");
    const byJobPath = pathname.match(/\/job\/[^/?#]+\/(\d{5,})\/?$/i);
    if (byJobPath) {
      return normalizeText(byJobPath[1]);
    }

    const byTail = pathname.match(/\/(\d{5,})\/?$/);
    if (byTail) {
      return normalizeText(byTail[1]);
    }
  } catch {
    // Ignore URL parsing failures.
  }

  return extractSourceIdFromUrl(jobUrl);
}

function isSuccessFactorsJobUrl(url) {
  try {
    const parsed = new URL(url);
    if (!/\/job\//i.test(parsed.pathname || "")) {
      return false;
    }

    return Boolean(extractSourceIdFromSuccessFactorsUrl(url));
  } catch {
    return false;
  }
}

function mergeListingEntry(existing, incoming) {
  const merged = {
    source_id: normalizeText(existing && existing.source_id),
    url: normalizeText(existing && existing.url),
    title: normalizeText(existing && existing.title),
    department: normalizeText(existing && existing.department),
    location: normalizeText(existing && existing.location),
    published_at: normalizeText(existing && existing.published_at),
    application_link: normalizeText(existing && existing.application_link),
  };

  const candidate = incoming || {};

  if (!merged.source_id) {
    merged.source_id = normalizeText(candidate.source_id);
  }

  if (!merged.url) {
    merged.url = normalizeText(candidate.url);
  }

  if (!merged.title) {
    merged.title = normalizeText(candidate.title);
  }

  if (!merged.department) {
    merged.department = normalizeText(candidate.department);
  }

  if (!merged.location) {
    merged.location = normalizeText(candidate.location);
  }

  if (!merged.published_at) {
    merged.published_at = normalizeText(candidate.published_at);
  }

  if (!merged.application_link) {
    merged.application_link = normalizeText(candidate.application_link);
  }

  return merged;
}

function extractDateText(value) {
  const text = normalizeText(value);
  if (!text) {
    return "";
  }

  const dateMatch = text.match(
    /\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}\b/i
  );

  return dateMatch ? normalizeText(dateMatch[0]) : "";
}

function extractLocationText(value) {
  const text = normalizeText(value);
  if (!text) {
    return "";
  }

  const locationMatch = text.match(
    /\b([A-Za-zÀ-ÿ0-9.'\-\s]+,\s*[A-Z]{2})\b/
  );

  return locationMatch ? normalizeText(locationMatch[1]) : "";
}

function extractListingEntries($, pageUrl) {
  const entriesBySourceId = new Map();

  $("table tr").each((_, row) => {
    const anchor = $(row).find('a[href*="/job/"]').first();
    if (!anchor.length) {
      return;
    }

    const jobUrl = normalizeMaybeUrl(pageUrl, anchor.attr("href"));
    if (!isSuccessFactorsJobUrl(jobUrl)) {
      return;
    }

    const sourceId = extractSourceIdFromSuccessFactorsUrl(jobUrl);
    if (!sourceId) {
      return;
    }

    const cells = $(row).children("th,td").toArray();
    const title = normalizeText(anchor.text());
    const department = cells.length >= 3 ? normalizeText($(cells[1]).text()) : "";
    const location = cells.length >= 4 ? normalizeText($(cells[2]).text()) : "";
    const rawDate = cells.length >= 5 ? normalizeText($(cells[3]).text()) : "";

    const entry = {
      source_id: sourceId,
      url: jobUrl,
      title,
      department,
      location,
      published_at: parseDateToIso(rawDate),
    };

    const existing = entriesBySourceId.get(sourceId);
    entriesBySourceId.set(sourceId, mergeListingEntry(existing, entry));
  });

  $("a[href]").each((_, node) => {
    const jobUrl = normalizeMaybeUrl(pageUrl, $(node).attr("href"));
    if (!isSuccessFactorsJobUrl(jobUrl)) {
      return;
    }

    const sourceId = extractSourceIdFromSuccessFactorsUrl(jobUrl);
    if (!sourceId) {
      return;
    }

    const container = $(node)
      .closest("tr,li,article,[class*='job'],[class*='result'],div")
      .first();
    const contextText = normalizeText(container.text());

    const entry = {
      source_id: sourceId,
      url: jobUrl,
      title: normalizeText($(node).text()),
      location: extractLocationText(contextText),
      published_at: parseDateToIso(extractDateText(contextText)),
    };

    const existing = entriesBySourceId.get(sourceId);
    entriesBySourceId.set(sourceId, mergeListingEntry(existing, entry));
  });

  return Array.from(entriesBySourceId.values()).filter(
    (entry) => entry && entry.source_id && entry.url
  );
}

function extractJobUrlsFromRawHtml(pageUrl, html) {
  const urls = new Set();
  const pattern =
    /(?:https?:\/\/[^"'\s<>]+|\/job\/[^"'\s<>]+\/\d{5,}\/?[^"'\s<>]*)/gi;

  for (const raw of String(html || "").match(pattern) || []) {
    const candidate = normalizeMaybeUrl(pageUrl, raw);
    if (!isSuccessFactorsJobUrl(candidate)) {
      continue;
    }

    urls.add(candidate);
  }

  return Array.from(urls);
}

function extractPaginationUrls(club, $, pageUrl) {
  const urls = new Set();
  const sourceHost = normalizeHost(club && club.source_url);
  const sourcePath = normalizePath(club && club.source_url);

  $("a[href]").each((_, node) => {
    const href = normalizeMaybeUrl(pageUrl, $(node).attr("href"));
    if (!href || isSuccessFactorsJobUrl(href) || href === pageUrl) {
      return;
    }

    let parsed;
    try {
      parsed = new URL(href);
    } catch {
      return;
    }

    if (sourceHost && parsed.hostname.toLowerCase() !== sourceHost) {
      return;
    }

    const pathname = decodeURIComponent(parsed.pathname || "")
      .replace(/\/+$/, "")
      .toLowerCase();
    if (!sourcePath || !pathname.startsWith(sourcePath)) {
      return;
    }

    const linkText = normalizeText($(node).text()).toLowerCase();
    const linkTitle = normalizeText($(node).attr("title")).toLowerCase();
    const pageNumberInPath = /\/\d+\/?$/i.test(pathname);
    const isSortLink =
      parsed.searchParams.has("sortColumn") ||
      parsed.searchParams.has("sortDirection") ||
      /#?hdr[a-z]+button$/i.test(parsed.hash || "");

    const looksLikePagerText =
      /^\d+$/.test(linkText) ||
      linkText === "«" ||
      linkText === "»" ||
      linkText.includes("next") ||
      linkText.includes("prev") ||
      linkText.includes("page") ||
      linkTitle.includes("page");
    const hasPagingParams = parsed.searchParams.has("q") || pageNumberInPath;

    // Ignore sort-only links from table headers; follow only real paging links.
    if (isSortLink && !pageNumberInPath && !linkTitle.includes("page")) {
      return;
    }

    if (looksLikePagerText || hasPagingParams) {
      urls.add(href);
    }
  });

  return Array.from(urls);
}

function resolveDescriptionHtml($, jobPosting) {
  const jsonLdDescription = normalizeHtmlFragment(jobPosting && jobPosting.description);
  if (jsonLdDescription) {
    return jsonLdDescription;
  }

  const selectors = [
    '[itemprop="description"]',
    "#jobdescription",
    ".jobdescription",
    "[class*='jobdescription']",
    "#content .article",
    "#content",
    "main",
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

    const html = normalizeHtmlFragment(node.html());
    if (html) {
      return html;
    }
  }

  return normalizeHtmlFragment(selectDescriptionHtml($));
}

function resolveApplicationLink(club, jobUrl, $, jobPosting, listingEntry) {
  const candidates = [];

  if (jobPosting && jobPosting.url) {
    candidates.push(jobPosting.url);
  }

  if (listingEntry && listingEntry.application_link) {
    candidates.push(listingEntry.application_link);
  }

  $("a[href]").each((_, node) => {
    const href = normalizeText($(node).attr("href"));
    if (!href) {
      return;
    }

    const text = normalizeText($(node).text()).toLowerCase();
    const lowerHref = href.toLowerCase();

    if (
      text.includes("apply") ||
      text.includes("bewerb") ||
      lowerHref.includes("jobpipeline") ||
      lowerHref.includes("career?company") ||
      lowerHref.includes("/application") ||
      lowerHref.includes("applyonline")
    ) {
      candidates.push(href);
    }
  });

  for (const candidate of candidates) {
    const normalized = normalizeMaybeUrl(jobUrl, candidate);
    if (!normalized || /^javascript:/i.test(normalized)) {
      continue;
    }

    return normalized;
  }

  return normalizeText(jobUrl);
}

function resolveDepartment($, listingEntry) {
  const fromListing = normalizeText(listingEntry && listingEntry.department);
  if (fromListing) {
    return fromListing;
  }

  const byLabel = findValueByLabels($, [
    "division",
    "department",
    "abteilung",
    "bereich",
    "team",
  ]);

  return normalizeText(byLabel || "");
}

function isLikelyLocationValue(value) {
  const normalized = normalizeText(value);
  if (!normalized) {
    return false;
  }

  const lowered = normalized.toLowerCase();
  if (
    lowered.includes("bereiche") ||
    lowered.includes("locations &") ||
    lowered === "location" ||
    lowered === "standort"
  ) {
    return false;
  }

  return true;
}

function resolveLocation($, jobPosting, listingEntry) {
  const fromJsonLd = extractLocationFromJsonLd(jobPosting);
  if (isLikelyLocationValue(fromJsonLd)) {
    return normalizeText(fromJsonLd);
  }

  const fromListing = normalizeText(listingEntry && listingEntry.location);
  if (isLikelyLocationValue(fromListing)) {
    return fromListing;
  }

  const byLabel = findValueByLabels($, [
    "location",
    "standort",
    "job location",
    "arbeitsort",
    "city",
  ]);
  if (isLikelyLocationValue(byLabel)) {
    return normalizeText(byLabel);
  }

  return fromListing;
}

function resolvePublishedAt($, jobPosting, listingEntry) {
  const candidates = [
    normalizeText(jobPosting && (jobPosting.datePosted || jobPosting.dateCreated)),
    normalizeText(listingEntry && listingEntry.published_at),
    findValueByLabels($, ["date", "posted on", "published", "datum", "referencedate"]),
  ];

  for (const candidate of candidates) {
    const parsed = parseDateToIso(candidate);
    if (parsed) {
      return parsed;
    }
  }

  return "";
}

async function discoverJobUrls(club, options = {}) {
  const queue = [normalizeText(club && club.source_url)];
  const queued = new Set(queue);
  const visited = new Set();
  const foundEntries = new Map();
  let crawledPages = 0;

  while (queue.length && crawledPages < MAX_DISCOVERY_PAGES) {
    const currentUrl = queue.shift();
    queued.delete(currentUrl);

    if (!currentUrl || visited.has(currentUrl)) {
      continue;
    }

    let html;

    try {
      html = await requestTextWithRetry(club, currentUrl, {
        ...options,
        timeoutMs: DISCOVERY_REQUEST_TIMEOUT_MS,
        maxRetries: DISCOVERY_MAX_RETRIES,
      });
    } catch (error) {
      console.warn(
        `[warn] ${club.club_id}: successfactors discover fallo ${currentUrl} -> ${formatNetworkError(error)}`
      );
      visited.add(currentUrl);
      continue;
    }

    visited.add(currentUrl);
    crawledPages += 1;

    const $ = cheerio.load(html);
    const listingEntries = extractListingEntries($, currentUrl);

    for (const entry of listingEntries) {
      const sourceId = normalizeText(entry.source_id);
      if (!sourceId) {
        continue;
      }

      const existing = foundEntries.get(sourceId);
      const merged = mergeListingEntry(existing, entry);
      foundEntries.set(sourceId, merged);
      LISTING_CACHE.set(buildCacheKey(club, sourceId), merged);
    }

    for (const rawJobUrl of extractJobUrlsFromRawHtml(currentUrl, html)) {
      const sourceId = extractSourceIdFromSuccessFactorsUrl(rawJobUrl);
      if (!sourceId) {
        continue;
      }

      const existing = foundEntries.get(sourceId);
      const merged = mergeListingEntry(existing, {
        source_id: sourceId,
        url: rawJobUrl,
      });
      foundEntries.set(sourceId, merged);
      LISTING_CACHE.set(buildCacheKey(club, sourceId), merged);
    }

    const paginationUrls = extractPaginationUrls(club, $, currentUrl);
    for (const pageUrl of paginationUrls) {
      if (!visited.has(pageUrl) && !queued.has(pageUrl)) {
        queue.push(pageUrl);
        queued.add(pageUrl);
      }
    }
  }

  return Array.from(foundEntries.values())
    .map((entry) => normalizeText(entry.url))
    .filter(Boolean);
}

async function fetchJob(club, jobUrl, options = {}) {
  const sourceId = extractSourceIdFromSuccessFactorsUrl(jobUrl);
  if (!sourceId) {
    throw new Error(`No se pudo extraer source_id desde ${jobUrl}`);
  }

  const cacheKey = buildCacheKey(club, sourceId);
  const listingEntry = LISTING_CACHE.get(cacheKey) || null;

  let html;

  try {
    html = await requestTextWithRetry(club, jobUrl, {
      ...options,
      timeoutMs: DETAIL_REQUEST_TIMEOUT_MS,
      maxRetries: DETAIL_MAX_RETRIES,
    });
  } catch (error) {
    console.warn(
      `[warn] ${club.club_id}: successfactors fetch fallback ${jobUrl} -> ${formatNetworkError(error)}`
    );

    const fallbackJob = {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      title: normalizeText((listingEntry && listingEntry.title) || ""),
      department: normalizeText((listingEntry && listingEntry.department) || ""),
      location: normalizeText((listingEntry && listingEntry.location) || ""),
      published_at: normalizeText((listingEntry && listingEntry.published_at) || ""),
      url: normalizeText(jobUrl),
      application_link: normalizeText(
        (listingEntry && listingEntry.application_link) || jobUrl
      ),
      html_description: "",
      plain_text_description: "",
      company_name: club.name,
      company_url: club.company_url || club.source_url || "",
      company_logo_url: club.company_logo_url || "",
      source_url: club.source_url || "",
      ats: "successfactors",
    };

    JOB_CACHE.set(cacheKey, fallbackJob);
    return fallbackJob;
  }

  const $ = cheerio.load(html);
  const jobPosting = extractJobPostingJsonLd($);
  const employmentType = normalizeText(
    extractEmploymentTypeFromJsonLd(jobPosting) ||
      findValueByLabels($, [
        "employment type",
        "job type",
        "contract type",
        "besch\u00e4ftigungsart",
      ])
  );

  const htmlDescription = resolveDescriptionHtml($, jobPosting);
  const plainTextDescription = htmlToStructuredPlainText(htmlDescription);
  const title =
    normalizeText(
      (jobPosting && (jobPosting.title || jobPosting.name)) ||
        (listingEntry && listingEntry.title) ||
        $("h1").first().text()
    ) || `job-${sourceId}`;

  const job = {
    club_id: club.club_id,
    source_id: sourceId,
    id: sourceId,
    title,
    department: resolveDepartment($, listingEntry),
    location: resolveLocation($, jobPosting, listingEntry),
    published_at: resolvePublishedAt($, jobPosting, listingEntry),
    expires_at: parseDateToIso(jobPosting && jobPosting.validThrough),
    arrangement: mapArrangementFromEmploymentType(employmentType),
    employment_type: employmentType,
    url: normalizeText(jobUrl),
    application_link: resolveApplicationLink(
      club,
      jobUrl,
      $,
      jobPosting,
      listingEntry
    ),
    html_description: htmlDescription,
    plain_text_description: plainTextDescription,
    company_name: club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url: club.company_logo_url || "",
    source_url: club.source_url || "",
    ats: "successfactors",
    _meta: {
      source_identifier:
        normalizeText(extractIdentifierFromJsonLd(jobPosting)) || sourceId,
    },
  };

  JOB_CACHE.set(cacheKey, job);
  return job;
}

module.exports = {
  discoverJobUrls,
  fetchJob,
  createSession,
};
