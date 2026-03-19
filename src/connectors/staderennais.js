const cheerio = require("cheerio");
const {
  MAX_DISCOVERY_PAGES,
  RATE_LIMIT_MS,
  RETRIES,
  normalizeText,
  parseDateToIso,
  canonicalizeUrl,
  htmlToStructuredPlainText,
  normalizeHtmlFragment,
  extractJobPostingJsonLd,
  extractLocationFromJsonLd,
  extractEmploymentTypeFromJsonLd,
  mapArrangementFromEmploymentType,
  selectDescriptionHtml,
  escapeHtml,
} = require("./utils");

const STADE_RENNAIS_COMPANY_REGEX = /stade\s+rennais/i;
const DETAIL_PATH_REGEX = /\/offre\/([^/?#]+)/i;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function logHttp(club, message) {
  const clubId = normalizeText(club && club.club_id) || "unknown";
  console.log(`[http] ${clubId} ${message}`);
}

async function requestTextWithRetry(club, url, options = {}) {
  let lastError;

  for (let attempt = 0; attempt <= RETRIES; attempt += 1) {
    try {
      logHttp(club, `${options.method || "GET"} ${url}`);
      const response = await fetch(url, options);
      logHttp(club, `${response.status} ${url}`);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const text = await response.text();
      await sleep(RATE_LIMIT_MS);
      return text;
    } catch (error) {
      lastError = error;
      logHttp(club, `ERROR ${url} -> ${error.message}`);
      if (attempt < RETRIES) {
        await sleep(RATE_LIMIT_MS);
      }
    }
  }

  throw lastError || new Error("error de request text");
}

function parseJsonSafe(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }

  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function extractItemListUrls($, currentUrl) {
  const urls = [];

  $('script[type="application/ld+json"]').each((_, node) => {
    const payload = parseJsonSafe($(node).text());
    if (!payload || payload["@type"] !== "ItemList") {
      return;
    }

    const items = Array.isArray(payload.itemListElement)
      ? payload.itemListElement
      : [];

    for (const item of items) {
      const absoluteUrl = canonicalizeUrl(
        currentUrl,
        normalizeText(item && item.url)
      );

      if (absoluteUrl && DETAIL_PATH_REGEX.test(absoluteUrl)) {
        urls.push(absoluteUrl);
      }
    }
  });

  return urls;
}

function extractCompaniesFromGtm($) {
  const node = $("[data-gtm-product-display-param]").first();
  const raw = normalizeText(node.attr("data-gtm-product-display-param"));
  if (!raw) {
    return [];
  }

  const parsed = parseJsonSafe(raw);
  if (!Array.isArray(parsed)) {
    return [];
  }

  return parsed.map((entry) => normalizeText(entry && entry.product_company));
}

function shouldIncludeCompany(name) {
  return STADE_RENNAIS_COMPANY_REGEX.test(normalizeText(name));
}

function extractDetailCandidates($, currentUrl) {
  const itemUrls = extractItemListUrls($, currentUrl);
  const companies = extractCompaniesFromGtm($);

  if (itemUrls.length && companies.length && itemUrls.length === companies.length) {
    return itemUrls
      .map((url, index) => ({
        url,
        company: companies[index],
      }))
      .filter((entry) => shouldIncludeCompany(entry.company));
  }

  if (itemUrls.length && companies.length) {
    return [];
  }

  return [];
}

function extractPaginationUrls($, currentUrl) {
  const urls = new Set();

  $('a[href*="/offres"]').each((_, node) => {
    const href = normalizeText($(node).attr("href"));
    const absoluteUrl = canonicalizeUrl(currentUrl, href);
    if (!absoluteUrl) {
      return;
    }

    try {
      const parsed = new URL(absoluteUrl);
      if (parsed.pathname !== "/offres") {
        return;
      }

      if (parsed.searchParams.has("page")) {
        urls.add(parsed.href);
      }
    } catch {
      // Ignore URL parsing failures.
    }
  });

  return Array.from(urls);
}

function extractSourceId(jobUrl) {
  const match = normalizeText(jobUrl).match(DETAIL_PATH_REGEX);
  return match ? normalizeText(match[1]) : "";
}

function buildDescriptionHtml(jobPosting, $) {
  const fromJsonLd = normalizeHtmlFragment(jobPosting && jobPosting.description);
  if (fromJsonLd) {
    const hasTag = /<([a-z][a-z0-9]*)\b[^>]*>/i.test(fromJsonLd);
    return hasTag ? fromJsonLd : `<p>${escapeHtml(fromJsonLd)}</p>`;
  }

  const selected = normalizeHtmlFragment(selectDescriptionHtml($));
  if (selected) {
    return selected;
  }

  return "";
}

function resolveLocationType(jobPosting) {
  const text = normalizeText(
    jobPosting &&
      (jobPosting.jobLocationType ||
        jobPosting.locationType ||
        jobPosting.employmentType ||
        "")
  ).toLowerCase();

  if (text.includes("hybrid")) {
    return "hybrid";
  }

  if (text.includes("remote")) {
    return "remote";
  }

  return "onsite";
}

function resolveApplicationLink($, jobUrl) {
  let applicationLink = "";

  $("a[href]").each((_, node) => {
    if (applicationLink) {
      return;
    }

    const href = normalizeText($(node).attr("href"));
    const absoluteUrl = canonicalizeUrl(jobUrl, href) || href;
    const text = normalizeText($(node).text()).toLowerCase();

    if (text.includes("postuler") || text.includes("apply")) {
      applicationLink = absoluteUrl;
    }
  });

  return applicationLink || jobUrl;
}

async function discoverJobUrls(club) {
  const startUrl = canonicalizeUrl(club.source_url, "/offres") || club.source_url;
  const queue = [startUrl];
  const queued = new Set(queue);
  const visited = new Set();
  const jobUrls = new Set();
  let pages = 0;
  let seedHost = "";

  try {
    seedHost = new URL(startUrl).hostname;
  } catch {
    seedHost = "";
  }

  while (queue.length && pages < MAX_DISCOVERY_PAGES) {
    const currentUrl = queue.shift();
    queued.delete(currentUrl);

    if (visited.has(currentUrl)) {
      continue;
    }

    let html = "";
    try {
      html = await requestTextWithRetry(club, currentUrl, {
        method: "GET",
        headers: {
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          Referer: club.source_url,
          "User-Agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        },
      });
    } catch {
      visited.add(currentUrl);
      pages += 1;
      continue;
    }

    const $ = cheerio.load(html);
    const candidates = extractDetailCandidates($, currentUrl);
    for (const candidate of candidates) {
      if (candidate && candidate.url) {
        jobUrls.add(candidate.url);
      }
    }

    for (const nextUrl of extractPaginationUrls($, currentUrl)) {
      try {
        const parsed = new URL(nextUrl);
        if (seedHost && parsed.hostname !== seedHost) {
          continue;
        }
      } catch {
        continue;
      }

      if (!visited.has(nextUrl) && !queued.has(nextUrl)) {
        queue.push(nextUrl);
        queued.add(nextUrl);
      }
    }

    visited.add(currentUrl);
    pages += 1;
  }

  return Array.from(jobUrls);
}

async function fetchJob(club, jobUrl) {
  const sourceId = extractSourceId(jobUrl);
  if (!sourceId) {
    return {
      source_id: "",
      id: "",
      title: "",
      url: jobUrl,
    };
  }

  const html = await requestTextWithRetry(club, jobUrl, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      Referer: club.source_url,
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    },
  });

  const $ = cheerio.load(html);
  const jobPosting = extractJobPostingJsonLd($);
  const hiringOrganization = normalizeText(
    jobPosting &&
      jobPosting.hiringOrganization &&
      jobPosting.hiringOrganization.name
  );

  if (!shouldIncludeCompany(hiringOrganization)) {
    return {
      source_id: "",
      id: "",
      title: normalizeText(jobPosting && jobPosting.title),
      url: jobUrl,
    };
  }

  const employmentType = extractEmploymentTypeFromJsonLd(jobPosting);
  const htmlDescription = buildDescriptionHtml(jobPosting, $);
  const plainTextDescription = htmlToStructuredPlainText(htmlDescription);

  return {
    club_id: club.club_id,
    source_id: sourceId,
    id: sourceId,
    title:
      normalizeText(jobPosting && (jobPosting.title || jobPosting.name)) ||
      normalizeText($("h1").first().text()),
    department: normalizeText(
      jobPosting &&
        (jobPosting.occupationalCategory ||
          (Array.isArray(jobPosting.industry) ? jobPosting.industry[0] : ""))
    ),
    location: extractLocationFromJsonLd(jobPosting),
    location_type: resolveLocationType(jobPosting),
    arrangement: mapArrangementFromEmploymentType(employmentType),
    employment_type: employmentType,
    html_description: htmlDescription,
    plain_text_description: plainTextDescription,
    published_at: parseDateToIso(jobPosting && jobPosting.datePosted),
    expires_at: parseDateToIso(jobPosting && jobPosting.validThrough),
    url: jobUrl,
    application_link: resolveApplicationLink($, jobUrl),
    company_name: club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url: club.company_logo_url || "",
    source_url: club.source_url || "",
    ats: "custom",
  };
}

module.exports = {
  discoverJobUrls,
  fetchJob,
};
