const cheerio = require("cheerio");
const {
  MAX_DISCOVERY_PAGES,
  RATE_LIMIT_MS,
  RETRIES,
  normalizeText,
  canonicalizeUrl,
  parseDateToIso,
  htmlToStructuredPlainText,
  normalizeHtmlFragment,
  extractJobPostingJsonLd,
  extractIdentifierFromJsonLd,
  extractLocationFromJsonLd,
  extractEmploymentTypeFromJsonLd,
  selectDescriptionHtml,
  findValueByLabels,
  mapArrangementFromEmploymentType,
} = require("./utils");

const LISTING_CACHE = new Map();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function logHttp(club, message) {
  const clubId = normalizeText(club && club.club_id) || "unknown";
  console.log(`[http] ${clubId} ${message}`);
}

function buildCacheKey(club, sourceId) {
  return `${normalizeText(club && club.club_id)}::${normalizeText(sourceId)}`;
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

function normalizePathname(value) {
  return String(value || "").replace(/\/+$/, "") || "/";
}

function getListingPath(club) {
  try {
    return normalizePathname(new URL(club.source_url).pathname);
  } catch {
    return "/";
  }
}

function extractSourceIdFromTeamworkUrl(jobUrl) {
  try {
    const parsed = new URL(jobUrl);
    const employmentOpportunityMatch = parsed.pathname.match(
      /\/employment_opportunities\/(\d+)\//i
    );
    if (employmentOpportunityMatch) {
      return normalizeText(employmentOpportunityMatch[1]);
    }

    const segments = parsed.pathname.split("/").filter(Boolean);
    const tail = decodeURIComponent(segments[segments.length - 1] || "");
    const detailMatch = tail.match(/-(\d+)$/);
    if (detailMatch) {
      return normalizeText(detailMatch[1]);
    }
  } catch {
    // Ignore URL parsing failures.
  }

  const fallback = String(jobUrl || "").match(/-(\d+)(?:\/)?(?:\?|#|$)/);
  return fallback ? normalizeText(fallback[1]) : "";
}

function isTeamworkApplyUrl(url) {
  try {
    return /\/employment_opportunities\/\d+\/(?:applications|interest_expressions)\/new\/?$/i.test(
      new URL(url).pathname
    );
  } catch {
    return false;
  }
}

function isTeamworkDetailUrlForClub(club, url) {
  try {
    const parsed = new URL(url);
    const listingUrl = new URL(club.source_url);
    const listingPath = normalizePathname(listingUrl.pathname);
    const pathname = normalizePathname(parsed.pathname);

    if (parsed.hostname !== listingUrl.hostname) {
      return false;
    }

    if (pathname === listingPath || !pathname.startsWith(`${listingPath}/`)) {
      return false;
    }

    const suffix = pathname.slice(listingPath.length + 1);
    return Boolean(suffix) && !suffix.includes("/") && /-\d+$/.test(suffix);
  } catch {
    return false;
  }
}

function isPaginationUrlForClub(club, url, linkText) {
  try {
    const parsed = new URL(url);
    const listingUrl = new URL(club.source_url);
    const pathname = normalizePathname(parsed.pathname);
    const listingPath = normalizePathname(listingUrl.pathname);
    const text = normalizeText(linkText).toLowerCase();

    if (parsed.hostname !== listingUrl.hostname) {
      return false;
    }

    if (pathname !== listingPath) {
      return false;
    }

    return (
      parsed.searchParams.has("page") ||
      text.includes("next") ||
      text.includes("more") ||
      text.includes("page")
    );
  } catch {
    return false;
  }
}

function normalizeListingLocation(value) {
  return normalizeText(value).replace(/\s*[·•]\s*/g, ", ");
}

function cacheListingEntry(club, entry) {
  const sourceId = normalizeText(entry && entry.source_id);
  if (!sourceId) {
    return;
  }

  LISTING_CACHE.set(buildCacheKey(club, sourceId), entry);
}

function collectListingData(club, baseUrl, html) {
  const $ = cheerio.load(String(html || ""));
  const jobUrls = new Set();
  const paginationUrls = new Set();

  $(".organization-portal__job, .organization-portal__job-container").each(
    (_, node) => {
      const root = $(node);
      const link = root
        .find(".organization-portal__job-title a[href], a[href]")
        .filter((__, el) => {
          const absoluteUrl = canonicalizeUrl(baseUrl, $(el).attr("href"));
          return isTeamworkDetailUrlForClub(club, absoluteUrl);
        })
        .first();

      if (!link.length) {
        return;
      }

      const jobUrl = canonicalizeUrl(baseUrl, link.attr("href"), {
        dropSearch: true,
      });
      if (!jobUrl) {
        return;
      }

      const sourceId = extractSourceIdFromTeamworkUrl(jobUrl);
      jobUrls.add(jobUrl);
      cacheListingEntry(club, {
        source_id: sourceId,
        url: jobUrl,
        title:
          normalizeText(root.find(".organization-portal__job-title").first().text()) ||
          normalizeText(link.text()),
        location: normalizeListingLocation(
          root.find(".organization-portal__job-location").first().text()
        ),
        career_level: normalizeText(
          root.find(".organization-portal__job__career-level").first().text()
        ),
        department: normalizeText(
          root.find(".organization-portal__job-category").first().text()
        ),
      });
    }
  );

  $("a[href]").each((_, node) => {
    const href = normalizeText($(node).attr("href"));
    const absoluteUrl = canonicalizeUrl(baseUrl, href);
    if (!absoluteUrl) {
      return;
    }

    if (isTeamworkDetailUrlForClub(club, absoluteUrl)) {
      jobUrls.add(canonicalizeUrl(baseUrl, absoluteUrl, { dropSearch: true }));
      return;
    }

    if (isPaginationUrlForClub(club, absoluteUrl, $(node).text())) {
      paginationUrls.add(absoluteUrl);
    }
  });

  return {
    jobUrls: Array.from(jobUrls).filter(Boolean),
    paginationUrls: Array.from(paginationUrls).filter(Boolean),
  };
}

function stripTeamworkTitle(value) {
  const title = normalizeText(value);
  if (!title) {
    return "";
  }

  return normalizeText(
    title
      .replace(/\|\s*TeamWork Online$/i, "")
      .replace(/\s*-\s*TeamWork Online$/i, "")
  );
}

function resolveTeamworkLocation(jobPosting, $, listingCache) {
  const structuredLocation = extractLocationFromJsonLd(jobPosting);
  if (structuredLocation) {
    return structuredLocation;
  }

  const infoItems = $(".opportunity-preview__info-content-item")
    .toArray()
    .map((node) => normalizeText($(node).text()))
    .filter(Boolean);

  const fromInfo = infoItems.find(
    (item) =>
      item.includes("·") ||
      /\b[A-Z][a-z]+,\s*[A-Z]{2}\b/.test(item) ||
      /\b[A-Z][a-z]+\s*[·•]\s*[A-Z]{2}\b/.test(item)
  );
  if (fromInfo) {
    return normalizeListingLocation(fromInfo);
  }

  const fromLabels = findValueByLabels($, [
    "location",
    "job location",
    "office location",
  ]);
  if (fromLabels) {
    return fromLabels;
  }

  return normalizeText(listingCache && listingCache.location);
}

function resolveTeamworkApplyLink($, jobUrl, sourceId) {
  let applyLink = "";

  $("a[href]").each((_, node) => {
    if (applyLink) {
      return;
    }

    const href = normalizeText($(node).attr("href"));
    const absoluteUrl = canonicalizeUrl(jobUrl, href);
    if (!absoluteUrl) {
      return;
    }

    const text = normalizeText($(node).text()).toLowerCase();
    if (text.includes("apply") || isTeamworkApplyUrl(absoluteUrl)) {
      applyLink = absoluteUrl;
    }
  });

  if (applyLink) {
    return applyLink;
  }

  if (sourceId) {
    return canonicalizeUrl(
      jobUrl,
      `/employment_opportunities/${sourceId}/applications/new`
    );
  }

  return normalizeText(jobUrl);
}

function resolveArrangementFromHints(hints) {
  const normalized = hints
    .map((value) => normalizeText(value).toLowerCase())
    .filter(Boolean)
    .join(" ");

  if (!normalized) {
    return "fulltime";
  }

  if (/\bpt\b|part[\s-]*time/.test(normalized)) {
    return "parttime";
  }

  if (/\bft\b|full[\s-]*time/.test(normalized)) {
    return "fulltime";
  }

  if (/casual|seasonal/.test(normalized)) {
    return "casual";
  }

  if (/contract|temporary|\btemp\b|fixed term|fixed-term/.test(normalized)) {
    return "contract";
  }

  return mapArrangementFromEmploymentType(normalized);
}

function resolveLocationType(hints) {
  const normalized = hints
    .map((value) => normalizeText(value).toLowerCase())
    .filter(Boolean)
    .join(" ");

  if (normalized.includes("hybrid")) {
    return "hybrid";
  }

  if (normalized.includes("remote")) {
    return "remote";
  }

  return "onsite";
}

async function discoverJobUrls(club) {
  const queue = [club.source_url];
  const queued = new Set(queue);
  const visited = new Set();
  const jobs = new Set();
  let pages = 0;

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
    } catch (error) {
      console.warn(
        `[warn] ${club.club_id}: teamworkonline listing falló ${currentUrl} -> ${error.message}`
      );
      visited.add(currentUrl);
      pages += 1;
      continue;
    }

    const collected = collectListingData(club, currentUrl, html);
    for (const jobUrl of collected.jobUrls) {
      jobs.add(jobUrl);
    }

    for (const listingUrl of collected.paginationUrls) {
      if (!visited.has(listingUrl) && !queued.has(listingUrl)) {
        queue.push(listingUrl);
        queued.add(listingUrl);
      }
    }

    visited.add(currentUrl);
    pages += 1;
  }

  return Array.from(jobs);
}

async function fetchJob(club, jobUrl) {
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
  const sourceId =
    extractIdentifierFromJsonLd(jobPosting) || extractSourceIdFromTeamworkUrl(jobUrl);
  const listingCache = sourceId
    ? LISTING_CACHE.get(buildCacheKey(club, sourceId))
    : null;

  const metaTitle = stripTeamworkTitle(
    $("meta[property='og:title']").attr("content") ||
      $("meta[name='twitter:title']").attr("content") ||
      $("title").first().text()
  );
  const title =
    normalizeText((jobPosting && (jobPosting.title || jobPosting.name)) || "") ||
    normalizeText($(".opportunity-preview__title").first().text()) ||
    normalizeText($("h1").first().text()) ||
    normalizeText(listingCache && listingCache.title) ||
    metaTitle;

  const htmlDescription =
    normalizeHtmlFragment((jobPosting && jobPosting.description) || "") ||
    normalizeHtmlFragment($(".opportunity-preview__body").first().html() || "") ||
    selectDescriptionHtml($);
  const plainTextDescription = htmlToStructuredPlainText(htmlDescription);
  const employmentType =
    extractEmploymentTypeFromJsonLd(jobPosting) ||
    findValueByLabels($, [
      "employment type",
      "job type",
      "type",
      "status",
      "position type",
    ]) ||
    normalizeText(listingCache && listingCache.career_level);
  const location = resolveTeamworkLocation(jobPosting, $, listingCache);
  const arrangement = resolveArrangementFromHints([
    employmentType,
    normalizeText(listingCache && listingCache.career_level),
    title,
    plainTextDescription.slice(0, 600),
  ]);
  const applicationLink = resolveTeamworkApplyLink($, jobUrl, sourceId);

  return {
    source_id: sourceId,
    id: sourceId,
    url: jobUrl,
    application_link: applicationLink,
    title,
    arrangement,
    employment_type: employmentType,
    location_type: resolveLocationType([
      location,
      employmentType,
      plainTextDescription.slice(0, 600),
    ]),
    location,
    published_at: parseDateToIso(jobPosting && jobPosting.datePosted),
    expires_at: parseDateToIso(jobPosting && jobPosting.validThrough),
    html_description: htmlDescription,
    plain_text_description: plainTextDescription,
    department: normalizeText(listingCache && listingCache.department),
    company_name:
      normalizeText(
        jobPosting &&
          jobPosting.hiringOrganization &&
          jobPosting.hiringOrganization.name
      ) || club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url:
      normalizeText(
        jobPosting &&
          jobPosting.hiringOrganization &&
          jobPosting.hiringOrganization.logo
      ) || club.company_logo_url || "",
    _jobPosting: jobPosting || undefined,
  };
}

module.exports = {
  discoverJobUrls,
  fetchJob,
};
