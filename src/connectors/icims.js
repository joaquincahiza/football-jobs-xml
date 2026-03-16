const cheerio = require("cheerio");
const {
  MAX_DISCOVERY_PAGES,
  RATE_LIMIT_MS,
  RETRIES,
  normalizeText,
  canonicalizeUrl,
  parseDateToIso,
  normalizeHtmlFragment,
  htmlToStructuredPlainText,
  findValueByLabels,
  mapArrangementFromEmploymentType,
  extractJobPostingJsonLd,
  extractLocationFromJsonLd,
  extractEmploymentTypeFromJsonLd,
  selectDescriptionHtml,
} = require("./utils");

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

function extractSourceIdFromIcimsUrl(value) {
  try {
    const parsed = new URL(value);
    const pathMatch = parsed.pathname.match(/\/jobs\/(\d+)\//i);
    if (pathMatch) {
      return normalizeText(pathMatch[1]);
    }
  } catch {
    // Ignore URL parsing failures.
  }

  const match = String(value || "").match(/\/jobs\/(\d+)\//i);
  return match ? normalizeText(match[1]) : "";
}

function isIcimsJobUrl(value) {
  try {
    const parsed = new URL(value);
    return /\/jobs\/\d+\/.+\/job\/?$/i.test(parsed.pathname);
  } catch {
    return false;
  }
}

function collectLinksFromHtml(baseUrl, html) {
  const $ = cheerio.load(String(html || ""));
  const jobUrls = new Set();
  const listingUrls = new Set();

  $("a[href]").each((_, node) => {
    const href = normalizeText($(node).attr("href"));
    const absolute = canonicalizeUrl(baseUrl, href);
    if (!absolute) {
      return;
    }

    if (isIcimsJobUrl(absolute)) {
      jobUrls.add(absolute);
      return;
    }

    try {
      const parsed = new URL(absolute);
      const pathname = normalizeText(parsed.pathname).toLowerCase();
      const text = normalizeText($(node).text()).toLowerCase();

      if (
        pathname.includes("/jobs/search") ||
        pathname.endsWith("/jobs") ||
        pathname.endsWith("/job") ||
        parsed.searchParams.has("ss") ||
        parsed.searchParams.has("mobile") ||
        text.includes("next") ||
        text.includes("more")
      ) {
        listingUrls.add(absolute);
      }
    } catch {
      // Ignore URL parsing failures.
    }
  });

  $("iframe[src]").each((_, node) => {
    const src = normalizeText($(node).attr("src"));
    const absolute = canonicalizeUrl(baseUrl, src);
    if (!absolute) {
      return;
    }

    listingUrls.add(absolute);
  });

  return {
    jobUrls: Array.from(jobUrls),
    listingUrls: Array.from(listingUrls),
  };
}

async function discoverJobUrls(club) {
  const queue = [club.source_url];
  const queued = new Set(queue);
  const visited = new Set();
  const jobs = new Set();
  let pages = 0;
  let seedHost = "";

  try {
    seedHost = new URL(club.source_url).hostname;
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
    } catch (error) {
      console.warn(
        `[warn] ${club.club_id}: icims listing falló ${currentUrl} -> ${error.message}`
      );
      visited.add(currentUrl);
      pages += 1;
      continue;
    }

    const collected = collectLinksFromHtml(currentUrl, html);
    for (const jobUrl of collected.jobUrls) {
      jobs.add(jobUrl);
    }

    for (const listingUrl of collected.listingUrls) {
      try {
        const parsed = new URL(listingUrl);
        if (seedHost && parsed.hostname !== seedHost) {
          continue;
        }
      } catch {
        continue;
      }

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

function resolveLocationType(value) {
  const normalized = normalizeText(value).toLowerCase();
  if (normalized.includes("hybrid")) {
    return "hybrid";
  }
  if (normalized.includes("remote")) {
    return "remote";
  }
  return "onsite";
}

function resolveApplyLink($, baseUrl) {
  let applicationLink = "";

  $("a[href]").each((_, node) => {
    if (applicationLink) {
      return;
    }

    const href = normalizeText($(node).attr("href"));
    const absolute = canonicalizeUrl(baseUrl, href);
    if (!absolute) {
      return;
    }

    const text = normalizeText($(node).text()).toLowerCase();
    if (
      text.includes("apply") ||
      /\/jobs\/\d+\/.+\/job/i.test(absolute) ||
      /icims2\.com/i.test(absolute)
    ) {
      applicationLink = absolute;
    }
  });

  return applicationLink || normalizeText(baseUrl);
}

async function loadJobDetailHtml(club, jobUrl) {
  const primary = await requestTextWithRetry(club, jobUrl, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      Referer: club.source_url,
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    },
  });

  const primary$ = cheerio.load(primary);
  const hasTitle = normalizeText(primary$("h1").first().text());
  if (hasTitle) {
    return primary;
  }

  try {
    const fallbackUrl = `${jobUrl}${jobUrl.includes("?") ? "&" : "?"}in_iframe=1`;
    const fallback = await requestTextWithRetry(club, fallbackUrl, {
      method: "GET",
      headers: {
        Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        Referer: club.source_url,
        "User-Agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      },
    });
    return fallback || primary;
  } catch {
    return primary;
  }
}

async function fetchJob(club, jobUrl) {
  const sourceId = extractSourceIdFromIcimsUrl(jobUrl);
  if (!sourceId) {
    throw new Error(`No se pudo extraer source_id desde ${jobUrl}`);
  }

  const html = await loadJobDetailHtml(club, jobUrl);
  const $ = cheerio.load(html);
  const jobPosting = extractJobPostingJsonLd($);
  const employmentType =
    extractEmploymentTypeFromJsonLd(jobPosting) ||
    findValueByLabels($, ["employment type", "job type", "schedule"]);
  const locationTypeRaw = findValueByLabels($, [
    "location type",
    "work model",
    "workplace",
  ]);
  const htmlDescription = normalizeHtmlFragment(
    (jobPosting && jobPosting.description) || selectDescriptionHtml($)
  );

  return {
    club_id: club.club_id,
    source_id: sourceId,
    id: sourceId,
    title:
      normalizeText(jobPosting && (jobPosting.title || jobPosting.name)) ||
      normalizeText($("h1").first().text()) ||
      normalizeText($("title").first().text()),
    location:
      extractLocationFromJsonLd(jobPosting) ||
      findValueByLabels($, ["location", "city", "office location", "country"]),
    location_type: resolveLocationType(locationTypeRaw),
    department: findValueByLabels($, ["department", "division", "team", "category"]),
    arrangement: mapArrangementFromEmploymentType(employmentType),
    employment_type: employmentType,
    published_at: parseDateToIso(
      (jobPosting && (jobPosting.datePosted || jobPosting.dateCreated)) ||
        findValueByLabels($, ["posted date", "posted on", "date posted", "published"])
    ),
    expires_at: parseDateToIso(
      (jobPosting && jobPosting.validThrough) ||
        findValueByLabels($, ["closing date", "deadline", "expires"])
    ),
    html_description: htmlDescription,
    plain_text_description: htmlToStructuredPlainText(htmlDescription),
    url: normalizeText(jobUrl),
    application_link: resolveApplyLink($, jobUrl),
    company_name: club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url: club.company_logo_url || "",
    source_url: club.source_url || "",
    ats: "icims",
    _jobPosting: jobPosting || undefined,
  };
}

module.exports = {
  discoverJobUrls,
  fetchJob,
};
