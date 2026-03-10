const cheerio = require("cheerio");
const {
  RATE_LIMIT_MS,
  RETRIES,
  normalizeText,
  parseDateToIso,
  htmlToStructuredPlainText,
  normalizeHtmlFragment,
  canonicalizeUrl,
  extractJobPostingJsonLd,
  extractLocationFromJsonLd,
  extractEmploymentTypeFromJsonLd,
  mapArrangementFromEmploymentType,
  findValueByLabels,
} = require("./utils");

const FEED_CACHE = new Map();
const JOB_CACHE = new Map();

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

  for (let attempt = 0; attempt <= RETRIES; attempt += 1) {
    try {
      logHttp(club, `${options.method || "GET"} ${url}`);
      const response = await fetch(url, {
        method: options.method || "GET",
        headers: {
          Accept:
            options.accept ||
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
          "User-Agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
          Referer: normalizeText(club && club.source_url),
          ...(options.headers || {}),
        },
        body: options.body,
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
      lastError = error;
      logHttp(club, `ERROR ${url} -> ${error.message}`);
      if (attempt < RETRIES) {
        await sleep(RATE_LIMIT_MS);
      }
    }
  }

  throw lastError || new Error("error de request text");
}

async function requestJsonWithRetry(club, url, options = {}) {
  const text = await requestTextWithRetry(club, url, {
    ...options,
    accept: "application/json, text/plain, */*",
  });

  return JSON.parse(text);
}

function extractSourceIdFromSoftgardenUrl(jobUrl) {
  try {
    const parsed = new URL(jobUrl);
    const byPath = (parsed.pathname || "").match(/\/jobs\/([a-z0-9-]{6,})\//i);
    if (byPath) {
      return normalizeText(byPath[1]);
    }

    const byQuery = normalizeText(
      parsed.searchParams.get("jobId") ||
        parsed.searchParams.get("jp") ||
        parsed.searchParams.get("id") ||
        ""
    );
    if (byQuery) {
      return byQuery;
    }
  } catch {
    // Ignore URL parsing failures.
  }

  const byText = String(jobUrl || "").match(/\/jobs\/([a-z0-9-]{6,})\//i);
  return byText ? normalizeText(byText[1]) : "";
}

function buildFeedUrl(club) {
  try {
    const parsed = new URL(club.source_url);
    return `${parsed.origin}/jobs.feed.json`;
  } catch {
    return "";
  }
}

function extractFeedLocation(jobPosting) {
  const source =
    (jobPosting && (jobPosting.jobLocation || jobPosting.location || "")) || "";

  const parts = [];

  const pushValue = (value) => {
    const normalized = normalizeText(value);
    if (normalized) {
      parts.push(normalized);
    }
  };

  const visit = (value) => {
    if (!value) {
      return;
    }

    if (Array.isArray(value)) {
      for (const item of value) {
        visit(item);
      }
      return;
    }

    if (typeof value === "string") {
      pushValue(value);
      return;
    }

    if (typeof value === "object") {
      if (value.address) {
        visit(value.address);
      }

      pushValue(value.name);
      pushValue(value.addressLocality);
      pushValue(value.addressRegion);
      pushValue(value.addressCountry);
    }
  };

  visit(source);

  return Array.from(new Set(parts)).join(", ");
}

function normalizeFeedItem(club, feedElement) {
  const jobPosting = (feedElement && feedElement.item) || feedElement || {};

  const sourceId =
    normalizeText(
      jobPosting &&
        jobPosting.identifier &&
        typeof jobPosting.identifier === "object"
        ? jobPosting.identifier.value
        : ""
    ) || extractSourceIdFromSoftgardenUrl(jobPosting.url || "");

  if (!sourceId) {
    return null;
  }

  const url =
    normalizeMaybeUrl(club.source_url, jobPosting.url) ||
    normalizeMaybeUrl(club.source_url, `/jobs/${sourceId}/`);

  const htmlDescription = normalizeHtmlFragment(jobPosting.description || "");

  return {
    club_id: club.club_id,
    source_id: sourceId,
    id: sourceId,
    title: normalizeText(jobPosting.title || ""),
    department: normalizeText(jobPosting.category || ""),
    location: extractFeedLocation(jobPosting),
    location_type: "onsite",
    arrangement: mapArrangementFromEmploymentType(
      normalizeText(jobPosting.employmentType || "")
    ),
    employment_type: normalizeText(jobPosting.employmentType || ""),
    url,
    application_link: url,
    published_at: parseDateToIso(jobPosting.datePosted || ""),
    expires_at: parseDateToIso(jobPosting.validThrough || ""),
    html_description: htmlDescription,
    plain_text_description: htmlToStructuredPlainText(htmlDescription),
    company_name: club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url: club.company_logo_url || "",
    source_url: club.source_url || "",
    ats: "softgarden",
  };
}

async function loadFeed(club) {
  const feedUrl = buildFeedUrl(club);
  if (!feedUrl) {
    throw new Error("No se pudo construir jobs.feed.json para softgarden");
  }

  const payload = await requestJsonWithRetry(club, feedUrl);
  const items = Array.isArray(payload && payload.dataFeedElement)
    ? payload.dataFeedElement
    : [];

  const jobs = [];

  for (const item of items) {
    const normalized = normalizeFeedItem(club, item);
    if (!normalized || !normalized.source_id) {
      continue;
    }

    FEED_CACHE.set(buildCacheKey(club, normalized.source_id), normalized);
    jobs.push(normalized);
  }

  return jobs;
}

function extractNextDataJob($) {
  const raw = $("#__NEXT_DATA__").html();
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw);
    return parsed?.props?.pageProps?.data?.context?.job || null;
  } catch {
    return null;
  }
}

function resolveApplyLink(jobUrl, $, nextDataJob, fallback) {
  const candidates = [];

  if (nextDataJob && typeof nextDataJob === "object") {
    candidates.push(nextDataJob.applyOnlineLink);
    candidates.push(nextDataJob.linkOfModernForApplyApplication);
  }

  $("a[href]").each((_, node) => {
    const href = normalizeText($(node).attr("href"));
    if (!href) {
      return;
    }

    const text = normalizeText($(node).text()).toLowerCase();
    const lowerHref = href.toLowerCase();

    if (
      lowerHref.includes("applyonline") ||
      lowerHref.includes("/application/") ||
      lowerHref.includes("?jobid=") ||
      text.includes("apply") ||
      text.includes("bewerb")
    ) {
      candidates.push(href);
    }
  });

  for (const candidate of candidates) {
    const normalized = normalizeMaybeUrl(jobUrl, candidate);
    if (normalized) {
      return normalized;
    }
  }

  return normalizeText(fallback || jobUrl);
}

function resolveLocationType(rawValue) {
  const normalized = normalizeText(rawValue).toLowerCase();

  if (!normalized) {
    return "onsite";
  }

  if (normalized.includes("remote")) {
    return "remote";
  }

  if (normalized.includes("hybrid") || normalized.includes("home office")) {
    return "hybrid";
  }

  return "onsite";
}

function parseSoftgardenDate(candidates) {
  const list = Array.isArray(candidates) ? candidates : [candidates];

  for (const candidate of list) {
    if (candidate === null || candidate === undefined || candidate === false) {
      continue;
    }

    if (typeof candidate === "number" && Number.isFinite(candidate)) {
      const parsed = new Date(candidate);
      if (!Number.isNaN(parsed.getTime())) {
        return parsed.toISOString();
      }
      continue;
    }

    if (typeof candidate === "boolean") {
      continue;
    }

    const parsed = parseDateToIso(candidate);
    if (parsed) {
      return parsed;
    }
  }

  return "";
}

async function discoverJobUrls(club) {
  let jobs;

  try {
    jobs = await loadFeed(club);
  } catch (error) {
    console.warn(`[warn] ${club.club_id}: softgarden discover falló -> ${error.message}`);
    return [];
  }

  return jobs.map((job) => normalizeText(job.url)).filter(Boolean);
}

async function fetchJob(club, jobUrl) {
  const sourceId = extractSourceIdFromSoftgardenUrl(jobUrl);
  if (!sourceId) {
    throw new Error(`No se pudo extraer source_id desde ${jobUrl}`);
  }

  const cacheKey = buildCacheKey(club, sourceId);
  let cachedJob = FEED_CACHE.get(cacheKey) || JOB_CACHE.get(cacheKey) || null;

  if (!cachedJob) {
    try {
      const feedJobs = await loadFeed(club);
      cachedJob = feedJobs.find((job) => normalizeText(job.source_id) === sourceId) || null;
    } catch (error) {
      console.warn(`[warn] ${club.club_id}: softgarden feed fallback -> ${error.message}`);
    }
  }

  let html = "";

  try {
    html = await requestTextWithRetry(club, jobUrl);
  } catch (error) {
    if (cachedJob) {
      return {
        ...cachedJob,
        url: normalizeText(jobUrl),
        application_link: normalizeText(cachedJob.application_link || jobUrl),
      };
    }

    return {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      url: normalizeText(jobUrl),
      application_link: normalizeText(jobUrl),
      title: "",
      department: "",
      location: "",
      html_description: "",
      plain_text_description: "",
      company_name: club.name,
      company_url: club.company_url || club.source_url || "",
      company_logo_url: club.company_logo_url || "",
      source_url: club.source_url || "",
      ats: "softgarden",
    };
  }

  const $ = cheerio.load(html);
  const nextDataJob = extractNextDataJob($);
  const jobPosting = extractJobPostingJsonLd($);

  const title =
    normalizeText(
      (nextDataJob && nextDataJob.title) ||
        (jobPosting && (jobPosting.title || jobPosting.name)) ||
        (cachedJob && cachedJob.title) ||
        $("h1").first().text()
    ) || `job-${sourceId}`;

  const department = normalizeText(
    (nextDataJob && nextDataJob.category) ||
      (cachedJob && cachedJob.department) ||
      findValueByLabels($, ["department", "category", "team", "abteilung", "bereich"])
  );

  const employmentType = normalizeText(
    (nextDataJob && nextDataJob.employmentType) ||
      extractEmploymentTypeFromJsonLd(jobPosting) ||
      (cachedJob && cachedJob.employment_type) ||
      ""
  );

  const location = normalizeText(
    (nextDataJob && nextDataJob.geoLocationName) ||
      extractLocationFromJsonLd(jobPosting) ||
      (cachedJob && cachedJob.location) ||
      findValueByLabels($, ["location", "standort", "city"])
  );

  const htmlDescription = normalizeHtmlFragment(
    (nextDataJob && nextDataJob.description) ||
      (jobPosting && jobPosting.description) ||
      (cachedJob && cachedJob.html_description) ||
      ""
  );

  const plainTextDescription = htmlToStructuredPlainText(htmlDescription);

  const job = {
    club_id: club.club_id,
    source_id: sourceId,
    id: sourceId,
    title,
    department,
    location,
    location_type: resolveLocationType(nextDataJob && nextDataJob.remoteStatus),
    arrangement: mapArrangementFromEmploymentType(employmentType),
    employment_type: employmentType,
    url: normalizeText(jobUrl),
    application_link: resolveApplyLink(
      jobUrl,
      $,
      nextDataJob,
      cachedJob && cachedJob.application_link
    ),
    published_at: parseSoftgardenDate([
      nextDataJob && nextDataJob.date,
      nextDataJob && nextDataJob.published,
      nextDataJob &&
        nextDataJob.googleMetadata &&
        nextDataJob.googleMetadata.datePosted,
      jobPosting && (jobPosting.datePosted || jobPosting.dateCreated),
      cachedJob && cachedJob.published_at,
    ]),
    expires_at: parseSoftgardenDate([
      jobPosting && jobPosting.validThrough,
      cachedJob && cachedJob.expires_at,
    ]),
    html_description: htmlDescription,
    plain_text_description: plainTextDescription,
    company_name: club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url: club.company_logo_url || "",
    source_url: club.source_url || "",
    ats: "softgarden",
  };

  JOB_CACHE.set(cacheKey, job);
  return job;
}

module.exports = {
  discoverJobUrls,
  fetchJob,
};
