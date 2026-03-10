const cheerio = require("cheerio");
const {
  RATE_LIMIT_MS,
  RETRIES,
  normalizeText,
  parseDateToIso,
  htmlToStructuredPlainText,
  normalizeHtmlFragment,
  canonicalizeUrl,
  mapArrangementFromEmploymentType,
} = require("./utils");

const JOBS_CONFIG_SELECTOR =
  'script.js-react-on-rails-component[data-component-name="External::Jobs"]';
const UUID_REGEX =
  /([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})/i;

const POSTING_CACHE = new Map();

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

async function requestJsonWithRetry(club, url, options = {}) {
  const text = await requestTextWithRetry(club, url, options);
  return JSON.parse(text);
}

function extractJobsConfig(html) {
  const $ = cheerio.load(String(html || ""));
  const script = $(JOBS_CONFIG_SELECTOR).first();

  if (!script.length) {
    return null;
  }

  const raw = normalizeText(script.text());
  if (!raw) {
    return null;
  }

  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function buildPostingsUrl(club, jobsConfig) {
  const fallback = canonicalizeUrl(club.source_url, "/postings.json");
  const configUrl = normalizeText(jobsConfig && jobsConfig.url);

  const url = configUrl ? canonicalizeUrl(club.source_url, configUrl) : fallback;
  if (!url) {
    return "";
  }

  let parsed;
  try {
    parsed = new URL(url);
  } catch {
    return url;
  }

  const target = normalizeText(jobsConfig && jobsConfig.target);
  if (target && !parsed.searchParams.has("target")) {
    parsed.searchParams.set("target", target);
  }

  return parsed.href;
}

function extractUuid(value) {
  const match = normalizeText(value).match(UUID_REGEX);
  return match ? match[1].toLowerCase() : "";
}

function extractSourceIdFromJobUrl(jobUrl) {
  try {
    const parsed = new URL(jobUrl);
    const pathId = extractUuid(parsed.pathname);
    if (pathId) {
      return pathId;
    }

    const queryId = extractUuid(parsed.searchParams.get("id") || "");
    if (queryId) {
      return queryId;
    }
  } catch {
    // Ignore parsing failures.
  }

  return extractUuid(jobUrl);
}

function resolveDepartment(item) {
  const department = item && item.job && item.job.department;
  if (department && typeof department === "object") {
    return normalizeText(department.name || department.value || "");
  }

  return normalizeText(
    (item && item.department) || (item && item.department_name) || ""
  );
}

function resolveLocation(item) {
  const location = item && item.location;

  if (location && typeof location === "object") {
    const parts = [location.name, location.city, location.province, location.country]
      .map((value) => normalizeText(value))
      .filter(Boolean);

    if (parts.length) {
      return Array.from(new Set(parts)).join(", ");
    }
  }

  return normalizeText((item && item.location_name) || "");
}

function resolveLocationType(item) {
  const text = normalizeText(
    (item && (item.workplace_type_text || item.workplace_type)) || ""
  ).toLowerCase();

  if (!text) {
    return "onsite";
  }

  if (text.includes("hybrid")) {
    return "hybrid";
  }

  if (text.includes("remote")) {
    return "remote";
  }

  return "onsite";
}

function buildDescriptionHtml(item) {
  const sections = [
    normalizeHtmlFragment(item && item.description),
    normalizeHtmlFragment(item && item.key_responsibilities),
    normalizeHtmlFragment(item && item.skills_knowledge_expertise),
    normalizeHtmlFragment(item && item.benefits),
  ].filter(Boolean);

  return sections.join("\n").trim();
}

function resolveLearnMoreUrl(club, item) {
  const path = normalizeText(item && item.path);
  if (path) {
    return canonicalizeUrl(club.source_url, path);
  }

  const rawUrl = normalizeText(item && item.url);
  if (rawUrl) {
    const maybePath = rawUrl.match(/\/en\/postings\/[0-9a-f-]{36}/i);
    if (maybePath) {
      return canonicalizeUrl(club.source_url, maybePath[0]);
    }

    return canonicalizeUrl(club.source_url, rawUrl) || rawUrl;
  }

  return "";
}

function resolveApplyUrl(club, item, learnMoreUrl) {
  const direct = normalizeText(
    (item && (item.apply_url || item.application_url || item.applyUrl)) || ""
  );

  if (direct) {
    return canonicalizeUrl(club.source_url, direct) || direct;
  }

  const path = normalizeText(item && item.path);
  if (path) {
    return canonicalizeUrl(club.source_url, `${path.replace(/\/+$/, "")}/applications/new`);
  }

  if (learnMoreUrl) {
    return `${learnMoreUrl.replace(/\/+$/, "")}/applications/new`;
  }

  return "";
}

function normalizePosting(club, item) {
  const sourceId =
    extractUuid(item && item.path) ||
    extractUuid(item && item.url) ||
    extractUuid(item && item.id) ||
    normalizeText(item && item.id);

  if (!sourceId) {
    return null;
  }

  const learnMoreUrl = resolveLearnMoreUrl(club, item);
  if (!learnMoreUrl) {
    return null;
  }

  const htmlDescription = buildDescriptionHtml(item);

  return {
    club_id: club.club_id,
    source_id: sourceId,
    id: sourceId,
    title: normalizeText(item && item.title),
    department: resolveDepartment(item),
    location: resolveLocation(item),
    location_type: resolveLocationType(item),
    arrangement: mapArrangementFromEmploymentType(
      normalizeText(
        (item && (item.employment_type_text || item.employment_type)) || ""
      )
    ),
    employment_type: normalizeText(
      (item && (item.employment_type_text || item.employment_type)) || ""
    ),
    html_description: htmlDescription,
    plain_text_description: htmlToStructuredPlainText(htmlDescription),
    published_at: parseDateToIso(
      (item &&
        (item.published_at || item.created_at || item.updated_at || item.opened_at)) ||
        ""
    ),
    expires_at: parseDateToIso((item && item.deadline_at) || ""),
    url: learnMoreUrl,
    application_link: resolveApplyUrl(club, item, learnMoreUrl),
    company_name: club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url: club.company_logo_url || "",
    source_url: club.source_url || "",
    ats: "pinpoint",
    _meta: {
      requisition_id: normalizeText(item && item.job && item.job.requisition_id),
      division: normalizeText(item && item.job && item.job.division),
    },
  };
}

async function loadPostings(club) {
  const listingHtml = await requestTextWithRetry(club, club.source_url, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    },
  });

  const jobsConfig = extractJobsConfig(listingHtml);
  const postingsUrl = buildPostingsUrl(club, jobsConfig);
  if (!postingsUrl) {
    throw new Error("No se pudo resolver postings.json para Pinpoint");
  }

  const payload = await requestJsonWithRetry(club, postingsUrl, {
    method: "GET",
    headers: {
      Accept: "application/json, text/plain, */*",
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      Referer: club.source_url,
    },
  });

  return Array.isArray(payload && payload.data) ? payload.data : [];
}

async function discoverJobUrls(club) {
  let postings;

  try {
    postings = await loadPostings(club);
  } catch (error) {
    console.warn(`[warn] ${club.club_id}: pinpoint discover falló -> ${error.message}`);
    return [];
  }

  const urls = [];

  for (const item of postings) {
    const job = normalizePosting(club, item);
    if (!job || !job.source_id || !job.url) {
      continue;
    }

    POSTING_CACHE.set(buildCacheKey(club, job.source_id), job);
    urls.push(job.url);
  }

  return Array.from(new Set(urls));
}

async function fetchJob(club, jobUrl) {
  const sourceId = extractSourceIdFromJobUrl(jobUrl);
  if (!sourceId) {
    throw new Error(`No se pudo extraer source_id desde ${jobUrl}`);
  }

  const key = buildCacheKey(club, sourceId);
  let job = POSTING_CACHE.get(key);

  if (!job) {
    const postings = await loadPostings(club);
    for (const item of postings) {
      const normalized = normalizePosting(club, item);
      if (!normalized || !normalized.source_id) {
        continue;
      }

      POSTING_CACHE.set(buildCacheKey(club, normalized.source_id), normalized);
    }

    job = POSTING_CACHE.get(key);
  }

  if (!job) {
    return {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      url: jobUrl,
      application_link: `${jobUrl.replace(/\/+$/, "")}/applications/new`,
      title: "",
      location: "",
      department: "",
      html_description: "",
      plain_text_description: "",
      company_name: club.name,
      company_url: club.company_url || club.source_url || "",
      company_logo_url: club.company_logo_url || "",
      source_url: club.source_url || "",
      ats: "pinpoint",
    };
  }

  return {
    ...job,
    url: job.url || jobUrl,
  };
}

module.exports = {
  discoverJobUrls,
  fetchJob,
};
