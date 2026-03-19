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
  extractJobPostingJsonLd,
  extractIdentifierFromJsonLd,
  extractLocationFromJsonLd,
  extractEmploymentTypeFromJsonLd,
  selectDescriptionHtml,
  escapeHtml,
} = require("./utils");

const CAREER_ENDPOINT_PATH = "/api/careez";
const APPLY_HOST = "https://taleez.com";
const JOB_CACHE = new Map();
const URL_TO_SOURCE_ID = new Map();
const TOKEN_TO_SOURCE_ID = new Map();

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

function buildUrlKey(club, url) {
  return `${normalizeText(club && club.club_id)}::${normalizeText(url)}`;
}

function buildTokenKey(club, token) {
  return `${normalizeText(club && club.club_id)}::${normalizeText(token)}`;
}

function parseUrlSafe(value) {
  try {
    return new URL(value);
  } catch {
    return null;
  }
}

function buildCareerApiUrl(club) {
  const parsed = parseUrlSafe(club && club.source_url);
  if (!parsed) {
    return "";
  }

  const endpoint = new URL(CAREER_ENDPOINT_PATH, parsed.origin);
  return endpoint.href;
}

function extractTokenFromApplyUrl(value) {
  const normalized = normalizeText(value);
  if (!normalized) {
    return "";
  }

  try {
    const parsed = new URL(normalized);
    const segments = parsed.pathname.split("/").filter(Boolean);
    const applyIndex = segments.findIndex(
      (segment) => String(segment || "").toLowerCase() === "apply"
    );

    if (applyIndex >= 0 && segments[applyIndex + 1]) {
      return normalizeText(decodeURIComponent(segments[applyIndex + 1]));
    }
  } catch {
    // Ignore URL parsing failures.
  }

  const match = normalized.match(/\/apply\/([^/?#]+)/i);
  return match ? normalizeText(decodeURIComponent(match[1])) : "";
}

function extractSourceIdFromTaleezUrl(club, jobUrl) {
  const normalizedUrl = normalizeText(jobUrl);
  if (!normalizedUrl) {
    return "";
  }

  const fromUrlCache = normalizeText(
    URL_TO_SOURCE_ID.get(buildUrlKey(club, normalizedUrl)) || ""
  );
  if (fromUrlCache) {
    return fromUrlCache;
  }

  const token = extractTokenFromApplyUrl(normalizedUrl);
  if (token) {
    const fromTokenCache = normalizeText(
      TOKEN_TO_SOURCE_ID.get(buildTokenKey(club, token)) || ""
    );
    if (fromTokenCache) {
      return fromTokenCache;
    }
  }

  return "";
}

function toIsoFromEpoch(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "";
  }

  const date = new Date(numeric);
  if (Number.isNaN(date.getTime())) {
    return "";
  }

  return date.toISOString();
}

function resolveDepartment(job, properties) {
  const jobProperties = Array.isArray(job && job.properties) ? job.properties : [];
  const allProperties = Array.isArray(properties) ? properties : [];

  const departmentProperty = allProperties.find((property) => {
    const lockedType = normalizeText(property && property.lockedType).toLowerCase();
    const label = normalizeText(
      (property && (property.internalName || property.publicName || property.label)) || ""
    ).toLowerCase();
    return lockedType === "department" || label.includes("depart");
  });

  if (!departmentProperty) {
    return "";
  }

  const selected = jobProperties.find(
    (item) => Number(item && item.id) === Number(departmentProperty.id)
  );
  const choiceId = Number(
    Array.isArray(selected && selected.choices) ? selected.choices[0] : 0
  );
  if (!choiceId) {
    return "";
  }

  const choice = (departmentProperty.choices || []).find(
    (entry) => Number(entry && entry.id) === choiceId
  );

  return normalizeText(
    (choice && (choice.value || choice.label || choice.name)) || ""
  );
}

function resolveLocation(job) {
  const parts = [job && job.city, job && job.cityDept, job && job.country]
    .map((value) => normalizeText(value))
    .filter(Boolean);

  return Array.from(new Set(parts)).join(", ");
}

function buildApplyUrl(token) {
  const normalizedToken = normalizeText(token);
  if (!normalizedToken) {
    return "";
  }

  return `${APPLY_HOST}/apply/${encodeURIComponent(normalizedToken)}`;
}

function mapTaleezArrangement(employmentType) {
  const normalized = normalizeText(employmentType).toLowerCase();

  if (
    normalized.includes("part-time") ||
    normalized.includes("part time") ||
    normalized.includes("parttime")
  ) {
    return "parttime";
  }

  if (
    normalized.includes("cdd") ||
    normalized.includes("stage") ||
    normalized.includes("intern") ||
    normalized.includes("alternance") ||
    normalized.includes("temp") ||
    normalized.includes("interim") ||
    normalized.includes("freelance") ||
    normalized.includes("contract")
  ) {
    return "contract";
  }

  return mapArrangementFromEmploymentType(employmentType);
}

function normalizeCareerJob(club, payload, job) {
  const sourceId = normalizeText(job && job.id);
  if (!sourceId) {
    return null;
  }

  const token = normalizeText((job && (job.token || job.slug)) || "");
  const applyUrl = buildApplyUrl(token);
  const fallbackUrl =
    canonicalizeUrl(club.source_url, `/jobs/${sourceId}`) || normalizeText(club.source_url);
  const url = applyUrl || fallbackUrl;
  const employmentType = normalizeText(job && job.contract);

  return {
    club_id: club.club_id,
    source_id: sourceId,
    id: sourceId,
    title: normalizeText(job && job.label),
    department: resolveDepartment(job, payload && payload.properties),
    location: resolveLocation(job),
    location_type: Boolean(job && job.remote) ? "remote" : "onsite",
    arrangement: mapTaleezArrangement(employmentType),
    employment_type: employmentType,
    html_description: "",
    plain_text_description: "",
    published_at:
      toIsoFromEpoch(job && job.publishDate) ||
      toIsoFromEpoch(job && job.creationDate) ||
      "",
    expires_at: "",
    url,
    application_link: url,
    company_name: club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url: club.company_logo_url || "",
    source_url: club.source_url || "",
    ats: "taleez",
    _meta: {
      token,
      unit_id: normalizeText(job && job.unitId),
    },
  };
}

function extractDetailPayload($) {
  const raw = String($("#ng-state").first().text() || "").trim();
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw);

    if (parsed && typeof parsed === "object") {
      if (parsed.DATA_QUESTIONS && typeof parsed.DATA_QUESTIONS === "object") {
        return parsed.DATA_QUESTIONS;
      }

      for (const value of Object.values(parsed)) {
        if (!value || typeof value !== "object") {
          continue;
        }

        if (value.b && typeof value.b === "object") {
          return value.b;
        }

        if (value.id && (value.jobDesc || value.profileDesc || value.label)) {
          return value;
        }
      }
    }
  } catch {
    // Ignore parse failures for ng-state.
  }

  return null;
}

function buildDescriptionHtmlFromDetail(detailPayload, jobPosting, $) {
  const sections = [];

  const jobDesc = normalizeHtmlFragment(detailPayload && detailPayload.jobDesc);
  if (jobDesc) {
    sections.push(jobDesc);
  }

  const profileDesc = normalizeHtmlFragment(detailPayload && detailPayload.profileDesc);
  if (profileDesc) {
    sections.push(profileDesc);
  }

  if (!sections.length) {
    const fromJobPosting = normalizeHtmlFragment(jobPosting && jobPosting.description);
    if (fromJobPosting) {
      sections.push(fromJobPosting);
    }
  }

  if (!sections.length) {
    const selected = normalizeHtmlFragment(selectDescriptionHtml($));
    if (selected) {
      sections.push(selected);
    }
  }

  const html = sections.join("\n").trim();
  if (!html) {
    return "";
  }

  const hasHtmlTag = /<([a-z][a-z0-9]*)\b[^>]*>/i.test(html);
  if (hasHtmlTag) {
    return html;
  }

  return `<p>${escapeHtml(html)}</p>`;
}

function resolveLocationType(detailPayload, fallbackValue) {
  if (detailPayload && detailPayload.remote === true) {
    return "remote";
  }

  if (detailPayload && detailPayload.remote === false) {
    return "onsite";
  }

  const normalized = normalizeText(fallbackValue).toLowerCase();
  if (normalized.includes("hybrid")) {
    return "hybrid";
  }

  if (normalized.includes("remote")) {
    return "remote";
  }

  return "onsite";
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

async function loadCareerPayload(club) {
  const apiUrl = buildCareerApiUrl(club);
  if (!apiUrl) {
    throw new Error("source_url inválida para Taleez");
  }

  return requestJsonWithRetry(club, apiUrl, {
    method: "GET",
    headers: {
      Accept: "application/json, text/plain, */*",
      Referer: club.source_url,
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    },
  });
}

async function discoverJobUrls(club) {
  let payload;

  try {
    payload = await loadCareerPayload(club);
  } catch (error) {
    console.warn(`[warn] ${club.club_id}: taleez discover falló -> ${error.message}`);
    return [];
  }

  const jobs = Array.isArray(payload && payload.jobs) ? payload.jobs : [];
  const urls = [];

  for (const entry of jobs) {
    const normalized = normalizeCareerJob(club, payload, entry);
    if (!normalized || !normalized.source_id || !normalized.url) {
      continue;
    }

    JOB_CACHE.set(buildCacheKey(club, normalized.source_id), normalized);
    URL_TO_SOURCE_ID.set(
      buildUrlKey(club, normalized.url),
      normalizeText(normalized.source_id)
    );

    const token = normalizeText(normalized._meta && normalized._meta.token);
    if (token) {
      TOKEN_TO_SOURCE_ID.set(buildTokenKey(club, token), normalized.source_id);
    }

    urls.push(normalized.url);
  }

  return Array.from(new Set(urls));
}

async function fetchJob(club, jobUrl) {
  const normalizedUrl = normalizeText(jobUrl);
  let sourceId = extractSourceIdFromTaleezUrl(club, normalizedUrl);
  let cached = sourceId ? JOB_CACHE.get(buildCacheKey(club, sourceId)) : null;

  if (!cached) {
    const payload = await loadCareerPayload(club);
    const jobs = Array.isArray(payload && payload.jobs) ? payload.jobs : [];

    for (const entry of jobs) {
      const normalized = normalizeCareerJob(club, payload, entry);
      if (!normalized || !normalized.source_id) {
        continue;
      }

      JOB_CACHE.set(buildCacheKey(club, normalized.source_id), normalized);
      URL_TO_SOURCE_ID.set(
        buildUrlKey(club, normalized.url),
        normalizeText(normalized.source_id)
      );

      const token = normalizeText(normalized._meta && normalized._meta.token);
      if (token) {
        TOKEN_TO_SOURCE_ID.set(buildTokenKey(club, token), normalized.source_id);
      }
    }

    sourceId = sourceId || extractSourceIdFromTaleezUrl(club, normalizedUrl);
    cached = sourceId ? JOB_CACHE.get(buildCacheKey(club, sourceId)) : null;
  }

  const detailUrl = normalizedUrl || normalizeText(cached && cached.url);
  if (!detailUrl) {
    throw new Error("No se pudo resolver URL de detalle Taleez");
  }

  const html = await requestTextWithRetry(club, detailUrl, {
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
  const detailPayload = extractDetailPayload($);
  const token = extractTokenFromApplyUrl(detailUrl);
  const detailSourceId = normalizeText(
    (detailPayload && detailPayload.id) ||
      sourceId ||
      extractIdentifierFromJsonLd(jobPosting) ||
      ""
  );

  if (detailSourceId) {
    URL_TO_SOURCE_ID.set(buildUrlKey(club, detailUrl), detailSourceId);
    if (token) {
      TOKEN_TO_SOURCE_ID.set(buildTokenKey(club, token), detailSourceId);
    }
  }

  const departmentFromDetail = (() => {
    const properties = Array.isArray(detailPayload && detailPayload.properties)
      ? detailPayload.properties
      : [];
    const candidate = properties.find((item) =>
      normalizeText(item && item.label).toLowerCase().includes("depart")
    );
    if (!candidate) {
      return "";
    }

    const values = Array.isArray(candidate.values) ? candidate.values : [];
    return normalizeText(values.join(", "));
  })();

  const locationFromDetail = [
    normalizeText(detailPayload && detailPayload.city),
    normalizeText(detailPayload && detailPayload.countryLabel),
  ]
    .filter(Boolean)
    .join(", ");

  const employmentType = normalizeText(
    (detailPayload && detailPayload.contract) ||
      extractEmploymentTypeFromJsonLd(jobPosting) ||
      normalizeText(cached && cached.employment_type) ||
      ""
  );

  const htmlDescription = buildDescriptionHtmlFromDetail(detailPayload, jobPosting, $);
  const plainTextDescription = htmlToStructuredPlainText(htmlDescription);

  let applicationLink = "";
  $("a[href]").each((_, node) => {
    if (applicationLink) {
      return;
    }

    const href = normalizeText($(node).attr("href"));
    const absoluteUrl = canonicalizeUrl(detailUrl, href) || href;
    const text = normalizeText($(node).text()).toLowerCase();

    if (/\/apply\/[^/?#]+/i.test(absoluteUrl) || text.includes("postuler") || text.includes("apply")) {
      applicationLink = absoluteUrl;
    }
  });

  if (!applicationLink) {
    applicationLink = detailUrl;
  }

  const normalized = {
    club_id: club.club_id,
    source_id: detailSourceId,
    id: detailSourceId,
    title:
      normalizeText(detailPayload && detailPayload.label) ||
      normalizeText(jobPosting && (jobPosting.title || jobPosting.name)) ||
      normalizeText(cached && cached.title),
    department:
      departmentFromDetail ||
      normalizeText(cached && cached.department) ||
      "",
    location:
      locationFromDetail ||
      extractLocationFromJsonLd(jobPosting) ||
      normalizeText(cached && cached.location) ||
      "",
    location_type: resolveLocationType(
      detailPayload,
      normalizeText(cached && cached.location_type)
    ),
    arrangement: mapTaleezArrangement(employmentType),
    employment_type: employmentType,
    html_description: htmlDescription,
    plain_text_description:
      plainTextDescription ||
      normalizeText(jobPosting && jobPosting.description) ||
      "",
    published_at: parseDateToIso(
      (detailPayload && detailPayload.datePosted) ||
        (jobPosting && (jobPosting.datePosted || jobPosting.dateCreated)) ||
        normalizeText(cached && cached.published_at)
    ),
    expires_at: parseDateToIso(
      (jobPosting && jobPosting.validThrough) ||
        normalizeText(cached && cached.expires_at)
    ),
    url: detailUrl,
    application_link: applicationLink,
    company_name: club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url: club.company_logo_url || "",
    source_url: club.source_url || "",
    ats: "taleez",
    _meta: {
      token: token || normalizeText(cached && cached._meta && cached._meta.token),
    },
  };

  if (detailSourceId) {
    JOB_CACHE.set(buildCacheKey(club, detailSourceId), normalized);
  }

  return normalized;
}

module.exports = {
  discoverJobUrls,
  fetchJob,
};
