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

const SITE_CONFIG_ENDPOINT =
  "https://api-careers-sites.talos360.com/api/careerssites/site/config/get";
const VACANCIES_SEARCH_ENDPOINT =
  "https://api-careers-sites.talos360.com/api/careerssite/vacancies/search";

const VACANCY_CACHE = new Map();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseUrlSafe(value) {
  try {
    return new URL(value);
  } catch {
    return null;
  }
}

function buildCacheKey(club, sourceId) {
  return `${normalizeText(club && club.club_id)}::${normalizeText(sourceId)}`;
}

function logHttp(club, message) {
  const clubId = normalizeText(club && club.club_id) || "unknown";
  console.log(`[http] ${clubId} ${message}`);
}

async function requestJsonWithRetry(club, url, options = {}) {
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
      const parsed = JSON.parse(text);
      await sleep(RATE_LIMIT_MS);
      return parsed;
    } catch (error) {
      lastError = error;
      logHttp(club, `ERROR ${url} -> ${error.message}`);
      if (attempt < RETRIES) {
        await sleep(RATE_LIMIT_MS);
      }
    }
  }

  throw lastError || new Error("error de request JSON");
}

function extractSourceIdFromTalosUrl(jobUrl) {
  try {
    const parsed = new URL(jobUrl);
    const byQuery = normalizeText(parsed.searchParams.get("jobPostId") || "");
    if (byQuery) {
      return byQuery;
    }

    const byPath = normalizeText(
      (parsed.pathname.match(/\/job\/(\d+)/i) || [])[1] || ""
    );
    if (byPath) {
      return byPath;
    }
  } catch {
    // Ignore URL parsing errors.
  }

  const match = String(jobUrl || "").match(/\/job\/(\d+)/i);
  return match ? normalizeText(match[1]) : "";
}

function extractMetadataValue(metadata, pattern) {
  const entries = Array.isArray(metadata) ? metadata : [];

  for (const entry of entries) {
    const key = normalizeText(entry && (entry.name || entry.key || "")).toLowerCase();
    if (!key || !pattern.test(key)) {
      continue;
    }

    const value = normalizeText(entry && entry.value);
    if (value) {
      return value;
    }
  }

  return "";
}

function resolveDepartment(vacancy) {
  return (
    extractMetadataValue(vacancy.metadata, /department|team|function|category/) ||
    normalizeText(vacancy.businessType || vacancy.businessTypes || "")
  );
}

function resolveLocation(vacancy) {
  const metadataLocation = extractMetadataValue(vacancy.metadata, /location|site/);
  if (metadataLocation) {
    return metadataLocation;
  }

  const parts = [vacancy.address, vacancy.city, vacancy.county, vacancy.country]
    .map((value) => normalizeText(value))
    .filter(Boolean);

  return Array.from(new Set(parts)).join(", ");
}

function resolveLocationType(vacancy) {
  const value = vacancy.remoteWork;

  if (value === true || value === 1) {
    return "remote";
  }

  if (value === false || value === 0) {
    return "onsite";
  }

  const text = normalizeText(value).toLowerCase();
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

function resolveLearnMoreUrl(club, vacancy) {
  const sourceId = normalizeText(vacancy.jobPostId);
  if (!sourceId) {
    return "";
  }

  return canonicalizeUrl(club.source_url, `/job/${sourceId}`);
}

function resolveApplyUrl(club, vacancy) {
  const route = normalizeText(vacancy.applyUrlRoute || vacancy.applyUrl || "");
  const base = normalizeText(vacancy.applyUrlBase || "");

  if (/^https?:\/\//i.test(route)) {
    return route;
  }

  if (base && route) {
    return canonicalizeUrl(base, route);
  }

  if (route) {
    return canonicalizeUrl(club.source_url, route);
  }

  if (base) {
    return base;
  }

  return "";
}

function normalizeTalosVacancy(club, vacancy) {
  const sourceId = normalizeText(vacancy.jobPostId);
  if (!sourceId) {
    return null;
  }

  const htmlDescription = normalizeHtmlFragment(
    vacancy.jobDescription || vacancy.openingJobDescriptionParagraph || ""
  );

  const learnMoreUrl = resolveLearnMoreUrl(club, vacancy);
  const applyUrl = resolveApplyUrl(club, vacancy);

  return {
    club_id: club.club_id,
    source_id: sourceId,
    id: sourceId,
    title: normalizeText(vacancy.jobTitle || vacancy.title || ""),
    department: resolveDepartment(vacancy),
    location: resolveLocation(vacancy),
    location_type: resolveLocationType(vacancy),
    arrangement: mapArrangementFromEmploymentType(
      normalizeText(vacancy.employment || vacancy.employmentType || "")
    ),
    employment_type: normalizeText(vacancy.employment || vacancy.employmentType || ""),
    html_description: htmlDescription,
    plain_text_description: htmlToStructuredPlainText(htmlDescription),
    published_at: parseDateToIso(vacancy.dateCreated || vacancy.publishedAt || ""),
    expires_at: parseDateToIso(vacancy.expiryDate || ""),
    url: learnMoreUrl,
    application_link: applyUrl,
    company_name: club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url: club.company_logo_url || "",
    source_url: club.source_url || "",
    ats: "talos",
    _meta: {
      job_reference: normalizeText(vacancy.jobReference || ""),
    },
  };
}

async function loadTalosVacancies(club) {
  const parsed = parseUrlSafe(club && club.source_url);
  if (!parsed || !parsed.hostname) {
    throw new Error("source_url inválida para Talos");
  }

  const configUrl = `${SITE_CONFIG_ENDPOINT}?host=${encodeURIComponent(
    parsed.hostname
  )}`;
  const config = await requestJsonWithRetry(club, configUrl, {
    method: "GET",
    headers: {
      Accept: "application/json, text/plain, */*",
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    },
  });

  const obfuscatedId = normalizeText(config?.siteConfig?.obfuscatedId || "");
  const siteType = normalizeText(config?.siteConfig?.siteType || "External") || "External";

  if (!obfuscatedId) {
    throw new Error("Talos obfuscatedId no disponible");
  }

  const payload = {
    careersSiteObfuscatedId: obfuscatedId,
    whereCriteria: null,
    metadataFilters: [],
    preFilters: [],
    siteType,
  };

  const result = await requestJsonWithRetry(club, VACANCIES_SEARCH_ENDPOINT, {
    method: "POST",
    headers: {
      Accept: "application/json, text/plain, */*",
      "Content-Type": "application/json",
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      Referer: club.source_url,
    },
    body: JSON.stringify(payload),
  });

  return Array.isArray(result && result.careersSiteVacancies)
    ? result.careersSiteVacancies
    : [];
}

async function discoverJobUrls(club) {
  let vacancies;

  try {
    vacancies = await loadTalosVacancies(club);
  } catch (error) {
    console.warn(`[warn] ${club.club_id}: talos discover falló -> ${error.message}`);
    return [];
  }

  const urls = [];

  for (const vacancy of vacancies) {
    const job = normalizeTalosVacancy(club, vacancy);
    if (!job || !job.source_id || !job.url) {
      continue;
    }

    VACANCY_CACHE.set(buildCacheKey(club, job.source_id), job);
    urls.push(job.url);
  }

  return Array.from(new Set(urls));
}

async function fetchJob(club, jobUrl) {
  const sourceId = extractSourceIdFromTalosUrl(jobUrl);
  if (!sourceId) {
    throw new Error(`No se pudo extraer source_id desde ${jobUrl}`);
  }

  const key = buildCacheKey(club, sourceId);
  let job = VACANCY_CACHE.get(key);

  if (!job) {
    const vacancies = await loadTalosVacancies(club);
    for (const vacancy of vacancies) {
      const normalized = normalizeTalosVacancy(club, vacancy);
      if (!normalized || !normalized.source_id) {
        continue;
      }

      VACANCY_CACHE.set(buildCacheKey(club, normalized.source_id), normalized);
    }

    job = VACANCY_CACHE.get(key);
  }

  if (!job) {
    return {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      url: canonicalizeUrl(club.source_url, `/job/${sourceId}`),
      application_link: "",
      title: "",
      location: "",
      department: "",
      html_description: "",
      plain_text_description: "",
      company_name: club.name,
      company_url: club.company_url || club.source_url || "",
      company_logo_url: club.company_logo_url || "",
      source_url: club.source_url || "",
      ats: "talos",
    };
  }

  return {
    ...job,
    url: job.url || canonicalizeUrl(club.source_url, `/job/${sourceId}`),
  };
}

module.exports = {
  discoverJobUrls,
  fetchJob,
};
