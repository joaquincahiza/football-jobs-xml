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

const DEFAULT_API_BASE =
  "https://postingpandaapi-live-prelive.azurewebsites.net/api";
const VACANCY_CACHE = new Map();

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

function extractSourceIdFromPostingPandaUrl(jobUrl) {
  try {
    const parsed = new URL(jobUrl);
    const byQuery = normalizeText(
      parsed.searchParams.get("job") ||
        parsed.searchParams.get("id") ||
        parsed.searchParams.get("advertid") ||
        ""
    );
    if (byQuery) {
      return byQuery;
    }

    const pathMatch = parsed.pathname.match(/\/job\/([^/?#]+)/i);
    if (pathMatch) {
      return normalizeText(pathMatch[1]);
    }
  } catch {
    // Ignore URL parsing failures.
  }

  const match = String(jobUrl || "").match(/\/job\/([^/?#]+)/i);
  return match ? normalizeText(match[1]) : "";
}

function buildApiUrl(club) {
  const base = normalizeText(club && club.postingpanda_api_base) || DEFAULT_API_BASE;
  const apiBase = base.replace(/\/+$/, "");
  const country = encodeURIComponent(normalizeText(club && club.country) || "UK");
  const distanceMiles = encodeURIComponent(
    normalizeText(club && club.distance_miles) || "10"
  );
  const searchTerm = encodeURIComponent(normalizeText(club && club.search_term) || "");
  const location = encodeURIComponent(normalizeText(club && club.location) || "");
  const extraDataFilters = encodeURIComponent(
    normalizeText(club && club.extra_data_filters) || ""
  );

  return `${apiBase}/liveadverts/filter/-in-?country=${country}&extraDataFilters=${extraDataFilters}&searchTerm=${searchTerm}&location=${location}&distanceMiles=${distanceMiles}`;
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

function readExtraDataValue(extraData, key) {
  const entries = Array.isArray(extraData) ? extraData : [];
  const expectedPrefix = `${normalizeText(key)}_`.toLowerCase();

  for (const entry of entries) {
    const text = normalizeText(entry);
    if (!text) {
      continue;
    }

    if (text.toLowerCase().startsWith(expectedPrefix)) {
      return text.slice(expectedPrefix.length);
    }
  }

  return "";
}

function resolveLocation(vacancy) {
  const parts = [vacancy.Address, vacancy.County, vacancy.Country]
    .map((value) => normalizeText(value))
    .filter(Boolean);

  return Array.from(new Set(parts)).join(", ");
}

function resolveEmploymentType(vacancy) {
  return [vacancy.EmploymentType, vacancy.Employment]
    .map((value) => normalizeText(value))
    .filter(Boolean)
    .join(" ");
}

function resolveApplyUrl(club, vacancy) {
  const applyUrl = normalizeText(vacancy.ApplyUrl || "");
  const urlTag = normalizeText(vacancy.UrlTag || "");

  if (!applyUrl && !urlTag) {
    return "";
  }

  if (applyUrl && /^https?:\/\//i.test(applyUrl)) {
    if (urlTag && !applyUrl.endsWith(`/${urlTag}`)) {
      return `${applyUrl.replace(/\/+$/, "")}/${urlTag}`;
    }

    return applyUrl;
  }

  if (applyUrl) {
    return canonicalizeUrl(club.source_url, applyUrl);
  }

  return canonicalizeUrl(club.source_url, urlTag);
}

function normalizeVacancy(club, vacancy) {
  const sourceId = normalizeText(
    vacancy.AdvertId || vacancy.advertId || vacancy.Id || vacancy.id || ""
  );
  if (!sourceId) {
    return null;
  }

  const employmentType = resolveEmploymentType(vacancy);
  const expiryFromExtraData = readExtraDataValue(vacancy.ExtraData, "Expiry");
  const htmlDescription = normalizeHtmlFragment(vacancy.JobDescription || "");
  const plainTextDescription =
    htmlToStructuredPlainText(htmlDescription) ||
    normalizeText(vacancy.JobDescription || "");
  const jobUrl = canonicalizeUrl(club.source_url, `/job/${sourceId}`);
  const applyUrl = resolveApplyUrl(club, vacancy);

  return {
    club_id: club.club_id,
    source_id: sourceId,
    id: sourceId,
    title: normalizeText(vacancy.JobTitle || ""),
    location: resolveLocation(vacancy),
    location_type: "onsite",
    department: normalizeText(vacancy.BusinessTypes || ""),
    arrangement: mapArrangementFromEmploymentType(employmentType),
    employment_type: employmentType,
    published_at: parseDateToIso(vacancy.DateCreated || vacancy.DateAdded || ""),
    expires_at: parseDateToIso(expiryFromExtraData || ""),
    html_description: htmlDescription,
    plain_text_description: plainTextDescription,
    url: jobUrl || normalizeText(club.source_url),
    application_link: applyUrl || jobUrl || normalizeText(club.source_url),
    company_name: club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url: club.company_logo_url || "",
    source_url: club.source_url || "",
    ats: "postingpanda",
    _meta: {
      job_reference: normalizeText(vacancy.JobReference || ""),
    },
  };
}

async function loadVacancies(club) {
  const apiUrl = buildApiUrl(club);
  const origin = (() => {
    try {
      return new URL(club.source_url).origin;
    } catch {
      return "";
    }
  })();
  const headers = {
    Accept: "application/json, text/plain, */*",
    Referer: club.source_url,
    "User-Agent":
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  };

  if (origin) {
    headers.Origin = origin;
  }

  const response = await requestJsonWithRetry(club, apiUrl, {
    method: "GET",
    headers,
  });

  return Array.isArray(response) ? response : [];
}

async function discoverJobUrls(club) {
  let vacancies;

  try {
    vacancies = await loadVacancies(club);
  } catch (error) {
    console.warn(
      `[warn] ${club.club_id}: postingpanda discover falló -> ${error.message}`
    );
    return [];
  }

  const urls = [];

  for (const vacancy of vacancies) {
    const normalized = normalizeVacancy(club, vacancy);
    if (!normalized || !normalized.source_id || !normalized.url) {
      continue;
    }

    VACANCY_CACHE.set(buildCacheKey(club, normalized.source_id), normalized);
    urls.push(normalized.url);
  }

  return Array.from(new Set(urls));
}

async function fetchJob(club, jobUrl) {
  const sourceId = extractSourceIdFromPostingPandaUrl(jobUrl);
  const key = buildCacheKey(club, sourceId);
  let cached = sourceId ? VACANCY_CACHE.get(key) : null;

  if (!cached) {
    const vacancies = await loadVacancies(club);
    for (const vacancy of vacancies) {
      const normalized = normalizeVacancy(club, vacancy);
      if (!normalized || !normalized.source_id) {
        continue;
      }

      VACANCY_CACHE.set(buildCacheKey(club, normalized.source_id), normalized);
    }

    cached = sourceId ? VACANCY_CACHE.get(key) : null;
  }

  if (!cached) {
    return {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      url: normalizeText(jobUrl),
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
      ats: "postingpanda",
    };
  }

  return {
    ...cached,
    url: cached.url || normalizeText(jobUrl),
  };
}

module.exports = {
  discoverJobUrls,
  fetchJob,
};
