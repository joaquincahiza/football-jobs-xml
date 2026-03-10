const {
  RATE_LIMIT_MS,
  RETRIES,
  normalizeText,
  parseDateToIso,
  htmlToStructuredPlainText,
  normalizeHtmlFragment,
  canonicalizeUrl,
  mapArrangementFromEmploymentType,
  escapeHtml,
} = require("./utils");

const JOB_ADS_PATH = "/api/job-ad";
const JOB_CACHE = new Map();

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

function resolveCompanyIdentifier(club) {
  const configured = normalizeText(club && club.hibob_company_identifier);
  if (configured) {
    return configured;
  }

  const parsed = parseUrlSafe(club && club.source_url);
  if (!parsed) {
    return "";
  }

  const parts = String(parsed.hostname || "").split(".").filter(Boolean);
  return normalizeText(parts[0] || "");
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

function toHtmlFragment(value) {
  const normalized = normalizeHtmlFragment(value || "");
  if (!normalized) {
    return "";
  }

  if (/<[a-z][\s\S]*>/i.test(normalized)) {
    return normalized;
  }

  return `<p>${escapeHtml(normalized)}</p>`;
}

function buildDescriptionHtml(jobAd) {
  const sections = [];

  const description = toHtmlFragment(jobAd && jobAd.description);
  if (description) {
    sections.push(description);
  }

  const responsibilities = toHtmlFragment(jobAd && jobAd.responsibilities);
  if (responsibilities) {
    sections.push(`<h3>Responsibilities</h3>${responsibilities}`);
  }

  const requirements = toHtmlFragment(jobAd && jobAd.requirements);
  if (requirements) {
    sections.push(`<h3>Requirements</h3>${requirements}`);
  }

  const benefits = toHtmlFragment(jobAd && jobAd.benefits);
  if (benefits) {
    sections.push(`<h3>Benefits</h3>${benefits}`);
  }

  return sections.join("\n").trim();
}

function resolveLocation(jobAd) {
  const parts = [jobAd && jobAd.site, jobAd && jobAd.country]
    .map((value) => normalizeText(value))
    .filter(Boolean);

  return Array.from(new Set(parts)).join(", ");
}

function resolveLocationType(jobAd) {
  const text = normalizeText(
    (jobAd && (jobAd.workspaceType || jobAd.workplaceType)) || ""
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

function extractSourceIdFromJobUrl(jobUrl) {
  try {
    const parsed = new URL(jobUrl);
    const match = parsed.pathname.match(/\/jobs\/([0-9a-f-]{36})/i);
    if (match) {
      return normalizeText(match[1].toLowerCase());
    }
  } catch {
    // Ignore URL parsing errors.
  }

  const match = String(jobUrl || "").match(/\/jobs\/([0-9a-f-]{36})/i);
  return match ? normalizeText(match[1].toLowerCase()) : "";
}

function normalizeJobAd(club, jobAd) {
  const sourceId = normalizeText(jobAd && jobAd.id).toLowerCase();
  if (!sourceId) {
    return null;
  }

  const learnMoreUrl = canonicalizeUrl(club.source_url, `/jobs/${sourceId}`);
  const htmlDescription = buildDescriptionHtml(jobAd);

  return {
    club_id: club.club_id,
    source_id: sourceId,
    id: sourceId,
    title: normalizeText(jobAd && jobAd.title),
    department: normalizeText(jobAd && jobAd.department),
    location: resolveLocation(jobAd),
    location_type: resolveLocationType(jobAd),
    arrangement: mapArrangementFromEmploymentType(
      normalizeText(jobAd && jobAd.employmentType)
    ),
    employment_type: normalizeText(jobAd && jobAd.employmentType),
    html_description: htmlDescription,
    plain_text_description: htmlToStructuredPlainText(htmlDescription),
    published_at: parseDateToIso(jobAd && jobAd.publishedAt),
    expires_at: parseDateToIso(jobAd && jobAd.expiryDate),
    url: learnMoreUrl,
    application_link: learnMoreUrl,
    company_name: club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url: club.company_logo_url || "",
    source_url: club.source_url || "",
    ats: "hibob",
  };
}

async function loadJobAds(club) {
  const companyIdentifier = resolveCompanyIdentifier(club);
  if (!companyIdentifier) {
    throw new Error("No se pudo resolver companyidentifier de HiBob");
  }

  const endpoint = canonicalizeUrl(club.source_url, JOB_ADS_PATH);
  if (!endpoint) {
    throw new Error("No se pudo resolver endpoint /api/job-ad");
  }

  const payload = await requestJsonWithRetry(club, endpoint, {
    method: "GET",
    headers: {
      Accept: "application/json, text/plain, */*",
      companyidentifier: companyIdentifier,
      Referer: club.source_url,
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    },
  });

  if (Array.isArray(payload && payload.jobAdDetails)) {
    return payload.jobAdDetails;
  }

  if (Array.isArray(payload && payload.jobAds)) {
    return payload.jobAds;
  }

  return [];
}

async function discoverJobUrls(club) {
  let jobAds;

  try {
    jobAds = await loadJobAds(club);
  } catch (error) {
    console.warn(`[warn] ${club.club_id}: hibob discover falló -> ${error.message}`);
    return [];
  }

  const urls = [];

  for (const jobAd of jobAds) {
    const job = normalizeJobAd(club, jobAd);
    if (!job || !job.source_id || !job.url) {
      continue;
    }

    JOB_CACHE.set(buildCacheKey(club, job.source_id), job);
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
  let job = JOB_CACHE.get(key);

  if (!job) {
    const jobAds = await loadJobAds(club);
    for (const jobAd of jobAds) {
      const normalized = normalizeJobAd(club, jobAd);
      if (!normalized || !normalized.source_id) {
        continue;
      }

      JOB_CACHE.set(buildCacheKey(club, normalized.source_id), normalized);
    }

    job = JOB_CACHE.get(key);
  }

  if (!job) {
    return {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      url: canonicalizeUrl(club.source_url, `/jobs/${sourceId}`),
      application_link: canonicalizeUrl(club.source_url, `/jobs/${sourceId}`),
      title: "",
      location: "",
      department: "",
      html_description: "",
      plain_text_description: "",
      company_name: club.name,
      company_url: club.company_url || club.source_url || "",
      company_logo_url: club.company_logo_url || "",
      source_url: club.source_url || "",
      ats: "hibob",
    };
  }

  return job;
}

module.exports = {
  discoverJobUrls,
  fetchJob,
};
