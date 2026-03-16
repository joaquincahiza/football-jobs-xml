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
  extractLocationFromJsonLd,
  extractEmploymentTypeFromJsonLd,
  findValueByLabels,
  selectDescriptionHtml,
} = require("./utils");

const JOB_CACHE = new Map();

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

function extractSourceIdFromPaylocityUrl(jobUrl) {
  try {
    const parsed = new URL(jobUrl);
    const pathMatch = parsed.pathname.match(/\/jobs\/(?:details|apply)\/(\d+)/i);
    if (pathMatch) {
      return normalizeText(pathMatch[1]);
    }

    const queryMatch = normalizeText(
      parsed.searchParams.get("jobId") ||
        parsed.searchParams.get("jobid") ||
        parsed.searchParams.get("id") ||
        ""
    );
    if (queryMatch) {
      return queryMatch;
    }
  } catch {
    // Ignore URL parsing errors.
  }

  const match = String(jobUrl || "").match(/\/jobs\/(?:details|apply)\/(\d+)/i);
  return match ? normalizeText(match[1]) : "";
}

function buildDetailUrl(club, sourceId) {
  return canonicalizeUrl(club.source_url, `/Recruiting/Jobs/Details/${sourceId}`);
}

function buildApplyUrl(club, sourceId) {
  return canonicalizeUrl(club.source_url, `/Recruiting/Jobs/Apply/${sourceId}`);
}

function extractAssignedJsonObject(scriptText, startIndex) {
  if (startIndex < 0) {
    return "";
  }

  const openIndex = scriptText.indexOf("{", startIndex);
  if (openIndex < 0) {
    return "";
  }

  let depth = 0;
  let quote = "";
  let escaped = false;

  for (let i = openIndex; i < scriptText.length; i += 1) {
    const char = scriptText[i];

    if (quote) {
      if (escaped) {
        escaped = false;
        continue;
      }

      if (char === "\\") {
        escaped = true;
        continue;
      }

      if (char === quote) {
        quote = "";
      }

      continue;
    }

    if (char === '"' || char === "'" || char === "`") {
      quote = char;
      continue;
    }

    if (char === "{") {
      depth += 1;
      continue;
    }

    if (char === "}") {
      depth -= 1;
      if (depth === 0) {
        return scriptText.slice(openIndex, i + 1);
      }
    }
  }

  return "";
}

function parsePageDataObject(html) {
  const $ = cheerio.load(String(html || ""));
  const scripts = $("script").toArray();

  for (const script of scripts) {
    const content = String($(script).html() || "");
    if (!content || !/pageData/i.test(content)) {
      continue;
    }

    const assignments = [
      "window.pageData",
      "pageData",
      "window.__pageData",
      "__pageData",
    ];

    for (const assignment of assignments) {
      const index = content.indexOf(`${assignment} =`);
      if (index < 0) {
        continue;
      }

      const rawObject = extractAssignedJsonObject(content, index);
      if (!rawObject) {
        continue;
      }

      try {
        return JSON.parse(rawObject);
      } catch {
        // Continue looking for another assignment.
      }
    }
  }

  return null;
}

function collectPotentialJobArrays(value, sink) {
  if (!value || typeof value !== "object") {
    return;
  }

  if (Array.isArray(value)) {
    if (
      value.length &&
      value.some(
        (item) =>
          item &&
          typeof item === "object" &&
          (item.JobId ||
            item.jobId ||
            item.JobTitle ||
            item.jobTitle ||
            item.ApplyUrl ||
            item.applyUrl)
      )
    ) {
      sink.push(value);
    }

    for (const item of value) {
      collectPotentialJobArrays(item, sink);
    }

    return;
  }

  for (const child of Object.values(value)) {
    collectPotentialJobArrays(child, sink);
  }
}

function extractJobsFromPageData(pageData) {
  if (!pageData || typeof pageData !== "object") {
    return [];
  }

  if (Array.isArray(pageData.Jobs)) {
    return pageData.Jobs;
  }

  if (Array.isArray(pageData.jobs)) {
    return pageData.jobs;
  }

  const arrays = [];
  collectPotentialJobArrays(pageData, arrays);

  const sorted = arrays.sort((a, b) => b.length - a.length);
  return sorted[0] || [];
}

function resolveLocation(item) {
  const parts = [
    item && (item.LocationName || item.locationName || item.Location || item.location),
    item && (item.City || item.city),
    item && (item.State || item.state || item.Region || item.region),
    item && (item.Country || item.country),
  ]
    .map((part) => normalizeText(part))
    .filter(Boolean);

  return Array.from(new Set(parts)).join(", ");
}

function resolveDepartment(item) {
  const fields = [
    item && item.Department,
    item && item.DepartmentName,
    item && item.department,
    item && item.departmentName,
    item && item.Category,
    item && item.CategoryName,
    item && item.JobCategory,
    item && item.Team,
  ];

  for (const field of fields) {
    const normalized = normalizeText(field);
    if (normalized) {
      return normalized;
    }
  }

  return "";
}

function resolveLocationType(item) {
  const text = normalizeText(
    (item &&
      (item.LocationType ||
        item.locationType ||
        item.WorkLocation ||
        item.workLocation ||
        item.WorksiteType ||
        item.worksiteType ||
        item.JobType)) ||
      ""
  ).toLowerCase();

  if (text.includes("hybrid")) {
    return "hybrid";
  }

  if (text.includes("remote")) {
    return "remote";
  }

  return "onsite";
}

function normalizeListingJob(club, item) {
  const sourceId = normalizeText(
    (item && (item.JobId || item.jobId || item.id || item.Id)) || ""
  );
  if (!sourceId) {
    return null;
  }

  const learnMoreRaw = normalizeText(
    (item &&
      (item.JobDetailsUrl ||
        item.jobDetailsUrl ||
        item.DetailsUrl ||
        item.detailsUrl ||
        item.JobUrl ||
        item.jobUrl ||
        item.Url)) ||
      ""
  );
  const applyRaw = normalizeText(
    (item && (item.ApplyUrl || item.applyUrl || item.ApplicationUrl)) || ""
  );
  const summary = normalizeText(
    (item &&
      (item.JobDescriptionSummary ||
        item.jobDescriptionSummary ||
        item.Summary ||
        item.summary)) ||
      ""
  );
  const employmentType = normalizeText(
    (item &&
      (item.EmploymentType ||
        item.employmentType ||
        item.TimeType ||
        item.timeType ||
        item.Type ||
        item.type)) ||
      ""
  );

  const htmlSummary = summary ? `<p>${summary}</p>` : "";
  const learnMoreUrl =
    canonicalizeUrl(club.source_url, learnMoreRaw) || buildDetailUrl(club, sourceId);
  const applyUrl =
    canonicalizeUrl(club.source_url, applyRaw) || buildApplyUrl(club, sourceId);

  return {
    club_id: club.club_id,
    source_id: sourceId,
    id: sourceId,
    title: normalizeText((item && (item.JobTitle || item.jobTitle || item.Title)) || ""),
    location: resolveLocation(item),
    location_type: resolveLocationType(item),
    department: resolveDepartment(item),
    arrangement: mapArrangementFromEmploymentType(employmentType),
    employment_type: employmentType,
    published_at: parseDateToIso(
      (item &&
        (item.PostedDate ||
          item.postedDate ||
          item.DatePosted ||
          item.datePosted ||
          item.CreatedDate ||
          item.createdDate)) ||
        ""
    ),
    expires_at: parseDateToIso(
      (item &&
        (item.ClosingDate ||
          item.closingDate ||
          item.ExpiryDate ||
          item.expiryDate ||
          item.ExpirationDate ||
          item.expirationDate)) ||
        ""
    ),
    html_description: htmlSummary,
    plain_text_description: htmlToStructuredPlainText(htmlSummary),
    url: learnMoreUrl,
    application_link: applyUrl || learnMoreUrl,
    company_name: club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url: club.company_logo_url || "",
    source_url: club.source_url || "",
    ats: "paylocity",
  };
}

async function loadListingJobs(club) {
  const html = await requestTextWithRetry(club, club.source_url, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    },
  });

  const pageData = parsePageDataObject(html);
  if (pageData) {
    const jobs = extractJobsFromPageData(pageData)
      .map((item) => normalizeListingJob(club, item))
      .filter(Boolean);

    if (jobs.length) {
      return jobs;
    }
  }

  const $ = cheerio.load(html);
  const jobs = [];

  $("a[href]").each((_, node) => {
    const href = normalizeText($(node).attr("href"));
    const absolute = canonicalizeUrl(club.source_url, href);
    if (!absolute) {
      return;
    }

    const sourceId = extractSourceIdFromPaylocityUrl(absolute);
    if (!sourceId || !/\/jobs\/details\//i.test(absolute)) {
      return;
    }

    jobs.push({
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      title: normalizeText($(node).text()),
      location: "",
      location_type: "onsite",
      department: "",
      arrangement: "fulltime",
      employment_type: "",
      published_at: "",
      expires_at: "",
      html_description: "",
      plain_text_description: "",
      url: absolute,
      application_link: buildApplyUrl(club, sourceId),
      company_name: club.name,
      company_url: club.company_url || club.source_url || "",
      company_logo_url: club.company_logo_url || "",
      source_url: club.source_url || "",
      ats: "paylocity",
    });
  });

  return jobs;
}

async function discoverJobUrls(club) {
  let listingJobs = [];

  try {
    listingJobs = await loadListingJobs(club);
  } catch (error) {
    console.warn(`[warn] ${club.club_id}: paylocity discover falló -> ${error.message}`);
    return [];
  }

  const urls = [];
  for (const job of listingJobs) {
    if (!job || !job.source_id || !job.url) {
      continue;
    }

    JOB_CACHE.set(buildCacheKey(club, job.source_id), job);
    urls.push(job.url);
  }

  return Array.from(new Set(urls));
}

function resolveApplicationLink($, jobUrl, sourceId, fallback) {
  let applicationLink = normalizeText(fallback);
  const expectedFragment = sourceId ? `/recruiting/jobs/apply/${sourceId}` : "";

  $("a[href]").each((_, node) => {
    if (applicationLink && expectedFragment && applicationLink.toLowerCase().includes(expectedFragment)) {
      return;
    }

    const href = normalizeText($(node).attr("href"));
    const absolute = canonicalizeUrl(jobUrl, href);
    if (!absolute) {
      return;
    }

    const text = normalizeText($(node).text()).toLowerCase();
    const lowerHref = absolute.toLowerCase();
    const isApplyLink =
      lowerHref.includes("/recruiting/jobs/apply/") ||
      lowerHref.includes("/jobs/apply/") ||
      text.includes("apply");

    if (isApplyLink) {
      applicationLink = absolute;
    }
  });

  if (applicationLink) {
    return applicationLink;
  }

  if (sourceId) {
    return buildApplyUrl(
      {
        source_url: jobUrl,
      },
      sourceId
    );
  }

  return normalizeText(jobUrl);
}

function resolveLocationTypeFromText(value) {
  const normalized = normalizeText(value).toLowerCase();
  if (normalized.includes("hybrid")) {
    return "hybrid";
  }
  if (normalized.includes("remote")) {
    return "remote";
  }
  return "onsite";
}

async function fetchJob(club, jobUrl) {
  const sourceId = extractSourceIdFromPaylocityUrl(jobUrl);
  if (!sourceId) {
    throw new Error(`No se pudo extraer source_id desde ${jobUrl}`);
  }

  const cacheKey = buildCacheKey(club, sourceId);
  let cached = JOB_CACHE.get(cacheKey);
  if (!cached) {
    await discoverJobUrls(club);
    cached = JOB_CACHE.get(cacheKey);
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

  const title =
    normalizeText(jobPosting && (jobPosting.title || jobPosting.name)) ||
    normalizeText($("h1").first().text()) ||
    normalizeText(cached && cached.title);
  const location =
    extractLocationFromJsonLd(jobPosting) ||
    findValueByLabels($, ["location", "job location", "office location"]) ||
    normalizeText(cached && cached.location);
  const employmentType =
    extractEmploymentTypeFromJsonLd(jobPosting) ||
    findValueByLabels($, ["employment type", "job type", "schedule", "contract"]) ||
    normalizeText(cached && cached.employment_type);
  const department =
    findValueByLabels($, ["department", "team", "function", "division"]) ||
    normalizeText(cached && cached.department);
  const htmlDescription = normalizeHtmlFragment(
    (jobPosting && jobPosting.description) || selectDescriptionHtml($)
  );
  const plainTextDescription = htmlToStructuredPlainText(htmlDescription);
  const locationTypeRaw = findValueByLabels($, [
    "location type",
    "workplace",
    "work model",
  ]);
  const applicationLink = resolveApplicationLink(
    $,
    jobUrl,
    sourceId,
    normalizeText(cached && cached.application_link)
  );

  return {
    club_id: club.club_id,
    source_id: sourceId,
    id: sourceId,
    title,
    location,
    location_type: resolveLocationTypeFromText(locationTypeRaw || location),
    department,
    arrangement: mapArrangementFromEmploymentType(employmentType),
    employment_type: employmentType,
    published_at: parseDateToIso(
      (jobPosting && (jobPosting.datePosted || jobPosting.dateCreated)) ||
        findValueByLabels($, ["posted", "posted date", "date posted"]) ||
        normalizeText(cached && cached.published_at)
    ),
    expires_at: parseDateToIso(
      (jobPosting && jobPosting.validThrough) ||
        findValueByLabels($, ["closing date", "deadline", "valid through"]) ||
        normalizeText(cached && cached.expires_at)
    ),
    html_description: htmlDescription,
    plain_text_description: plainTextDescription,
    url: normalizeText(jobUrl),
    application_link: applicationLink || normalizeText(jobUrl),
    company_name: club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url: club.company_logo_url || "",
    source_url: club.source_url || "",
    ats: "paylocity",
    _jobPosting: jobPosting || undefined,
  };
}

module.exports = {
  discoverJobUrls,
  fetchJob,
};
