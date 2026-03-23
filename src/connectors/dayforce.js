const {
  RATE_LIMIT_MS,
  RETRIES,
  normalizeText,
  canonicalizeUrl,
  parseDateToIso,
  htmlToStructuredPlainText,
  normalizeHtmlFragment,
  gotoWithRetry,
  withPage,
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

function extractSourceIdFromDayforceUrl(jobUrl) {
  try {
    const parsed = new URL(jobUrl);
    const match = parsed.pathname.match(/\/jobs\/(\d+)(?:\/|$)/i);
    if (match) {
      return normalizeText(match[1]);
    }
  } catch {
    // Ignore URL parsing failures.
  }

  const fallback = String(jobUrl || "").match(/\/jobs\/(\d+)(?:\/|$)/i);
  return fallback ? normalizeText(fallback[1]) : "";
}

function isDayforceDetailUrlForClub(club, url) {
  try {
    const parsed = new URL(url);
    const listingUrl = new URL(club.source_url);
    return (
      parsed.hostname === listingUrl.hostname &&
      normalizeText(parsed.pathname).toLowerCase().includes("/jobs/") &&
      /\/jobs\/\d+(?:\/)?$/i.test(parsed.pathname)
    );
  } catch {
    return false;
  }
}

function extractNextData(html) {
  const match = String(html || "").match(
    /<script id="__NEXT_DATA__" type="application\/json">([\s\S]*?)<\/script>/i
  );

  if (!match) {
    return null;
  }

  try {
    return JSON.parse(match[1]);
  } catch {
    return null;
  }
}

function getDehydratedQueries(nextData) {
  return (
    (((nextData || {}).props || {}).pageProps || {}).dehydratedState?.queries || []
  );
}

function getQueryKeyName(query) {
  const key = query && query.queryKey;
  return Array.isArray(key) ? normalizeText(key[0]).toLowerCase() : "";
}

function extractQueryData(nextData, queryName) {
  const normalizedName = normalizeText(queryName).toLowerCase();
  return (
    getDehydratedQueries(nextData).find(
      (query) => getQueryKeyName(query) === normalizedName
    )?.state?.data || null
  );
}

function extractDayforceLocation(jobData) {
  const locations = Array.isArray(jobData && jobData.postingLocations)
    ? jobData.postingLocations
    : [];

  const formatted = locations
    .map((location) => normalizeText(location && location.formattedAddress))
    .filter(Boolean);
  if (formatted.length) {
    return Array.from(new Set(formatted)).join(" | ");
  }

  const parts = locations
    .map((location) =>
      [
        normalizeText(location && location.cityName),
        normalizeText(location && location.stateCode),
        normalizeText(location && location.isoCountryCode),
      ]
        .filter(Boolean)
        .join(", ")
    )
    .filter(Boolean);

  return Array.from(new Set(parts)).join(" | ");
}

function extractDayforceDescriptionHtml(jobData) {
  const content = (jobData && jobData.jobPostingContent) || {};
  return normalizeHtmlFragment(
    [
      content.jobDescriptionHeader,
      content.jobDescription,
      content.jobDescriptionFooter,
    ]
      .map((value) => String(value || "").trim())
      .filter(Boolean)
      .join("\n")
  );
}

function resolveArrangement(jobData, plainTextDescription) {
  const hints = [
    normalizeText(jobData && jobData.jobTitle),
    plainTextDescription.slice(0, 800),
    ...((Array.isArray(jobData && jobData.jobPostingAttributes)
      ? jobData.jobPostingAttributes
      : []
    ).map((attribute) =>
      `${normalizeText(attribute && attribute.name)} ${normalizeText(
        attribute && attribute.value
      )}`
    )),
  ]
    .join(" ")
    .toLowerCase();

  if (/\bpt\b|part[\s-]*time/.test(hints)) {
    return "parttime";
  }

  if (/casual|seasonal/.test(hints)) {
    return "casual";
  }

  if (/contract|temporary|\btemp\b|fixed term|fixed-term/.test(hints)) {
    return "contract";
  }

  return "fulltime";
}

function resolveLocationType(jobData, plainTextDescription) {
  if (jobData && jobData.hasVirtualLocation) {
    return "remote";
  }

  const hints = [
    normalizeText(jobData && jobData.jobTitle),
    plainTextDescription.slice(0, 600),
  ]
    .join(" ")
    .toLowerCase();

  if (hints.includes("hybrid")) {
    return "hybrid";
  }

  if (hints.includes("remote")) {
    return "remote";
  }

  return "onsite";
}

function buildApplyUrl(jobUrl) {
  const normalized = normalizeText(jobUrl).replace(/\/+$/, "");
  if (!normalized) {
    return "";
  }

  return `${normalized}/apply?flowSelection=true`;
}

function extractDepartmentFromTitle(title) {
  const normalizedTitle = normalizeText(title);
  if (!normalizedTitle.includes(" - ")) {
    return "";
  }

  return normalizeText(normalizedTitle.split(" - ")[0]);
}

async function discoverJobUrls(club, options = {}) {
  return withPage(options, async (page) => {
    const discovered = new Set();
    let stableRounds = 0;
    let previousCount = 0;

    await gotoWithRetry(page, club.source_url);

    for (let round = 0; round < 8 && stableRounds < 2; round += 1) {
      try {
        await page.waitForSelector('a[href*="/jobs/"]', { timeout: 15000 });
      } catch {
        // Continue with the current DOM snapshot.
      }

      const hrefs = await page.$$eval('a[href*="/jobs/"]', (nodes) =>
        nodes
          .map((node) => node.getAttribute("href") || "")
          .map((value) => value.trim())
          .filter(Boolean)
      );

      for (const href of hrefs) {
        const absoluteUrl = canonicalizeUrl(club.source_url, href, {
          dropSearch: true,
        });
        if (isDayforceDetailUrlForClub(club, absoluteUrl)) {
          discovered.add(absoluteUrl);
        }
      }

      if (discovered.size === previousCount) {
        stableRounds += 1;
      } else {
        stableRounds = 0;
        previousCount = discovered.size;
      }

      await page.evaluate(() => {
        window.scrollTo(0, document.body.scrollHeight);
      });
      await page.waitForTimeout(1500);
    }

    return Array.from(discovered);
  });
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

  const nextData = extractNextData(html);
  const jobData = extractQueryData(nextData, "jobs");
  if (!jobData) {
    throw new Error("No se encontró el estado del job en __NEXT_DATA__");
  }

  const siteInfo = extractQueryData(nextData, "site-info") || {};
  const sourceId = extractSourceIdFromDayforceUrl(jobUrl);
  const htmlDescription = extractDayforceDescriptionHtml(jobData);
  const plainTextDescription = htmlToStructuredPlainText(htmlDescription);

  return {
    source_id: sourceId,
    id: sourceId,
    url: jobUrl,
    application_link: buildApplyUrl(jobUrl),
    title: normalizeText(jobData.jobTitle),
    arrangement: resolveArrangement(jobData, plainTextDescription),
    location_type: resolveLocationType(jobData, plainTextDescription),
    location: extractDayforceLocation(jobData),
    published_at: parseDateToIso(
      jobData.postingStartTimestampUTC || jobData.createdTimestampUTC
    ),
    expires_at: parseDateToIso(jobData.postingExpiryTimestampUTC),
    html_description: htmlDescription,
    plain_text_description: plainTextDescription,
    department: extractDepartmentFromTitle(jobData.jobTitle),
    company_name:
      normalizeText(siteInfo.candidateCorrespondenceClientName) || club.name,
    company_url: club.company_url || club.source_url || "",
    company_logo_url:
      normalizeText(
        (siteInfo.smallLogo && siteInfo.smallLogo.url) ||
          (siteInfo.largeLogo && siteInfo.largeLogo.url)
      ) || club.company_logo_url || "",
    _meta: {
      job_req_id: normalizeText(jobData.jobReqId),
      job_application_template_id: normalizeText(jobData.jobApplicationTemplateId),
      posting_type: normalizeText(jobData.postingType),
    },
  };
}

module.exports = {
  discoverJobUrls,
  fetchJob,
};
