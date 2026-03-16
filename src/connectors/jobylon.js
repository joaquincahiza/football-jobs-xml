const cheerio = require("cheerio");
const {
  MAX_DISCOVERY_PAGES,
  withPage,
  gotoWithRetry,
  createSession,
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

function extractSourceIdFromJobylonUrl(value) {
  try {
    const parsed = new URL(value);
    const pathMatch = parsed.pathname.match(/\/jobs\/(\d+)(?:[-/]|$)/i);
    if (pathMatch) {
      return normalizeText(pathMatch[1]);
    }

    const byQuery = normalizeText(
      parsed.searchParams.get("job") ||
        parsed.searchParams.get("id") ||
        parsed.searchParams.get("job_id") ||
        ""
    );
    if (byQuery) {
      return byQuery;
    }
  } catch {
    // Ignore URL parsing failures.
  }

  const match = String(value || "").match(/\/jobs\/(\d+)(?:[-/]|$)/i);
  return match ? normalizeText(match[1]) : "";
}

function isJobylonJobUrl(value) {
  try {
    const parsed = new URL(value);
    return /jobylon\.com$/i.test(parsed.hostname) && /\/jobs\/\d+/i.test(parsed.pathname);
  } catch {
    return false;
  }
}

function shouldQueueUrl(seedHost, url, text) {
  try {
    const parsed = new URL(url);
    const sameHost = !seedHost || parsed.hostname === seedHost;
    if (!sameHost) {
      return false;
    }

    const normalizedText = normalizeText(text).toLowerCase();
    return (
      /jobs?|careers?|opportunit/i.test(parsed.pathname) ||
      normalizedText.includes("jobs") ||
      normalizedText.includes("careers") ||
      normalizedText.includes("next") ||
      normalizedText.includes("more")
    );
  } catch {
    return false;
  }
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

async function extractJobLinksFromPage(page, pageUrl) {
  const html = await page.content();
  const $ = cheerio.load(html);
  const jobUrls = new Set();
  const queueCandidates = new Set();

  $("a[href]").each((_, node) => {
    const href = normalizeText($(node).attr("href"));
    const absolute = canonicalizeUrl(pageUrl, href);
    if (!absolute) {
      return;
    }

    if (isJobylonJobUrl(absolute)) {
      jobUrls.add(absolute);
      return;
    }

    queueCandidates.add(`${absolute}|||${normalizeText($(node).text())}`);
  });

  return {
    html,
    jobUrls: Array.from(jobUrls),
    queueCandidates: Array.from(queueCandidates),
  };
}

async function discoverJobUrls(club, options = {}) {
  return withPage(options, async (page) => {
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

      await gotoWithRetry(page, currentUrl);
      await page.waitForTimeout(1000);
      const extracted = await extractJobLinksFromPage(page, currentUrl);

      for (const jobUrl of extracted.jobUrls) {
        jobs.add(jobUrl);
      }

      for (const candidate of extracted.queueCandidates) {
        const [url, text] = candidate.split("|||");
        if (!url) {
          continue;
        }

        if (shouldQueueUrl(seedHost, url, text) && !visited.has(url) && !queued.has(url)) {
          queue.push(url);
          queued.add(url);
        }
      }

      visited.add(currentUrl);
      pages += 1;
    }

    if (!jobs.size && club.jobylon_host) {
      const fallbackListing = canonicalizeUrl(club.jobylon_host, "/jobs");
      if (fallbackListing) {
        await gotoWithRetry(page, fallbackListing);
        await page.waitForTimeout(1000);
        const extracted = await extractJobLinksFromPage(page, fallbackListing);
        for (const jobUrl of extracted.jobUrls) {
          jobs.add(jobUrl);
        }
      }
    }

    return Array.from(jobs);
  });
}

function resolveApplyUrl($, jobUrl, sourceId) {
  let applicationLink = "";

  $("a[href]").each((_, node) => {
    if (applicationLink) {
      return;
    }

    const href = normalizeText($(node).attr("href"));
    const absolute = canonicalizeUrl(jobUrl, href);
    if (!absolute) {
      return;
    }

    const text = normalizeText($(node).text()).toLowerCase();
    const isApplyLink =
      /\/applications\/jobs\/\d+\/create\/?$/i.test(absolute) ||
      text.includes("apply") ||
      text.includes("ansök") ||
      text.includes("candidate");

    if (isApplyLink) {
      applicationLink = absolute;
    }
  });

  if (applicationLink) {
    return applicationLink;
  }

  if (sourceId) {
    try {
      const parsed = new URL(jobUrl);
      return `${parsed.origin}/applications/jobs/${sourceId}/create/`;
    } catch {
      return normalizeText(jobUrl);
    }
  }

  return normalizeText(jobUrl);
}

async function fetchJob(club, jobUrl, options = {}) {
  return withPage(options, async (page) => {
    await gotoWithRetry(page, jobUrl);
    await page.waitForTimeout(1000);

    const html = await page.content();
    const $ = cheerio.load(html);
    const jobPosting = extractJobPostingJsonLd($);
    const sourceId =
      extractSourceIdFromJobylonUrl(jobUrl) ||
      normalizeText(findValueByLabels($, ["id", "job id", "reference"]));
    const employmentType =
      extractEmploymentTypeFromJsonLd(jobPosting) ||
      findValueByLabels($, ["employment type", "contract type", "job type"]);
    const locationTypeRaw = findValueByLabels($, [
      "location type",
      "work model",
      "workplace",
    ]);
    const htmlDescription = normalizeHtmlFragment(
      (jobPosting && jobPosting.description) || selectDescriptionHtml($)
    );
    const plainTextDescription = htmlToStructuredPlainText(htmlDescription);

    return {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      title:
        normalizeText(jobPosting && (jobPosting.title || jobPosting.name)) ||
        normalizeText($("h1").first().text()),
      location:
        extractLocationFromJsonLd(jobPosting) ||
        findValueByLabels($, ["location", "city", "office", "country"]),
      location_type: resolveLocationType(locationTypeRaw),
      department: findValueByLabels($, ["department", "team", "function", "division"]),
      arrangement: mapArrangementFromEmploymentType(employmentType),
      employment_type: employmentType,
      published_at: parseDateToIso(
        (jobPosting && (jobPosting.datePosted || jobPosting.dateCreated)) ||
          findValueByLabels($, ["posted on", "published", "date posted"])
      ),
      expires_at: parseDateToIso(
        (jobPosting && jobPosting.validThrough) ||
          findValueByLabels($, ["closing date", "deadline", "expires"])
      ),
      html_description: htmlDescription,
      plain_text_description: plainTextDescription,
      url: normalizeText(jobUrl),
      application_link: resolveApplyUrl($, jobUrl, sourceId),
      company_name: club.name,
      company_url: club.company_url || club.source_url || "",
      company_logo_url: club.company_logo_url || "",
      source_url: club.source_url || "",
      ats: "jobylon",
      _jobPosting: jobPosting || undefined,
    };
  });
}

module.exports = {
  discoverJobUrls,
  fetchJob,
  createSession,
};
