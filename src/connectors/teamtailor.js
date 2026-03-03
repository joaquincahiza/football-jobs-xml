const cheerio = require("cheerio");
const {
  MAX_DISCOVERY_PAGES,
  withPage,
  gotoWithRetry,
  canonicalizeUrl,
  normalizeText,
  extractJobPostingJsonLd,
  extractIdentifierFromJsonLd,
  extractLocationFromJsonLd,
  extractEmploymentTypeFromJsonLd,
  parseDateToIso,
  htmlToStructuredPlainText,
  selectDescriptionHtml,
  findValueByLabels,
  extractSourceIdFromUrl,
  mapArrangementFromEmploymentType,
  normalizeHtmlFragment,
  createSession,
} = require("./utils");

const JOB_PATH_REGEX = /^\/jobs\/[^/?#]+\/?$/i;
const LISTING_PATH_REGEX = /^\/jobs\/?$/i;
const JOB_LINK_IN_HTML_REGEX = /(?:https?:\/\/[^\s"'<>]+|\/jobs\/[a-z0-9][a-z0-9-]*)/gi;

function isTeamtailorJobUrl(url) {
  try {
    return JOB_PATH_REGEX.test(new URL(url).pathname);
  } catch {
    return false;
  }
}

function isTeamtailorListingUrl(url) {
  try {
    return LISTING_PATH_REGEX.test(new URL(url).pathname);
  } catch {
    return false;
  }
}

function extractSourceIdFromTeamtailorUrl(jobUrl) {
  try {
    const parsed = new URL(jobUrl);
    const segment = parsed.pathname.split("/").filter(Boolean).pop() || "";

    const leadingNumber = segment.match(/^(\d{3,})/);
    if (leadingNumber) {
      return leadingNumber[1];
    }

    const trailingNumber = segment.match(/-(\d{3,})$/);
    if (trailingNumber) {
      return trailingNumber[1];
    }
  } catch {
    // Ignore URL parsing errors.
  }

  return extractSourceIdFromUrl(jobUrl);
}

function isPaginationLink(url, linkText) {
  const text = normalizeText(linkText).toLowerCase();
  if (text.includes("next") || text.includes("more") || text.includes("page")) {
    return true;
  }

  try {
    return new URL(url).searchParams.has("page");
  } catch {
    return false;
  }
}

function extractFallbackLocation($) {
  const byLabel = findValueByLabels($, [
    "location",
    "job location",
    "office location",
  ]);
  if (byLabel) {
    return byLabel;
  }

  const selectors = [
    '[data-testid*="location"]',
    '[class*="location"]',
    '[itemprop="jobLocation"]',
  ];

  for (const selector of selectors) {
    const text = normalizeText($(selector).first().text());
    if (text) {
      return text;
    }
  }

  return "";
}

function extractFallbackEmploymentType($) {
  return findValueByLabels($, [
    "employment type",
    "contract type",
    "job type",
    "type",
  ]);
}

async function discoverJobUrls(club, options = {}) {
  return withPage(options, async (page) => {
    const queue = [club.source_url];
    const queued = new Set(queue);
    const visited = new Set();
    const jobUrls = new Set();
    let seedHost = "";
    let crawledPages = 0;

    try {
      seedHost = new URL(club.source_url).hostname;
    } catch {
      seedHost = "";
    }

    while (queue.length && crawledPages < MAX_DISCOVERY_PAGES) {
      const currentUrl = queue.shift();
      queued.delete(currentUrl);

      if (visited.has(currentUrl)) {
        continue;
      }

      await gotoWithRetry(page, currentUrl);
      visited.add(currentUrl);
      crawledPages += 1;

      const html = await page.content();
      const $ = cheerio.load(html);

      $("a[href]").each((_, el) => {
        const href = $(el).attr("href");
        const absoluteUrl = canonicalizeUrl(currentUrl, href);
        if (!absoluteUrl) {
          return;
        }

        let parsed;
        try {
          parsed = new URL(absoluteUrl);
        } catch {
          return;
        }

        if (seedHost && parsed.hostname !== seedHost) {
          return;
        }

        if (isTeamtailorJobUrl(absoluteUrl)) {
          jobUrls.add(canonicalizeUrl(currentUrl, absoluteUrl, { dropSearch: true }));
        }

        if (
          (isTeamtailorListingUrl(absoluteUrl) ||
            isPaginationLink(absoluteUrl, $(el).text())) &&
          !visited.has(absoluteUrl) &&
          !queued.has(absoluteUrl)
        ) {
          queue.push(absoluteUrl);
          queued.add(absoluteUrl);
        }
      });

      const matches = html.match(JOB_LINK_IN_HTML_REGEX) || [];
      for (const match of matches) {
        const absoluteUrl = canonicalizeUrl(currentUrl, match);
        if (!absoluteUrl || !isTeamtailorJobUrl(absoluteUrl)) {
          continue;
        }

        try {
          const parsed = new URL(absoluteUrl);
          if (seedHost && parsed.hostname !== seedHost) {
            continue;
          }
        } catch {
          continue;
        }

        jobUrls.add(canonicalizeUrl(currentUrl, absoluteUrl, { dropSearch: true }));
      }
    }

    return Array.from(jobUrls).filter(Boolean);
  });
}

async function fetchJob(club, jobUrl, options = {}) {
  return withPage(options, async (page) => {
    await gotoWithRetry(page, jobUrl);
    const $ = cheerio.load(await page.content());
    const jobPosting = extractJobPostingJsonLd($);

    const sourceId =
      extractSourceIdFromTeamtailorUrl(jobUrl) ||
      extractIdentifierFromJsonLd(jobPosting);
    const title =
      normalizeText((jobPosting && (jobPosting.title || jobPosting.name)) || "") ||
      normalizeText($("h1").first().text());
    const location =
      extractLocationFromJsonLd(jobPosting) || extractFallbackLocation($);
    const employmentType =
      extractEmploymentTypeFromJsonLd(jobPosting) ||
      extractFallbackEmploymentType($);
    const htmlDescription =
      normalizeHtmlFragment(jobPosting && jobPosting.description) ||
      selectDescriptionHtml($);
    const plainTextDescription = htmlToStructuredPlainText(htmlDescription);
    const publishedAt = parseDateToIso(
      (jobPosting && (jobPosting.datePosted || jobPosting.dateCreated)) ||
        findValueByLabels($, ["posted on", "published", "date posted", "posted"])
    );
    const expiresAt = parseDateToIso(
      (jobPosting && jobPosting.validThrough) ||
        findValueByLabels($, [
          "closing date",
          "application deadline",
          "expires",
          "valid through",
        ])
    );

    return {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      url: jobUrl,
      application_link: jobUrl,
      title,
      arrangement: mapArrangementFromEmploymentType(employmentType),
      employment_type: employmentType,
      location_type: "onsite",
      location,
      published_at: publishedAt,
      expires_at: expiresAt,
      highlighted: false,
      sticky: false,
      html_description: htmlDescription,
      plain_text_description: plainTextDescription,
      company_name: club.name,
      company_url: club.company_url || club.source_url || "",
      company_logo_url: club.company_logo_url || "",
      _jobPosting: jobPosting || undefined,
    };
  });
}

module.exports = {
  discoverJobUrls,
  fetchJob,
  createSession,
};
