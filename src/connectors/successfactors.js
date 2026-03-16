const cheerio = require("cheerio");
const {
  MAX_DISCOVERY_PAGES,
  withPage,
  gotoWithRetry,
  createSession,
  normalizeText,
  parseDateToIso,
  canonicalizeUrl,
  normalizeHtmlFragment,
  htmlToStructuredPlainText,
  findValueByLabels,
  mapArrangementFromEmploymentType,
  extractJobPostingJsonLd,
  extractIdentifierFromJsonLd,
  extractLocationFromJsonLd,
  extractEmploymentTypeFromJsonLd,
  selectDescriptionHtml,
} = require("./utils");

const JOB_CACHE = new Map();

function buildCacheKey(club, sourceId) {
  return `${normalizeText(club && club.club_id)}::${normalizeText(sourceId)}`;
}

function extractSourceIdFromSuccessFactorsUrl(value) {
  try {
    const parsed = new URL(value);
    const pathMatch = parsed.pathname.match(/\/job\/[^/?#]+\/(\d{3,})\/?$/i);
    if (pathMatch) {
      return normalizeText(pathMatch[1]);
    }

    const candidates = [
      parsed.searchParams.get("career_job_req_id"),
      parsed.searchParams.get("jobReqId"),
      parsed.searchParams.get("job_id"),
      parsed.searchParams.get("jobId"),
      parsed.searchParams.get("reqId"),
    ];

    for (const candidate of candidates) {
      const normalized = normalizeText(candidate);
      if (normalized) {
        return normalized;
      }
    }
  } catch {
    // Ignore URL parsing failures.
  }

  const match = String(value || "").match(
    /(?:career(?:_|%5f)job(?:_|%5f)req(?:_|%5f)id|jobReqId|jobId|reqId)=([0-9]{2,})/i
  );
  if (match) {
    return normalizeText(match[1]);
  }

  const byPath = String(value || "").match(/\/job\/[^/?#]+\/(\d{3,})\/?$/i);
  return byPath ? normalizeText(byPath[1]) : "";
}

function buildFallbackDetailUrl(club, sourceId) {
  try {
    const src = new URL(club.source_url);
    const detail = new URL("/career", src.origin);
    const company = normalizeText(src.searchParams.get("company"));
    if (company) {
      detail.searchParams.set("company", company);
    }
    detail.searchParams.set("career_ns", "job_listing");
    detail.searchParams.set("navBarLevel", "JOB_SEARCH");
    detail.searchParams.set("career_job_req_id", sourceId);
    return detail.href;
  } catch {
    return "";
  }
}

function resolveLocationType(value) {
  const text = normalizeText(value).toLowerCase();
  if (text.includes("hybrid")) {
    return "hybrid";
  }
  if (text.includes("remote")) {
    return "remote";
  }
  return "onsite";
}

function extractListingMeta($, anchorNode) {
  const card = $(anchorNode).closest("tr,li,article,[class*='job'],[class*='career']").first();
  if (!card.length) {
    return {
      title: normalizeText($(anchorNode).text()),
      location: "",
      department: "",
      published_at: "",
      employment_type: "",
    };
  }

  const local$ = cheerio.load(`<section id="__sf_card__">${card.html() || ""}</section>`);
  const cardText = normalizeText(card.text());

  return {
    title:
      normalizeText($(anchorNode).text()) ||
      normalizeText(
        card
          .find("h1,h2,h3,h4,h5,h6,[class*='title']")
          .first()
          .text()
      ) ||
      "",
    location:
      findValueByLabels(local$, ["location", "job location", "city"]) ||
      normalizeText(
        card
          .find("[class*='location'],[data-automation-id*='location']")
          .first()
          .text()
      ) ||
      "",
    department:
      findValueByLabels(local$, ["department", "division", "team", "function"]) ||
      normalizeText(
        card
          .find("[class*='department'],[class*='division']")
          .first()
          .text()
      ) ||
      "",
    published_at:
      findValueByLabels(local$, [
        "posted on",
        "date posted",
        "posted",
        "publication date",
      ]) || "",
    employment_type:
      findValueByLabels(local$, ["employment type", "job type", "contract"]) || "",
    _raw: cardText,
  };
}

function collectJobIdsFromHtml(html) {
  const ids = new Set();
  const source = String(html || "");
  const patterns = [
    /career(?:_|%5f)job(?:_|%5f)req(?:_|%5f)id=([0-9]{2,})/gi,
    /"career_job_req_id"\s*:\s*"([0-9]{2,})"/gi,
    /"jobReqId"\s*:\s*"([0-9]{2,})"/gi,
    /"jobReqId"\s*:\s*([0-9]{2,})/gi,
    /\/job\/[^"'<>/?]+\D+(\d{3,})\/?/gi,
    /job-id-(\d{3,})/gi,
  ];

  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) {
      const id = normalizeText(match[1]);
      if (id) {
        ids.add(id);
      }
    }
  }

  return Array.from(ids);
}

function collectJobDetailUrlsFromHtml(baseUrl, html) {
  const urls = new Set();
  const source = String(html || "");
  const patterns = [
    /\/job\/[^"'<>?#]+\/\d{3,}\/?/gi,
    /https?:\/\/[^"'<>]+\/job\/[^"'<>?#]+\/\d{3,}\/?/gi,
  ];

  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) {
      const normalized = canonicalizeUrl(baseUrl, match[0]);
      if (normalized) {
        urls.add(normalized);
      }
    }
  }

  return Array.from(urls);
}

function shouldQueueSuccessFactorsUrl(seedHost, url, text) {
  try {
    const parsed = new URL(url);
    if (seedHost && parsed.hostname !== seedHost) {
      return false;
    }

    const hasPagingParam =
      parsed.searchParams.has("startrow") ||
      parsed.searchParams.has("startRow") ||
      parsed.searchParams.has("startIndex") ||
      parsed.searchParams.has("page");
    const normalizedText = normalizeText(text).toLowerCase();

    return (
      hasPagingParam ||
      normalizedText.includes("next") ||
      normalizedText.includes("more") ||
      normalizedText.includes("page")
    );
  } catch {
    return false;
  }
}

async function discoverJobUrls(club, options = {}) {
  return withPage(options, async (page) => {
    const queue = [club.source_url];
    const queued = new Set(queue);
    const visited = new Set();
    const listings = new Map();
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
      await page.waitForTimeout(1200);
      const html = await page.content();
      const $ = cheerio.load(html);

      $("a[href]").each((_, node) => {
        const href = normalizeText($(node).attr("href"));
        const absolute = canonicalizeUrl(currentUrl, href);
        if (!absolute) {
          return;
        }

        const sourceId = extractSourceIdFromSuccessFactorsUrl(absolute);
        if (sourceId) {
          const meta = extractListingMeta($, node);
          const existing = listings.get(sourceId) || {
            source_id: sourceId,
            id: sourceId,
            url: absolute,
            application_link: absolute,
            title: "",
            location: "",
            department: "",
            published_at: "",
            employment_type: "",
            location_type: "onsite",
          };

          if (!existing.url) {
            existing.url = absolute;
          }
          if (!existing.application_link) {
            existing.application_link = absolute;
          }
          if (!existing.title && meta.title) {
            existing.title = meta.title;
          }
          if (!existing.location && meta.location) {
            existing.location = meta.location;
          }
          if (!existing.department && meta.department) {
            existing.department = meta.department;
          }
          if (!existing.published_at && meta.published_at) {
            existing.published_at = meta.published_at;
          }
          if (!existing.employment_type && meta.employment_type) {
            existing.employment_type = meta.employment_type;
          }

          listings.set(sourceId, existing);
        }

        if (
          shouldQueueSuccessFactorsUrl(seedHost, absolute, $(node).text()) &&
          !visited.has(absolute) &&
          !queued.has(absolute)
        ) {
          queue.push(absolute);
          queued.add(absolute);
        }
      });

      for (const detailUrl of collectJobDetailUrlsFromHtml(currentUrl, html)) {
        const sourceId = extractSourceIdFromSuccessFactorsUrl(detailUrl);
        if (!sourceId) {
          continue;
        }

        const existing = listings.get(sourceId) || {
          source_id: sourceId,
          id: sourceId,
          url: detailUrl,
          application_link: detailUrl,
          title: "",
          location: "",
          department: "",
          published_at: "",
          employment_type: "",
          location_type: "onsite",
        };

        if (!existing.url) {
          existing.url = detailUrl;
        }
        if (!existing.application_link) {
          existing.application_link = detailUrl;
        }

        listings.set(sourceId, existing);
      }

      for (const sourceId of collectJobIdsFromHtml(html)) {
        if (listings.has(sourceId)) {
          continue;
        }

        const fallbackUrl = buildFallbackDetailUrl(club, sourceId);
        if (!fallbackUrl) {
          continue;
        }

        listings.set(sourceId, {
          source_id: sourceId,
          id: sourceId,
          url: fallbackUrl,
          application_link: fallbackUrl,
          title: "",
          location: "",
          department: "",
          published_at: "",
          employment_type: "",
          location_type: "onsite",
        });
      }

      visited.add(currentUrl);
      pages += 1;
    }

    const urls = [];
    for (const listing of listings.values()) {
      if (!listing.source_id) {
        continue;
      }

      if (!listing.url) {
        listing.url = buildFallbackDetailUrl(club, listing.source_id);
      }
      if (!listing.url) {
        continue;
      }

      if (!listing.application_link) {
        listing.application_link = listing.url;
      }

      JOB_CACHE.set(buildCacheKey(club, listing.source_id), listing);
      urls.push(listing.url);
    }

    return Array.from(new Set(urls));
  });
}

function resolveApplyLink($, jobUrl) {
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
    if (
      text.includes("apply") ||
      text.includes("bewerb") ||
      /\/career\?/i.test(absolute)
    ) {
      applicationLink = absolute;
    }
  });

  return applicationLink || normalizeText(jobUrl);
}

async function fetchJob(club, jobUrl, options = {}) {
  return withPage(options, async (page) => {
    await gotoWithRetry(page, jobUrl);
    await page.waitForTimeout(1200);
    const html = await page.content();
    const $ = cheerio.load(html);
    const jobPosting = extractJobPostingJsonLd($);

    let sourceId =
      extractIdentifierFromJsonLd(jobPosting) ||
      extractSourceIdFromSuccessFactorsUrl(jobUrl);
    if (!sourceId) {
      sourceId = normalizeText(
        findValueByLabels($, ["job requisition id", "job req id", "requisition id"])
      );
    }

    const cached = sourceId ? JOB_CACHE.get(buildCacheKey(club, sourceId)) : null;
    const locationTypeRaw = findValueByLabels($, [
      "location type",
      "work model",
      "workplace",
    ]);
    const employmentType =
      extractEmploymentTypeFromJsonLd(jobPosting) ||
      findValueByLabels($, ["employment type", "contract type", "job type"]) ||
      normalizeText(cached && cached.employment_type);

    const htmlDescription = normalizeHtmlFragment(
      (jobPosting && jobPosting.description) || selectDescriptionHtml($)
    );
    const plainTextDescription = htmlToStructuredPlainText(htmlDescription);
    const applicationLink = resolveApplyLink($, jobUrl);

    return {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      title:
        normalizeText(jobPosting && (jobPosting.title || jobPosting.name)) ||
        normalizeText($("h1").first().text()) ||
        normalizeText(cached && cached.title),
      location:
        extractLocationFromJsonLd(jobPosting) ||
        findValueByLabels($, ["location", "job location", "city"]) ||
        normalizeText(cached && cached.location),
      location_type: resolveLocationType(locationTypeRaw),
      department:
        findValueByLabels($, ["department", "division", "team", "function"]) ||
        normalizeText(cached && cached.department),
      arrangement: mapArrangementFromEmploymentType(employmentType),
      employment_type: employmentType,
      published_at: parseDateToIso(
        (jobPosting && (jobPosting.datePosted || jobPosting.dateCreated)) ||
          findValueByLabels($, ["posted on", "date posted", "publication date"]) ||
          normalizeText(cached && cached.published_at)
      ),
      expires_at: parseDateToIso(
        (jobPosting && jobPosting.validThrough) ||
          findValueByLabels($, ["closing date", "deadline", "valid through"])
      ),
      html_description: htmlDescription,
      plain_text_description: plainTextDescription,
      url: normalizeText(jobUrl),
      application_link: applicationLink,
      company_name: club.name,
      company_url: club.company_url || club.source_url || "",
      company_logo_url: club.company_logo_url || "",
      source_url: club.source_url || "",
      ats: "successfactors",
      _jobPosting: jobPosting || undefined,
    };
  });
}

module.exports = {
  discoverJobUrls,
  fetchJob,
  createSession,
};
