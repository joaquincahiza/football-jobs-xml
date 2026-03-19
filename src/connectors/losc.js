const cheerio = require("cheerio");
const {
  RATE_LIMIT_MS,
  RETRIES,
  normalizeText,
  slugify,
  canonicalizeUrl,
  htmlToStructuredPlainText,
  escapeHtml,
} = require("./utils");

const JOB_CACHE = new Map();
const URL_TO_SOURCE_ID = new Map();

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

function extractLinkedInActivityId(url) {
  const match = normalizeText(url).match(/activity-(\d{6,})/i);
  return match ? match[1] : "";
}

function extractPdfSourceId(url) {
  try {
    const parsed = new URL(url);
    const filename = decodeURIComponent(parsed.pathname.split("/").pop() || "");
    const cleaned = filename.replace(/\.pdf$/i, "");
    return slugify(cleaned);
  } catch {
    return "";
  }
}

function mapArrangementFromTitle(title) {
  const normalized = normalizeText(title).toLowerCase();

  if (
    normalized.includes("stage") ||
    normalized.includes("intern") ||
    normalized.includes("alternance") ||
    normalized.includes("cdd")
  ) {
    return "contract";
  }

  if (normalized.includes("part-time") || normalized.includes("part time")) {
    return "parttime";
  }

  return "fulltime";
}

function parseLoscEntries(club, html) {
  const $ = cheerio.load(String(html || ""));
  const entries = [];

  $(".is-twoCols").each((index, section) => {
    const title = normalizeText(
      $(section).find("div").first().find("p").first().text()
    );
    const href = normalizeText(
      $(section).find("div").last().find("a[href]").first().attr("href")
    );
    const url = canonicalizeUrl(club.source_url, href) || href;

    if (!title || !url) {
      return;
    }

    const sourceId =
      extractLinkedInActivityId(url) ||
      extractPdfSourceId(url) ||
      slugify(title) ||
      `losc-${index + 1}`;

    const htmlDescription = `<p>${escapeHtml(title)}</p>`;

    entries.push({
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      title,
      department: "",
      location: "Lille",
      location_type: "onsite",
      arrangement: mapArrangementFromTitle(title),
      employment_type: "",
      html_description: htmlDescription,
      plain_text_description: htmlToStructuredPlainText(htmlDescription),
      published_at: "",
      expires_at: "",
      url,
      application_link: url,
      company_name: club.name,
      company_url: club.company_url || club.source_url || "",
      company_logo_url: club.company_logo_url || "",
      source_url: club.source_url || "",
      ats: "custom",
    });
  });

  return entries;
}

function extractSourceIdFromUrl(club, jobUrl) {
  return normalizeText(URL_TO_SOURCE_ID.get(buildUrlKey(club, jobUrl)) || "");
}

async function discoverJobUrls(club) {
  let html;

  try {
    html = await requestTextWithRetry(club, club.source_url, {
      method: "GET",
      headers: {
        Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "User-Agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      },
    });
  } catch (error) {
    console.warn(`[warn] ${club.club_id}: losc discover falló -> ${error.message}`);
    return [];
  }

  const entries = parseLoscEntries(club, html);
  const urls = [];

  for (const entry of entries) {
    JOB_CACHE.set(buildCacheKey(club, entry.source_id), entry);
    URL_TO_SOURCE_ID.set(buildUrlKey(club, entry.url), entry.source_id);
    urls.push(entry.url);
  }

  return Array.from(new Set(urls));
}

async function fetchJob(club, jobUrl) {
  const normalizedUrl = normalizeText(jobUrl);
  const sourceId = extractSourceIdFromUrl(club, normalizedUrl);
  let cached = sourceId ? JOB_CACHE.get(buildCacheKey(club, sourceId)) : null;

  if (!cached) {
    await discoverJobUrls(club);
    const refreshedSourceId = extractSourceIdFromUrl(club, normalizedUrl);
    cached = refreshedSourceId
      ? JOB_CACHE.get(buildCacheKey(club, refreshedSourceId))
      : null;
  }

  if (!cached) {
    throw new Error(`No se pudo resolver job LOSC para ${jobUrl}`);
  }

  return {
    ...cached,
    url: normalizedUrl || cached.url,
    application_link: normalizedUrl || cached.application_link,
  };
}

module.exports = {
  discoverJobUrls,
  fetchJob,
};
