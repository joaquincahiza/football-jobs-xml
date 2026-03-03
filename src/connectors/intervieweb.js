const cheerio = require("cheerio");
const {
  MAX_DISCOVERY_PAGES,
  withPage,
  gotoWithRetry,
  canonicalizeUrl,
  normalizeText,
  parseDateToIso,
  htmlToStructuredPlainText,
  findValueByLabels,
  mapArrangementFromEmploymentType,
  extractSourceIdFromUrl,
  extractJobPostingJsonLd,
  extractLocationFromJsonLd,
  normalizeHtmlFragment,
  createSession,
} = require("./utils");

const APPLY_TEXT_REGEX = /(apply|application|apply now|invia candidatura|candidati|candidatura)/i;
const INVALID_USER_REGEX = /invalid user|publishing key not set/i;
const ID_PARAM_REGEX = /(?:\?|&|#|;|\\u0026|&amp;)IdAnnuncio=([0-9]+)/gi;
const JOB_SLUG_LINK_REGEX = /\/jobs\/([a-z0-9-]*?-(\d{3,}))(?:\/[a-z-]+)?\/?/gi;
const JOB_SLUG_PATH_REGEX = /\/jobs\/([^/?#]+?)(?:\/[a-z-]+)?\/?$/i;
const URL_FOR_ANNOUNCES_SELECTOR = "#url-for-announces";
const JOB_CACHE = new Map();

function cacheKey(club, sourceId) {
  return `${club.club_id}::${sourceId}`;
}

function resolveClubLang(club) {
  const lang = normalizeText(club && club.lang);
  return lang || "en";
}

function setLangParam(url, lang) {
  if (!url) {
    return "";
  }

  try {
    const parsed = new URL(url);
    parsed.searchParams.set("lang", lang);
    return parsed.href;
  } catch {
    return url;
  }
}

function decodeHtmlEntities(value) {
  return String(value || "")
    .replace(/&amp;/gi, "&")
    .replace(/&#x2F;/gi, "/")
    .replace(/&#47;/gi, "/");
}

function extractSlugFromJobUrl(value) {
  const source = String(value || "");
  if (!source) {
    return "";
  }

  try {
    const parsed = new URL(source);
    const match = parsed.pathname.match(JOB_SLUG_PATH_REGEX);
    return match ? normalizeText(match[1]) : "";
  } catch {
    const match = source.match(/\/jobs\/([^/?#]+?)(?:\/[a-z-]+)?\/?$/i);
    return match ? normalizeText(match[1]) : "";
  }
}

function extractAnnouncementId(value) {
  const source = String(value || "");
  if (!source) {
    return "";
  }

  try {
    const parsed = new URL(source);
    const id = normalizeText(
      parsed.searchParams.get("IdAnnuncio") ||
        parsed.searchParams.get("idannuncio") ||
        parsed.searchParams.get("idAnnuncio") ||
        ""
    );

    if (/^\d+$/.test(id)) {
      return id;
    }

    const slugMatch = parsed.pathname.match(JOB_SLUG_PATH_REGEX);
    const slug = slugMatch ? slugMatch[1] : "";
    const slugId = slug.match(/-(\d{3,})$/);
    if (slugId) {
      return slugId[1];
    }
  } catch {
    // Ignore URL parsing errors.
  }

  const queryMatch = source.match(/IdAnnuncio=([0-9]+)/i);
  if (queryMatch) {
    return queryMatch[1];
  }

  const slugId = source.match(/-([0-9]{3,})(?:\/|$)/);
  return slugId ? slugId[1] : "";
}

function buildCanonicalJobUrl(club, sourceId) {
  const lang = resolveClubLang(club);
  let origin = "https://inter.intervieweb.it";

  try {
    origin = new URL(club.source_url).origin;
  } catch {
    // Keep fallback origin.
  }

  const url = new URL("/app.php", origin);
  url.searchParams.set("IdAnnuncio", String(sourceId));
  url.searchParams.set("module", "iframeAnnunci");
  url.searchParams.set("opmode", "guest");
  url.searchParams.set("typeView", "large");
  url.searchParams.set("view", "list");
  url.searchParams.set("lang", lang);
  return url.href;
}

function buildFallbackListUrl(club) {
  const lang = resolveClubLang(club);
  let origin = "https://inter.intervieweb.it";

  try {
    origin = new URL(club.source_url).origin;
  } catch {
    // Keep fallback origin.
  }

  const url = new URL("/app.php", origin);
  url.searchParams.set("module", "iframeAnnunci");
  url.searchParams.set("opmode", "guest");
  url.searchParams.set("typeView", "large");
  url.searchParams.set("view", "list");
  url.searchParams.set("lang", lang);
  return url.href;
}

function buildAnnuncioDetailUrl(club, slug) {
  const lang = resolveClubLang(club);
  let origin = "https://inter.intervieweb.it";

  try {
    origin = new URL(club.source_url).origin;
  } catch {
    // Keep fallback origin.
  }

  const url = new URL("/app.php", origin);
  url.searchParams.set("module", "annunci");
  url.searchParams.set("l", slug);
  url.searchParams.set("lang", lang);
  return url.href;
}

function buildPublicJobUrl(club, slug) {
  const lang = resolveClubLang(club);
  let origin = "https://inter.intervieweb.it";

  try {
    origin = new URL(club.source_url).origin;
  } catch {
    // Keep fallback origin.
  }

  return `${origin}/jobs/${slug}/${lang}/`;
}

function cacheSlug(club, sourceId, slug) {
  const id = normalizeText(sourceId);
  const normalizedSlug = normalizeText(slug);

  if (!id || !normalizedSlug) {
    return;
  }

  JOB_CACHE.set(cacheKey(club, id), normalizedSlug);
}

function collectSectionIdsFromHtml(html, state) {
  for (const match of String(html || "").matchAll(/['"]section['"]\s*:\s*['"]([^'"]+)['"]/gi)) {
    const section = normalizeText(match[1]);
    if (section) {
      state.sectionIds.add(section);
    }
  }
}

function collectIdsFromText(text, club, state) {
  const source = String(text || "");

  for (const match of source.matchAll(ID_PARAM_REGEX)) {
    const id = normalizeText(match[1]);
    if (/^\d+$/.test(id)) {
      state.ids.add(id);
    }
  }

  for (const match of source.matchAll(JOB_SLUG_LINK_REGEX)) {
    const slug = normalizeText(match[1]);
    const id = normalizeText(match[2]);

    if (/^\d+$/.test(id)) {
      state.ids.add(id);
      cacheSlug(club, id, slug);
    }
  }
}

function collectFromHtml(baseUrl, html, club, state) {
  collectIdsFromText(html, club, state);
  collectSectionIdsFromHtml(html, state);

  const $ = cheerio.load(String(html || ""));

  const listEndpointValue = decodeHtmlEntities($(URL_FOR_ANNOUNCES_SELECTOR).val() || "");
  const listEndpointUrl = canonicalizeUrl(baseUrl, listEndpointValue);
  if (listEndpointUrl) {
    state.listApiUrls.add(listEndpointUrl);
  }

  $("input[name*='IdAnnuncio'],input[id*='IdAnnuncio'],input[name*='idannuncio'],input[id*='idannuncio']").each(
    (_, el) => {
      const id = normalizeText($(el).val() || $(el).attr("value") || "");
      if (/^\d+$/.test(id)) {
        state.ids.add(id);
      }
    }
  );

  $("a[href],iframe[src],frame[src],[data-src],[data-href],form[action]").each(
    (_, el) => {
      const raw =
        $(el).attr("href") ||
        $(el).attr("src") ||
        $(el).attr("data-src") ||
        $(el).attr("data-href") ||
        $(el).attr("action");

      const absolute = canonicalizeUrl(baseUrl, decodeHtmlEntities(raw));
      if (!absolute) {
        return;
      }

      const id = extractAnnouncementId(absolute);
      if (id) {
        state.ids.add(id);
      }

      const slug = extractSlugFromJobUrl(absolute);
      if (slug) {
        const slugId = extractAnnouncementId(slug);
        if (slugId) {
          state.ids.add(slugId);
          cacheSlug(club, slugId, slug);
          state.detailUrls.add(buildPublicJobUrl(club, slug));
        }
      }

      const lower = absolute.toLowerCase();
      if (
        lower.includes("app.php") &&
        (lower.includes("module=iframeannunci") ||
          lower.includes("module=newcareer") ||
          lower.includes("module=career"))
      ) {
        state.listUrls.add(absolute);
      }
    }
  );
}

function parseAjaxResponseText(responseText) {
  const raw = String(responseText || "").trim();
  if (!raw) {
    return { success: false, dataHtml: "" };
  }

  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object") {
      const success = parsed.success !== false;
      const dataHtml =
        typeof parsed.data === "string"
          ? parsed.data
          : typeof parsed.message === "string"
            ? parsed.message
            : "";
      return { success, dataHtml };
    }
  } catch {
    // Not JSON, continue.
  }

  return { success: true, dataHtml: raw };
}

async function crawlListApi(page, apiUrl, sectionId, club, state) {
  const lang = resolveClubLang(club);
  const endpoint = setLangParam(apiUrl, lang);
  let stablePages = 0;

  for (let pageNumber = 1; pageNumber <= MAX_DISCOVERY_PAGES; pageNumber += 1) {
    const beforeCount = state.ids.size;
    let responseText = "";

    try {
      const response = await page.request.post(endpoint, {
        form: {
          act1: "vacancyListCareer",
          section: sectionId || "",
          page: String(pageNumber),
          order: "date",
        },
        timeout: 45000,
      });

      responseText = await response.text();
      await page.waitForTimeout(1000);
    } catch {
      break;
    }

    const parsed = parseAjaxResponseText(responseText);
    if (!parsed.success || !parsed.dataHtml) {
      break;
    }

    collectFromHtml(endpoint, parsed.dataHtml, club, state);

    if (state.ids.size === beforeCount) {
      stablePages += 1;
    } else {
      stablePages = 0;
    }

    if (stablePages >= 2) {
      break;
    }
  }
}

async function discoverJobUrls(club, options = {}) {
  return withPage(options, async (page) => {
    const lang = resolveClubLang(club);
    const state = {
      ids: new Set(),
      listUrls: new Set(),
      listApiUrls: new Set(),
      detailUrls: new Set(),
      sectionIds: new Set(),
    };

    const startUrl = setLangParam(club.source_url, lang);
    await gotoWithRetry(page, startUrl);
    collectFromHtml(startUrl, await page.content(), club, state);

    if (!state.listUrls.size) {
      state.listUrls.add(buildFallbackListUrl(club));
    }

    let listPageVisited = 0;

    for (const listUrl of Array.from(state.listUrls)) {
      if (listPageVisited >= MAX_DISCOVERY_PAGES) {
        break;
      }

      await gotoWithRetry(page, setLangParam(listUrl, lang));
      collectFromHtml(listUrl, await page.content(), club, state);
      listPageVisited += 1;
    }

    const sectionId = Array.from(state.sectionIds)[0] || "";

    for (const apiUrl of Array.from(state.listApiUrls)) {
      await crawlListApi(page, apiUrl, sectionId, club, state);
    }

    const canonicalUrls = Array.from(state.ids)
      .filter((id) => /^\d+$/.test(String(id)))
      .sort((a, b) => Number(a) - Number(b))
      .map((id) => buildCanonicalJobUrl(club, id));

    return canonicalUrls;
  });
}

function extractLocationFallback($) {
  const fromLabel = findValueByLabels($, [
    "location",
    "sede",
    "sede di lavoro",
    "luogo di lavoro",
    "job location",
  ]);

  if (fromLabel) {
    return fromLabel;
  }

  const selectors = [
    "#description__subtitle .subtitle__informations",
    '[class*="location"]',
    '[data-testid*="location"]',
  ];

  for (const selector of selectors) {
    const value = normalizeText($(selector).first().text());
    if (value) {
      return value;
    }
  }

  return "";
}

function extractEmploymentType($) {
  return findValueByLabels($, [
    "employment type",
    "contract type",
    "tipologia contratto",
    "tipologia",
    "job type",
  ]);
}

function extractDescriptionHtml($, jobPosting) {
  const fromJsonLd = normalizeHtmlFragment(jobPosting && jobPosting.description);
  if (fromJsonLd) {
    return fromJsonLd;
  }

  const selectors = [
    "#description__description",
    ".vacancy__description",
    ".vacancy-description",
    ".job-description",
    ".description",
    "article",
    "main",
  ];

  for (const selector of selectors) {
    const node = $(selector).first();
    if (!node.length) {
      continue;
    }

    const text = normalizeText(node.text());
    if (text.length < 40) {
      continue;
    }

    const html = String(node.html() || "").trim();
    if (html) {
      return html;
    }
  }

  return String($("body").first().html() || "").trim();
}

function decodeBase64Url(value) {
  const raw = normalizeText(value);
  if (!raw) {
    return "";
  }

  try {
    const decoded = Buffer.from(raw, "base64").toString("utf8");
    return normalizeText(decoded);
  } catch {
    return "";
  }
}

function extractApplicationLink($, currentUrl, lang) {
  let link = "";

  $("a[href],button[data-href],button[onclick]").each((_, el) => {
    if (link) {
      return;
    }

    const text = normalizeText($(el).text());
    if (!APPLY_TEXT_REGEX.test(text)) {
      return;
    }

    const raw =
      $(el).attr("href") ||
      $(el).attr("data-href") ||
      $(el).attr("onclick") ||
      "";
    const match = String(raw).match(/https?:\/\/[^'")\s]+|\/[^'")\s]+/i);
    const href = match ? match[0] : raw;
    const absolute = canonicalizeUrl(currentUrl, decodeHtmlEntities(href));

    if (absolute) {
      link = absolute;
    }
  });

  if (link) {
    return setLangParam(link, lang);
  }

  const fwValue = $("input[name='FW']").attr("value");
  const decodedFw = decodeBase64Url(fwValue);
  if (decodedFw) {
    const absolute = canonicalizeUrl(currentUrl, decodedFw);
    if (absolute) {
      return setLangParam(absolute, lang);
    }
  }

  return setLangParam(currentUrl, lang);
}

async function resolveSlugForId(page, club, sourceId) {
  const cached = JOB_CACHE.get(cacheKey(club, sourceId));
  if (cached) {
    return cached;
  }

  const lang = resolveClubLang(club);
  const startUrl = setLangParam(club.source_url, lang);
  await gotoWithRetry(page, startUrl);

  const html = await page.content();
  const state = {
    ids: new Set(),
    listUrls: new Set(),
    listApiUrls: new Set(),
    detailUrls: new Set(),
    sectionIds: new Set(),
  };
  collectFromHtml(startUrl, html, club, state);

  const slug = JOB_CACHE.get(cacheKey(club, sourceId));
  return slug || "";
}

async function openValidDetailPage(page, club, sourceId, canonicalUrl) {
  const lang = resolveClubLang(club);
  const candidates = [];

  if (canonicalUrl) {
    candidates.push(setLangParam(canonicalUrl, lang));
  }

  const cachedSlug = JOB_CACHE.get(cacheKey(club, sourceId));
  if (cachedSlug) {
    candidates.push(buildAnnuncioDetailUrl(club, cachedSlug));
    candidates.push(buildPublicJobUrl(club, cachedSlug));
  }

  if (!cachedSlug) {
    const resolved = await resolveSlugForId(page, club, sourceId);
    if (resolved) {
      candidates.push(buildAnnuncioDetailUrl(club, resolved));
      candidates.push(buildPublicJobUrl(club, resolved));
    }
  }

  const seen = new Set();

  for (const candidate of candidates) {
    const url = normalizeText(candidate);
    if (!url || seen.has(url)) {
      continue;
    }
    seen.add(url);

    await gotoWithRetry(page, url);
    const html = await page.content();
    if (INVALID_USER_REGEX.test(html)) {
      continue;
    }

    const $ = cheerio.load(html);
    if (
      $("#description__vacancy-title").length ||
      $("input[name='input__IdAnnuncio'],input[id='input__IdAnnuncio']").length ||
      extractJobPostingJsonLd($)
    ) {
      return { url, html, $ };
    }
  }

  throw new Error(`No se pudo abrir detalle Intervieweb para IdAnnuncio=${sourceId}`);
}

async function fetchJob(club, jobUrl, options = {}) {
  return withPage(options, async (page) => {
    const firstSeenAt = new Date().toISOString();
    let sourceId = extractAnnouncementId(jobUrl);

    if (!sourceId) {
      sourceId = extractSourceIdFromUrl(jobUrl);
    }

    if (!sourceId) {
      throw new Error(`No se pudo resolver source_id desde ${jobUrl}`);
    }

    const canonicalUrl = buildCanonicalJobUrl(club, sourceId);
    const detail = await openValidDetailPage(page, club, sourceId, canonicalUrl);
    const { url: detailUrl, $ } = detail;

    const jobPosting = extractJobPostingJsonLd($);
    const title =
      normalizeText((jobPosting && (jobPosting.title || jobPosting.name)) || "") ||
      normalizeText($("#description__vacancy-title").first().text()) ||
      normalizeText($("h1").first().text()) ||
      normalizeText($("h2").first().text()) ||
      `job-${sourceId}`;
    const location =
      extractLocationFromJsonLd(jobPosting) || extractLocationFallback($);
    const employmentType = extractEmploymentType($);
    const publishedAt =
      parseDateToIso(
        (jobPosting && (jobPosting.datePosted || jobPosting.dateCreated)) ||
          findValueByLabels($, [
            "published on",
            "posted on",
            "publication date",
            "date",
            "data pubblicazione",
            "pubblicato il",
          ])
      ) || firstSeenAt;
    const expiresAt = parseDateToIso(
      (jobPosting && jobPosting.validThrough) ||
        findValueByLabels($, [
          "closing date",
          "application deadline",
          "valid through",
          "expires",
          "scadenza",
        ])
    );
    const htmlDescription = extractDescriptionHtml($, jobPosting);
    const plainTextDescription = htmlToStructuredPlainText(htmlDescription);
    const applicationLink = extractApplicationLink(
      $,
      detailUrl,
      resolveClubLang(club)
    );

    const hiddenId = normalizeText(
      $("input[name='input__IdAnnuncio'],input[id='input__IdAnnuncio']")
        .first()
        .val() || ""
    );
    if (/^\d+$/.test(hiddenId)) {
      sourceId = hiddenId;
    }

    return {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      url: buildCanonicalJobUrl(club, sourceId),
      application_link: applicationLink || canonicalUrl,
      title,
      arrangement: mapArrangementFromEmploymentType(employmentType),
      location_type: "onsite",
      location,
      published_at: publishedAt,
      expires_at: expiresAt,
      highlighted: false,
      sticky: false,
      html_description: htmlDescription,
      plain_text_description: plainTextDescription,
      company_name: "Inter Milan",
      company_url: "https://www.inter.it/",
      company_logo_url: club.company_logo_url || "",
      _jobPosting: jobPosting || undefined,
      _meta: {
        first_seen_at: firstSeenAt,
        detail_url: detailUrl,
      },
    };
  });
}

module.exports = {
  discoverJobUrls,
  fetchJob,
  createSession,
};
