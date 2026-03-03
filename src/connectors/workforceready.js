const {
  RATE_LIMIT_MS,
  RETRIES,
  normalizeText,
  parseDateToIso,
  htmlToStructuredPlainText,
  normalizeHtmlFragment,
  escapeHtml,
  canonicalizeUrl,
} = require("./utils");

const DEFAULT_PAGE_SIZE = 20;
const MAX_PAGES = 80;
const REQUISITION_CACHE = new Map();
const CLUB_STATE_CACHE = new Map();

function logHttp(club, message) {
  const clubId = normalizeText(club && club.club_id) || "unknown";
  console.log(`[http] ${clubId} ${message}`);
}

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

function resolveLang(club) {
  const configured = normalizeText(club && club.lang);
  if (configured) {
    return configured;
  }

  const parsed = parseUrlSafe(club && club.source_url);
  if (!parsed) {
    return "en-GB";
  }

  return normalizeText(parsed.searchParams.get("lang")) || "en-GB";
}

function resolvePageSize(club) {
  const configured = Number(club && club.page_size);
  if (Number.isFinite(configured) && configured > 0) {
    return Math.floor(configured);
  }

  return DEFAULT_PAGE_SIZE;
}

function resolveCompanyId(club) {
  const configured = normalizeText(club && club.workforce_company_id);
  if (configured) {
    return configured;
  }

  const parsed = parseUrlSafe(club && club.source_url);
  if (!parsed) {
    return "";
  }

  const match = parsed.pathname.match(/\/(\d+)\.careers\/?$/i);
  return match ? normalizeText(match[1]) : "";
}

function buildApiBase(club) {
  const parsed = parseUrlSafe(club && club.source_url);
  if (!parsed) {
    throw new Error("source_url inválida para workforceready");
  }

  const companyId = resolveCompanyId(club);
  if (!companyId) {
    throw new Error("workforce_company_id faltante para workforceready");
  }

  return `${parsed.origin}/ta/rest/ui/recruitment/companies/%7C${companyId}/job-requisitions`;
}

function buildApiUrl(apiBase, lang, offset, size) {
  const parsed = parseUrlSafe(apiBase);
  if (!parsed) {
    throw new Error("API base inválida para workforceready");
  }

  parsed.search = "";
  parsed.searchParams.set("offset", String(offset));
  parsed.searchParams.set("size", String(size));
  parsed.searchParams.set("sort", "desc");
  parsed.searchParams.set("ein_id", "");
  parsed.searchParams.set("lang", lang);
  return parsed.href;
}

function getFirstExisting(source, paths) {
  for (const path of paths) {
    const parts = path.split(".");
    let cursor = source;
    let ok = true;

    for (const part of parts) {
      if (!cursor || typeof cursor !== "object" || !(part in cursor)) {
        ok = false;
        break;
      }

      cursor = cursor[part];
    }

    if (ok && cursor !== undefined && cursor !== null) {
      return cursor;
    }
  }

  return undefined;
}

function extractRequisitionList(payload) {
  if (!payload) {
    return [];
  }

  const direct = getFirstExisting(payload, [
    "job_requisitions",
    "requisitions",
    "items",
    "data",
    "results",
  ]);

  if (Array.isArray(direct)) {
    return direct;
  }

  if (Array.isArray(payload)) {
    return payload;
  }

  return [];
}

function extractPagingTotal(payload, collectedCount) {
  const candidates = [
    getFirstExisting(payload, ["_paging.total"]),
    getFirstExisting(payload, ["paging.total"]),
    getFirstExisting(payload, ["total"]),
    getFirstExisting(payload, ["count"]),
  ];

  for (const candidate of candidates) {
    const value = Number(candidate);
    if (Number.isFinite(value) && value >= 0) {
      return value;
    }
  }

  return collectedCount;
}

function extractCookieHeader(setCookieRaw) {
  const source = String(setCookieRaw || "");
  if (!source) {
    return "";
  }

  const pairs = source.match(/(?:^|,\s*)([^=;,\s]+=[^;,\s]+)/g) || [];
  const normalized = pairs
    .map((pair) => pair.replace(/^,\s*/, "").trim())
    .filter(Boolean);

  return Array.from(new Set(normalized)).join("; ");
}

function buildBaseHeaders(club, cookieHeader) {
  const headers = {
    Accept: "application/json, text/plain, */*",
    "User-Agent":
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    Referer: normalizeText(club && club.source_url),
  };

  if (cookieHeader) {
    headers.Cookie = cookieHeader;
  }

  return headers;
}

async function buildSessionHeaders(club) {
  let cookieHeader = "";

  try {
    const sourceUrl = normalizeText(club && club.source_url);
    if (sourceUrl) {
      logHttp(club, `GET ${sourceUrl}`);
      const response = await fetch(sourceUrl, {
        method: "GET",
        headers: {
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "User-Agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        },
      });
      logHttp(club, `${response.status} ${sourceUrl}`);

      const setCookie = response.headers.get("set-cookie");
      cookieHeader = extractCookieHeader(setCookie);
      await sleep(RATE_LIMIT_MS);
    }
  } catch (error) {
    logHttp(club, `ERROR ${club.source_url} -> ${error.message}`);
  }

  return buildBaseHeaders(club, cookieHeader);
}

async function fetchJsonWithRetry(club, url, headers) {
  let lastError;

  for (let attempt = 0; attempt <= RETRIES; attempt += 1) {
    try {
      logHttp(club, `GET ${url}`);
      const response = await fetch(url, {
        method: "GET",
        headers: headers || {},
      });
      logHttp(club, `${response.status} ${url}`);

      if (!response.ok) {
        const httpError = new Error(`HTTP ${response.status}`);
        httpError.status = response.status;
        throw httpError;
      }

      const text = await response.text();
      let payload;

      try {
        payload = JSON.parse(text);
      } catch {
        throw new Error("respuesta JSON inválida");
      }

      await sleep(RATE_LIMIT_MS);
      return payload;
    } catch (error) {
      lastError = error;
      logHttp(club, `ERROR ${url} -> ${error.message}`);
      if (attempt < RETRIES) {
        await sleep(RATE_LIMIT_MS);
      }
    }
  }

  throw lastError || new Error("error desconocido en request JSON");
}

function mapArrangement(employeeTypeName) {
  const normalized = normalizeText(employeeTypeName).toLowerCase();

  if (!normalized) {
    return "";
  }

  if (normalized.includes("part")) {
    return "parttime";
  }

  if (normalized.includes("full")) {
    return "fulltime";
  }

  if (
    normalized.includes("contract") ||
    normalized.includes("fixed") ||
    normalized.includes("temporary") ||
    normalized.includes("temp")
  ) {
    return "contract";
  }

  return "";
}

function buildLocation(requisition) {
  const city = normalizeText(
    getFirstExisting(requisition, [
      "location.city",
      "city",
      "location_city",
      "address.city",
      "job_location.city",
    ])
  );

  const country = normalizeText(
    getFirstExisting(requisition, [
      "location.country",
      "country",
      "location_country",
      "address.country",
      "job_location.country",
    ])
  );

  if (city && country) {
    return `${city}, ${country}`;
  }

  return (
    city ||
    country ||
    normalizeText(
      getFirstExisting(requisition, [
        "location_name",
        "location.label",
        "job_location.name",
      ])
    )
  );
}

function resolveLocationType(requisition) {
  const raw = getFirstExisting(requisition, [
    "is_remote_job",
    "location_type",
    "remote_type",
  ]);

  if (raw === true || raw === 1) {
    return "remote";
  }

  if (raw === false || raw === 0) {
    return "onsite";
  }

  const normalized = normalizeText(raw).toLowerCase();

  if (!normalized) {
    return "onsite";
  }

  if (normalized.includes("hybrid")) {
    return "hybrid";
  }

  if (normalized.includes("remote")) {
    return "remote";
  }

  return "onsite";
}

function buildHtmlDescription(requisition) {
  const rawDescription = normalizeHtmlFragment(
    getFirstExisting(requisition, ["job_description", "description", "summary"]) ||
      ""
  );

  if (!rawDescription) {
    return "";
  }

  if (/<[a-z][\s\S]*>/i.test(rawDescription)) {
    return rawDescription;
  }

  return `<p>${escapeHtml(rawDescription)}</p>`;
}

function normalizeCandidateUrl(club, value) {
  const text = normalizeText(value);
  if (!text) {
    return "";
  }

  const absolute = canonicalizeUrl(club.source_url, text);
  return normalizeText(absolute || text);
}

function resolveJobLinks(club, requisition) {
  const publicCandidates = [
    getFirstExisting(requisition, ["job_url_public"]),
    getFirstExisting(requisition, ["public_job_url"]),
    getFirstExisting(requisition, ["public_url"]),
    getFirstExisting(requisition, ["job_url"]),
    getFirstExisting(requisition, ["url"]),
    getFirstExisting(requisition, ["detail_url"]),
    getFirstExisting(requisition, ["career_url"]),
    getFirstExisting(requisition, ["_links.self.href"]),
  ]
    .map((value) => normalizeCandidateUrl(club, value))
    .filter(Boolean);

  const applyCandidates = [
    getFirstExisting(requisition, ["application_link"]),
    getFirstExisting(requisition, ["application_url"]),
    getFirstExisting(requisition, ["apply_url"]),
    getFirstExisting(requisition, ["apply_link"]),
    getFirstExisting(requisition, ["external_apply_url"]),
    getFirstExisting(requisition, ["_links.apply.href"]),
  ]
    .map((value) => normalizeCandidateUrl(club, value))
    .filter(Boolean);

  const sourceUrl = normalizeText(club.source_url || "");
  const url = publicCandidates[0] || sourceUrl;
  const applicationLink = applyCandidates[0] || publicCandidates[0] || sourceUrl;

  return {
    url,
    application_link: applicationLink,
  };
}

function normalizeRequisition(club, requisition, apiBase) {
  const sourceId = normalizeText(
    getFirstExisting(requisition, ["id", "requisition_id", "job_requisition_id"])
  );

  if (!sourceId) {
    return null;
  }

  const title = normalizeText(
    getFirstExisting(requisition, ["job_title", "title", "name"])
  );

  const employeeTypeName = normalizeText(
    getFirstExisting(requisition, [
      "employee_type.name",
      "employee_type",
      "employment_type.name",
      "employment_type",
      "job_type",
    ])
  );

  const htmlDescription = buildHtmlDescription(requisition);
  const plainTextDescription =
    htmlToStructuredPlainText(htmlDescription) ||
    normalizeText(
      getFirstExisting(requisition, ["job_description", "description", "summary"])
    );

  const links = resolveJobLinks(club, requisition);

  return {
    club_id: club.club_id,
    source_id: sourceId,
    id: sourceId,
    url: links.url,
    application_link: links.application_link,
    title,
    arrangement: mapArrangement(employeeTypeName),
    employment_type: employeeTypeName,
    location_type: resolveLocationType(requisition),
    location: buildLocation(requisition),
    published_at: parseDateToIso(
      getFirstExisting(requisition, [
        "date_posted",
        "posted_on",
        "published_at",
        "open_date",
        "created_at",
      ])
    ),
    expires_at: parseDateToIso(
      getFirstExisting(requisition, [
        "closing_date",
        "expires_at",
        "valid_through",
        "close_date",
      ])
    ),
    highlighted: false,
    sticky: false,
    html_description: htmlDescription,
    plain_text_description: plainTextDescription,
    company_name: club.name,
    company_url: club.company_url || "",
    company_logo_url: club.company_logo_url || "",
    _meta: {
      source_type: "workforceready",
      endpoint: apiBase || "",
    },
  };
}

async function loadRequisitions(club) {
  const apiBase = buildApiBase(club);
  const lang = resolveLang(club);
  const pageSize = resolvePageSize(club);
  const headers = await buildSessionHeaders(club);

  const requisitionMap = new Map();
  let offset = 1;

  for (let pageNumber = 1; pageNumber <= MAX_PAGES; pageNumber += 1) {
    const apiUrl = buildApiUrl(apiBase, lang, offset, pageSize);
    const payload = await fetchJsonWithRetry(club, apiUrl, headers);
    const items = extractRequisitionList(payload);

    for (const item of items) {
      const sourceId = normalizeText(
        getFirstExisting(item, ["id", "requisition_id", "job_requisition_id"])
      );

      if (!sourceId) {
        continue;
      }

      requisitionMap.set(sourceId, item);
    }

    const total = extractPagingTotal(payload, requisitionMap.size);
    const nextOffset = offset + pageSize;

    if (!items.length) {
      break;
    }

    if (Number.isFinite(total) && nextOffset > total) {
      break;
    }

    if (!Number.isFinite(total) && items.length < pageSize) {
      break;
    }

    offset = nextOffset;
  }

  CLUB_STATE_CACHE.set(normalizeText(club && club.club_id), {
    apiBase,
    lang,
    pageSize,
  });

  return {
    apiBase,
    requisitions: Array.from(requisitionMap.values()),
  };
}

function extractSourceIdFromJobUrl(jobUrl) {
  const parsed = parseUrlSafe(jobUrl);

  if (parsed) {
    const wfrHash = parsed.hash.match(/^#wfr-(.+)$/i);
    if (wfrHash) {
      return normalizeText(wfrHash[1]);
    }

    const jobHash = parsed.hash.match(/^#job-(.+)$/i);
    if (jobHash) {
      return normalizeText(jobHash[1]);
    }

    const queryId = normalizeText(
      parsed.searchParams.get("id") ||
        parsed.searchParams.get("job") ||
        parsed.searchParams.get("jobId") ||
        ""
    );

    if (queryId) {
      return queryId;
    }
  }

  const raw = String(jobUrl || "");
  const hashMatch = raw.match(/#wfr-([^&#]+)/i) || raw.match(/#job-([^&#]+)/i);
  if (hashMatch) {
    return normalizeText(hashMatch[1]);
  }

  return "";
}

async function discoverJobUrls(club) {
  let loaded;

  try {
    loaded = await loadRequisitions(club);
  } catch (error) {
    console.warn(`[warn] ${club.club_id}: workforceready discover falló -> ${error.message}`);
    return [];
  }

  const urls = [];
  const sourceUrl = normalizeText(club.source_url || "");

  for (const requisition of loaded.requisitions) {
    const normalized = normalizeRequisition(club, requisition, loaded.apiBase);
    if (!normalized || !normalized.source_id) {
      continue;
    }

    REQUISITION_CACHE.set(buildCacheKey(club, normalized.source_id), {
      requisition,
      apiBase: loaded.apiBase,
    });

    const jobUrl =
      normalized.url && normalized.url !== sourceUrl
        ? normalized.url
        : `${sourceUrl}#wfr-${normalized.source_id}`;

    if (jobUrl) {
      urls.push(jobUrl);
    }
  }

  return Array.from(new Set(urls));
}

async function fetchJob(club, jobUrl) {
  const sourceId = extractSourceIdFromJobUrl(jobUrl);

  if (!sourceId) {
    throw new Error(`No se pudo extraer source_id desde ${jobUrl}`);
  }

  const key = buildCacheKey(club, sourceId);
  let cached = REQUISITION_CACHE.get(key);

  if (!cached || !cached.requisition) {
    try {
      const loaded = await loadRequisitions(club);
      for (const requisition of loaded.requisitions) {
        const id = normalizeText(
          getFirstExisting(requisition, ["id", "requisition_id", "job_requisition_id"])
        );

        if (!id) {
          continue;
        }

        REQUISITION_CACHE.set(buildCacheKey(club, id), {
          requisition,
          apiBase: loaded.apiBase,
        });
      }

      cached = REQUISITION_CACHE.get(key);
    } catch (error) {
      console.warn(`[warn] ${club.club_id}: workforceready fetch falló -> ${error.message}`);
      return {
        club_id: club.club_id,
        source_id: sourceId,
        id: sourceId,
        url: normalizeText(club.source_url || ""),
        application_link: normalizeText(club.source_url || ""),
        title: "",
        arrangement: "",
        location_type: "onsite",
        location: "",
        published_at: "",
        expires_at: "",
        html_description: "",
        plain_text_description: "",
        company_name: club.name,
        company_url: club.company_url || "",
        company_logo_url: club.company_logo_url || "",
      };
    }
  }

  if (!cached || !cached.requisition) {
    return {
      club_id: club.club_id,
      source_id: sourceId,
      id: sourceId,
      url: normalizeText(club.source_url || ""),
      application_link: normalizeText(club.source_url || ""),
      title: "",
      arrangement: "",
      location_type: "onsite",
      location: "",
      published_at: "",
      expires_at: "",
      html_description: "",
      plain_text_description: "",
      company_name: club.name,
      company_url: club.company_url || "",
      company_logo_url: club.company_logo_url || "",
    };
  }

  const job = normalizeRequisition(club, cached.requisition, cached.apiBase || "");
  if (!job) {
    throw new Error(`Requisition inválida para source_id ${sourceId}`);
  }

  return job;
}

module.exports = {
  discoverJobUrls,
  fetchJob,
};
