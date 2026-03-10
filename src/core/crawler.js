const tribepad = require("../connectors/tribepad");
const custom = require("../connectors/custom");
const teamtailor = require("../connectors/teamtailor");
const intervieweb = require("../connectors/intervieweb");
const workforceready = require("../connectors/workforceready");
const talos = require("../connectors/talos");
const pinpoint = require("../connectors/pinpoint");
const hibob = require("../connectors/hibob");
const { normalizeJobRecord } = require("../connectors/utils");
const { upsertJobs } = require("../storage/fileStore");

const BLOCKED_NATIVE_ATS = new Set([
  "workday",
  "greenhouse",
  "smartrecruiters",
  "workable",
  "jazzhr",
  "bamboohr",
  "level",
  "trakstar",
]);

const CONNECTORS = {
  tribepad,
  custom,
  teamtailor,
  intervieweb,
  workforceready,
  talos,
  pinpoint,
  hibob,
};

const LANDING_PAGE_TITLES = new Set([
  "careers",
  "jobs",
  "jobs and careers",
  "permanent and temporary opportunities",
]);

function normalizeSourceType(sourceType) {
  return String(sourceType || "").trim().toLowerCase();
}

function isBlockedSourceType(sourceType) {
  return BLOCKED_NATIVE_ATS.has(normalizeSourceType(sourceType));
}

function normalizeComparableUrl(value) {
  try {
    const parsed = new URL(String(value || ""));
    parsed.hash = "";

    if (parsed.pathname.length > 1) {
      parsed.pathname = parsed.pathname.replace(/\/+$/, "");
    }

    return parsed.href;
  } catch {
    return String(value || "").trim();
  }
}

function normalizeLandingTitle(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function shouldSkipLandingPage(club, job) {
  if (!job || !job.source_id) {
    return true;
  }

  const sourceType = normalizeSourceType(club && club.source_type);
  const normalizedJobUrl = normalizeComparableUrl(job.url);
  const normalizedSourceUrl = normalizeComparableUrl(club && club.source_url);

  if (
    sourceType !== "workforceready" &&
    normalizedJobUrl &&
    normalizedSourceUrl &&
    normalizedJobUrl === normalizedSourceUrl
  ) {
    return true;
  }

  const normalizedTitle = normalizeLandingTitle(job.title);
  if (LANDING_PAGE_TITLES.has(normalizedTitle)) {
    return true;
  }

  return false;
}

function logLandingPageSkip(club, job, fallbackUrl) {
  const title = String((job && job.title) || "").trim();
  const url = String((job && job.url) || fallbackUrl || "").trim();
  console.log(`[skip] landing page: ${club.club_id} ${title} ${url}`.trim());
}

async function crawlClub(club) {
  const sourceType = normalizeSourceType(club.source_type);

  if (isBlockedSourceType(sourceType)) {
    console.warn(
      `[skip] ${club.club_id}: source_type "${club.source_type}" bloqueado por integración nativa`
    );
    return [];
  }

  const connector = CONNECTORS[sourceType];
  if (!connector) {
    console.warn(
      `[skip] ${club.club_id}: source_type "${club.source_type}" sin conector implementado`
    );
    return [];
  }

  let session;

  try {
    if (typeof connector.createSession === "function") {
      session = await connector.createSession();
    }

    let jobUrls = [];
    try {
      jobUrls = await connector.discoverJobUrls(club, {
        page: session && session.page ? session.page : undefined,
      });
    } catch (error) {
      console.error(
        `[warn] ${club.club_id}: fallo en discoverJobUrls -> ${error.message}`
      );
      return [];
    }

    const uniqueJobUrls = Array.from(new Set(jobUrls));
    console.log(
      `[crawl] ${club.club_id}: encontrados ${uniqueJobUrls.length} job urls`
    );
    const jobs = [];

    for (const jobUrl of uniqueJobUrls) {
      try {
        const rawJob = await connector.fetchJob(club, jobUrl, {
          page: session && session.page ? session.page : undefined,
        });
        const job = normalizeJobRecord(club, rawJob);

        if (!job) {
          logLandingPageSkip(club, rawJob, jobUrl);
          continue;
        }

        if (shouldSkipLandingPage(club, job)) {
          logLandingPageSkip(club, job, jobUrl);
          continue;
        }

        jobs.push(job);
      } catch (error) {
        console.error(
          `[warn] ${club.club_id}: fallo al procesar ${jobUrl} -> ${error.message}`
        );
      }
    }

    return jobs;
  } finally {
    if (session && typeof session.close === "function") {
      await session.close();
    }
  }
}

async function crawlClubs(clubs) {
  const collected = [];

  for (const club of clubs) {
    if (!club || !club.club_id || !club.source_url) {
      continue;
    }

    console.log(`[crawl] ${club.club_id} (${club.source_type})`);
    const jobs = await crawlClub(club);
    console.log(`[crawl] ${club.club_id}: ${jobs.length} jobs`);
    collected.push(...jobs);
  }

  const storedJobs = await upsertJobs(collected);
  return { scrapedJobs: collected, storedJobs };
}

module.exports = {
  crawlClubs,
  BLOCKED_NATIVE_ATS,
};
