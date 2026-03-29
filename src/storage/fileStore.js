const fs = require("fs/promises");
const path = require("path");
const {
  normalizeText,
  normalizeUrlForIdentity,
  buildStableJobId,
  buildStableGuid,
} = require("../connectors/utils");
const {
  isDisabledSourceEntry,
  findDisabledSourcePolicy,
} = require("../config/disabledSources");

const DATA_FILE = path.resolve(__dirname, "../../data/jobs.json");

function normalizeNameKey(value) {
  return normalizeText(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "");
}

function buildDedupeKey(job) {
  return `${normalizeText(job.club_id)}::${normalizeText(job.id || job.source_id)}`;
}

function sortJobs(jobs) {
  return [...jobs].sort((a, b) => {
    const keyA = buildDedupeKey(a);
    const keyB = buildDedupeKey(b);
    return keyA.localeCompare(keyB);
  });
}

function normalizeStoredJob(job) {
  if (!job || !job.club_id) {
    return null;
  }

  const url = normalizeText(job.url || "");
  const applicationLink = normalizeText(job.application_link || url);
  const sourceId =
    normalizeText(job.source_id) ||
    buildStableJobId(job.id, url, applicationLink);
  const id =
    normalizeText(job.id) || buildStableJobId(sourceId, url, applicationLink);

  if (!id) {
    return null;
  }

  const clubIdentity =
    normalizeText(job.source_url || "") ||
    normalizeText(job.company_url || "") ||
    normalizeText(job.club_id || "");

  return {
    ...job,
    club_id: normalizeText(job.club_id),
    club: normalizeText(job.club || job.company_name || ""),
    source_id: sourceId || id,
    id,
    guid: normalizeText(job.guid || buildStableGuid(clubIdentity, id)),
    url,
    application_link: applicationLink,
    title: normalizeText(job.title || ""),
    location_type: normalizeText(job.location_type || "onsite") || "onsite",
    location: normalizeText(job.location || ""),
    published_at: normalizeText(job.published_at || ""),
    expires_at: normalizeText(job.expires_at || ""),
    company_name: normalizeText(job.company_name || ""),
    company_url: normalizeText(job.company_url || ""),
    company_logo_url:
      job.company_logo_url !== undefined ? String(job.company_logo_url) : "",
    source_url: normalizeText(job.source_url || ""),
    ats: normalizeText(job.ats || ""),
  };
}

function buildComparableKeys(job) {
  const normalizedJob = normalizeStoredJob(job);
  if (!normalizedJob) {
    return [];
  }

  const keys = [];
  const sourceId = normalizeText(normalizedJob.source_id);
  const id = normalizeText(normalizedJob.id);
  const urlKey = normalizeUrlForIdentity(normalizedJob.url);
  const applicationKey = normalizeUrlForIdentity(normalizedJob.application_link);
  const sourceUrlKey = normalizeUrlForIdentity(normalizedJob.source_url);

  if (sourceId) {
    keys.push(`source:${sourceId}`);
  }
  if (id) {
    keys.push(`id:${id}`);
  }
  // Some ATSes reuse the listing URL for every job. Those URLs are not safe
  // identity aliases, so we only use detail/app URLs when they differ from the
  // club source_url.
  if (urlKey && urlKey !== sourceUrlKey) {
    keys.push(`url:${urlKey}`);
  }
  if (
    applicationKey &&
    applicationKey !== urlKey &&
    applicationKey !== sourceUrlKey
  ) {
    keys.push(`apply:${applicationKey}`);
  }

  return Array.from(new Set(keys));
}

function buildCleanupIdentity(job) {
  const keys = buildComparableKeys(job);
  return (
    keys.find((key) => key.startsWith("source:")) ||
    keys.find((key) => key.startsWith("url:")) ||
    keys.find((key) => key.startsWith("apply:")) ||
    keys.find((key) => key.startsWith("id:")) ||
    buildDedupeKey(job)
  );
}

function buildComparableIndex(jobs) {
  const index = new Map();

  for (const job of jobs) {
    for (const key of buildComparableKeys(job)) {
      index.set(key, job);
    }
  }

  return index;
}

function findComparableMatch(index, job) {
  const keys = buildComparableKeys(job);
  const priorities = [
    keys.find((key) => key.startsWith("source:")),
    keys.find((key) => key.startsWith("url:")),
    keys.find((key) => key.startsWith("apply:")),
    keys.find((key) => key.startsWith("id:")),
  ].filter(Boolean);

  for (const key of priorities) {
    if (index.has(key)) {
      return index.get(key);
    }
  }

  return null;
}

function createClubLineageMatcher(club) {
  const clubId = normalizeText(club && club.club_id);
  const clubSourceUrl = normalizeUrlForIdentity(club && club.source_url);
  const clubCompanyUrl = normalizeUrlForIdentity(club && club.company_url);
  const clubNameKey = normalizeNameKey(club && club.name);

  return (job) => {
    const normalizedJob = normalizeStoredJob(job);
    if (!normalizedJob) {
      return false;
    }

    if (clubId && normalizedJob.club_id === clubId) {
      return true;
    }

    if (
      clubSourceUrl &&
      normalizeUrlForIdentity(normalizedJob.source_url) === clubSourceUrl
    ) {
      return true;
    }

    if (
      clubCompanyUrl &&
      normalizeUrlForIdentity(normalizedJob.company_url) === clubCompanyUrl
    ) {
      const jobNameKeys = [
        normalizeNameKey(normalizedJob.company_name),
        normalizeNameKey(normalizedJob.club),
      ].filter(Boolean);

      if (!clubNameKey || !jobNameKeys.length) {
        return true;
      }

      return jobNameKeys.includes(clubNameKey);
    }

    return false;
  };
}

function scoreJobForClub(job, club) {
  const normalizedJob = normalizeStoredJob(job);
  if (!normalizedJob) {
    return Number.NEGATIVE_INFINITY;
  }

  let score = 0;

  if (normalizeText(normalizedJob.club_id) === normalizeText(club.club_id)) {
    score += 100;
  }

  if (
    normalizeUrlForIdentity(normalizedJob.source_url) &&
    normalizeUrlForIdentity(normalizedJob.source_url) ===
      normalizeUrlForIdentity(club.source_url)
  ) {
    score += 50;
  }

  if (
    normalizeUrlForIdentity(normalizedJob.company_url) &&
    normalizeUrlForIdentity(normalizedJob.company_url) ===
      normalizeUrlForIdentity(club.company_url)
  ) {
    score += 25;
  }

  if (
    normalizeNameKey(normalizedJob.company_name || normalizedJob.club) ===
    normalizeNameKey(club.name)
  ) {
    score += 10;
  }

  if (normalizeText(normalizedJob.guid)) {
    score += 5;
  }

  if (normalizeText(normalizedJob.published_at)) {
    score += 1;
  }

  return score;
}

function normalizeClubScopedJob(job, club, existingMatch) {
  const normalizedJob = normalizeStoredJob({
    ...job,
    club_id: normalizeText(club.club_id),
    club: normalizeText(job.club || club.name || ""),
    source_url: normalizeText(job.source_url || club.source_url || ""),
    company_url: normalizeText(job.company_url || club.company_url || ""),
    ats: normalizeText(job.ats || club.source_type || ""),
  });

  if (!normalizedJob) {
    return null;
  }

  const stableId =
    normalizeText(existingMatch && existingMatch.id) ||
    normalizeText(normalizedJob.id) ||
    buildStableJobId(
      normalizedJob.source_id,
      normalizedJob.url,
      normalizedJob.application_link
    );

  const clubIdentity =
    normalizeText(normalizedJob.source_url || "") ||
    normalizeText(normalizedJob.company_url || "") ||
    normalizeText(club.club_id || "");

  return normalizeStoredJob({
    ...normalizedJob,
    id: stableId,
    guid:
      normalizeText(existingMatch && existingMatch.guid) ||
      buildStableGuid(clubIdentity, stableId),
  });
}

function shouldReplaceClubLineage(clubResult, existingLineageJobs) {
  if (!clubResult || !clubResult.club) {
    return { replace: false, reason: "missing_result" };
  }

  if (!clubResult.sync_safe) {
    return {
      replace: false,
      reason: normalizeText(clubResult.sync_reason || "unsafe_sync"),
    };
  }

  // A successful-but-empty crawl over an already populated club can still be a
  // transient partial failure. Preserve the existing records unless we have
  // positive evidence from a non-empty replacement set.
  if (clubResult.jobs.length === 0 && existingLineageJobs.length > 0) {
    return {
      replace: false,
      reason: "empty_result_guard",
    };
  }

  return { replace: true, reason: "synced" };
}

function dedupeJobs(jobs) {
  const map = new Map();

  for (const job of jobs) {
    const normalizedJob = normalizeStoredJob(job);
    if (!normalizedJob) {
      continue;
    }

    map.set(buildDedupeKey(normalizedJob), normalizedJob);
  }

  return sortJobs(Array.from(map.values()));
}

function applyClubSync(existingJobs, clubResult) {
  const normalizedExistingJobs = dedupeJobs(existingJobs);
  const club = clubResult.club;
  const matcher = createClubLineageMatcher(club);
  const existingLineageJobs = normalizedExistingJobs.filter(matcher);
  const otherJobs = normalizedExistingJobs.filter((job) => !matcher(job));
  const decision = shouldReplaceClubLineage(clubResult, existingLineageJobs);

  if (!decision.replace) {
    return {
      jobs: normalizedExistingJobs,
      report: {
        club_id: normalizeText(club && club.club_id),
        replaced: false,
        reason: decision.reason,
        existing_jobs: existingLineageJobs.length,
        incoming_jobs: Array.isArray(clubResult.jobs) ? clubResult.jobs.length : 0,
      },
    };
  }

  const currentIndex = buildComparableIndex(existingLineageJobs);
  const incomingJobs = [];
  const incomingMap = new Map();

  for (const job of clubResult.jobs || []) {
    const existingMatch = findComparableMatch(currentIndex, job);
    const normalizedJob = normalizeClubScopedJob(job, club, existingMatch);

    if (!normalizedJob) {
      continue;
    }

    incomingMap.set(buildDedupeKey(normalizedJob), normalizedJob);
    incomingJobs.push(normalizedJob);

    for (const key of buildComparableKeys(normalizedJob)) {
      currentIndex.set(key, normalizedJob);
    }
  }

  const synchronizedJobs = dedupeJobs([
    ...otherJobs,
    ...Array.from(incomingMap.values()),
  ]);

  return {
    jobs: synchronizedJobs,
    report: {
      club_id: normalizeText(club && club.club_id),
      replaced: true,
      reason: decision.reason,
      existing_jobs: existingLineageJobs.length,
      incoming_jobs: Array.from(incomingMap.values()).length,
      removed_jobs:
        existingLineageJobs.length - Array.from(incomingMap.values()).length,
    },
  };
}

async function ensureStoreFile() {
  await fs.mkdir(path.dirname(DATA_FILE), { recursive: true });
  try {
    await fs.access(DATA_FILE);
  } catch {
    await fs.writeFile(DATA_FILE, "[]\n", "utf8");
  }
}

function filterDisabledSourceJobs(jobs) {
  const keptJobs = [];
  const removedJobs = [];
  const removedCounts = new Map();

  for (const job of Array.isArray(jobs) ? jobs : []) {
    const normalizedJob = normalizeStoredJob(job);
    if (!normalizedJob) {
      continue;
    }

    if (!isDisabledSourceEntry(normalizedJob)) {
      keptJobs.push(normalizedJob);
      continue;
    }

    const policy = findDisabledSourcePolicy(normalizedJob);
    const policyKey = normalizeText(policy && policy.key) || "disabled_source";
    removedCounts.set(policyKey, (removedCounts.get(policyKey) || 0) + 1);
    removedJobs.push(normalizedJob);
  }

  return {
    jobs: dedupeJobs(keptJobs),
    removed_jobs: removedJobs,
    reports: Array.from(removedCounts.entries()).map(([policyKey, count]) => ({
      policy_key: policyKey,
      removed_jobs: count,
    })),
  };
}

async function loadRawJobs() {
  await ensureStoreFile();

  try {
    const raw = await fs.readFile(DATA_FILE, "utf8");
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed)
      ? parsed.map((job) => normalizeStoredJob(job)).filter(Boolean)
      : [];
  } catch {
    return [];
  }
}

async function loadJobs() {
  const rawJobs = await loadRawJobs();
  return filterDisabledSourceJobs(rawJobs).jobs;
}

async function saveJobs(jobs) {
  await ensureStoreFile();
  const filtered = filterDisabledSourceJobs(Array.isArray(jobs) ? jobs : []);
  await fs.writeFile(DATA_FILE, `${JSON.stringify(filtered.jobs, null, 2)}\n`, "utf8");
}

async function syncClubResults(clubResults) {
  const existingJobs = await loadJobs();
  let nextJobs = existingJobs;
  const reports = [];

  for (const clubResult of Array.isArray(clubResults) ? clubResults : []) {
    const applied = applyClubSync(nextJobs, clubResult);
    nextJobs = applied.jobs;
    reports.push(applied.report);
  }

  await saveJobs(nextJobs);
  return { jobs: await loadJobs(), reports };
}

function cleanupHistoricalJobs(jobs, clubs) {
  let pendingJobs = dedupeJobs(Array.isArray(jobs) ? jobs : []);
  const cleanedJobs = [];
  const reports = [];

  for (const club of Array.isArray(clubs) ? clubs : []) {
    const matcher = createClubLineageMatcher(club);
    const lineageJobs = pendingJobs.filter(matcher);

    if (!lineageJobs.length) {
      continue;
    }

    pendingJobs = pendingJobs.filter((job) => !matcher(job));

    const grouped = new Map();
    for (const job of lineageJobs) {
      const identity = buildCleanupIdentity(job);
      if (!grouped.has(identity)) {
        grouped.set(identity, []);
      }
      grouped.get(identity).push(job);
    }

    let removedDuplicates = 0;
    let rewrittenOwnership = 0;

    for (const group of grouped.values()) {
      const [bestMatch] = [...group].sort(
        (jobA, jobB) => scoreJobForClub(jobB, club) - scoreJobForClub(jobA, club)
      );

      const normalizedBest = normalizeStoredJob({
        ...bestMatch,
        club_id: normalizeText(club.club_id),
        club: normalizeText(bestMatch.club || club.name || ""),
      });

      if (bestMatch.club_id !== normalizedBest.club_id) {
        rewrittenOwnership += 1;
      }

      removedDuplicates += Math.max(group.length - 1, 0);
      cleanedJobs.push(normalizedBest);
    }

    reports.push({
      club_id: normalizeText(club.club_id),
      lineage_jobs: lineageJobs.length,
      removed_duplicates: removedDuplicates,
      rewritten_ownership: rewrittenOwnership,
    });
  }

  return {
    jobs: dedupeJobs([...cleanedJobs, ...pendingJobs]),
    reports,
  };
}

async function cleanupStore(clubs) {
  const existingJobs = await loadRawJobs();
  const filtered = filterDisabledSourceJobs(existingJobs);
  const cleaned = cleanupHistoricalJobs(filtered.jobs, clubs);
  await saveJobs(cleaned.jobs);

  return {
    jobs: await loadJobs(),
    disabled_reports: filtered.reports,
    removed_disabled_jobs: filtered.removed_jobs.length,
    reports: cleaned.reports,
  };
}

async function purgeDisabledJobsFromStore() {
  const existingJobs = await loadRawJobs();
  const filtered = filterDisabledSourceJobs(existingJobs);
  await saveJobs(filtered.jobs);

  return {
    jobs: await loadJobs(),
    removed_disabled_jobs: filtered.removed_jobs.length,
    reports: filtered.reports,
  };
}

module.exports = {
  DATA_FILE,
  buildDedupeKey,
  loadRawJobs,
  loadJobs,
  saveJobs,
  syncClubResults,
  cleanupHistoricalJobs,
  cleanupStore,
  filterDisabledSourceJobs,
  purgeDisabledJobsFromStore,
  createClubLineageMatcher,
  normalizeStoredJob,
};
