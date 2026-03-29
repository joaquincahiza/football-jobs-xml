const {
  normalizeText,
  normalizeUrlForIdentity,
} = require("../connectors/utils");

const DISABLED_SOURCE_POLICIES = [
  {
    key: "laliga",
    label: "LaLiga",
    envVar: "ENABLE_LALIGA_SOURCE",
    club_ids: ["laliga"],
    names: ["laliga", "la liga"],
    source_urls: [
      "https://career2.successfactors.eu/career?company=liganacion&career_ns=job_listing_summary&navBarLevel=JOB_SEARCH",
    ],
    company_urls: ["https://www.laliga.com/"],
  },
];

function normalizeNameKey(value) {
  return normalizeText(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "");
}

function isPolicyEnabled(policy) {
  const raw = String(process.env[policy.envVar] || "").trim().toLowerCase();
  return raw === "1" || raw === "true" || raw === "yes";
}

function findDisabledSourcePolicy(entry) {
  if (!entry) {
    return null;
  }

  const clubId = normalizeText(entry.club_id).toLowerCase();
  const names = [
    normalizeNameKey(entry.name),
    normalizeNameKey(entry.club),
    normalizeNameKey(entry.company_name),
  ].filter(Boolean);
  const sourceUrl = normalizeUrlForIdentity(entry.source_url);
  const companyUrl = normalizeUrlForIdentity(entry.company_url);

  for (const policy of DISABLED_SOURCE_POLICIES) {
    const policyClubIds = policy.club_ids.map((value) =>
      normalizeText(value).toLowerCase()
    );
    const policyNames = policy.names.map((value) => normalizeNameKey(value));
    const policySourceUrls = policy.source_urls.map((value) =>
      normalizeUrlForIdentity(value)
    );
    const policyCompanyUrls = policy.company_urls.map((value) =>
      normalizeUrlForIdentity(value)
    );

    if (clubId && policyClubIds.includes(clubId)) {
      return policy;
    }

    if (names.some((value) => policyNames.includes(value))) {
      return policy;
    }

    if (sourceUrl && policySourceUrls.includes(sourceUrl)) {
      return policy;
    }

    if (companyUrl && policyCompanyUrls.includes(companyUrl)) {
      return policy;
    }
  }

  return null;
}

function isDisabledSourceEntry(entry) {
  const policy = findDisabledSourcePolicy(entry);
  return Boolean(policy && !isPolicyEnabled(policy));
}

function describeDisabledSource(policy) {
  if (!policy) {
    return "disabled_source";
  }

  return `${policy.label} disabled by policy; set ${policy.envVar}=true to re-enable`;
}

module.exports = {
  DISABLED_SOURCE_POLICIES,
  findDisabledSourcePolicy,
  isDisabledSourceEntry,
  isPolicyEnabled,
  describeDisabledSource,
};
