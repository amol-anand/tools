// This file is used to purge the code across all sites

/**
 * Purges a Cloudflare production CDN
 * @param {Object} creds purge credentials
 * @param {Array<string>} [params.paths] url paths to purge
 */
async function purgeCloudflare(creds, paths = []) {
  const {
    host,
    zoneId,
    apiToken,
    plan,
  } = creds;

  const url = `https://api.cloudflare.com/client/v4/zones/${zoneId}/purge_cache`;
  const headers = { Accept: 'application/json', Authorization: `Bearer ${apiToken}` };
  const method = 'POST';

  const body = {};
  if (paths.length <= 30) {
    body.files = paths.map((path) => `https://${host}${path}`);
  } else {
    log.info(`[cloudflare] ${host} key purge not supported for the plan '${plan}', 
      or more than 30 changed files. purging everything`);
    body.purge_everything = true;
  }
  try {
    const resp = await fetch(url, { method, headers, body: JSON.stringify(body) });
    if (resp.ok) {
      log.info(`[cloudflare] ${host} purge succeeded - ${resp.status}`);
    } else {
      log.error(`[cloudflare] ${host} purge failed - ${resp.status} - ${await resp.text()}`);
    }
  } catch (err) {
    log.error(`[cloudflare] ${host} purge failed: ${err}`);
  }
}

try {
  const {
    ALL_CHANGED_FILES,
    SITE_1_HOST,
    SITE_1_ZONEID,
    SITE_1_APITOKEN,
    SITE_1_PLAN,
  } = process.env;
  const changedFilesArray = ALL_CHANGED_FILES.split(' ');
  console.log(`changedFiles: ${JSON.stringify(changedFilesArray)}`);
  await purgeCloudflare({
    host: SITE_1_HOST,
    zoneId: SITE_1_ZONEID,
    apiToken: SITE_1_APITOKEN,
    plan: SITE_1_PLAN,
  }, changedFilesArray);
} catch (error) {
  console.error(error.message);
}