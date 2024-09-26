// Action to purge Cloudflare cache for a set of paths 
// or the whole site if more than 30 paths

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
    console.info(`[cloudflare] ${host} key purge not supported for the plan '${plan}', 
      or more than 30 changed files. purging everything`);
    body.purge_everything = true;
  }
  try {
    const resp = await fetch(url, { method, headers, body: JSON.stringify(body) });
    if (resp.ok) {
      console.info(`[cloudflare] ${host} purge succeeded - ${resp.status}`);
    } else {
      console.error(`[cloudflare] ${host} purge failed - ${resp.status} - ${await resp.text()}`);
    }
  } catch (err) {
    console.error(`[cloudflare] ${host} purge failed: ${err}`);
  }
}

try {
  const {
    ALL_CHANGED_FILES,
    HOST,
    ZONEID,
    APITOKEN,
    PLAN,
  } = process.env;
  if (!ALL_CHANGED_FILES || !HOST || !ZONEID || !APITOKEN || !PLAN) {
    throw new Error('missing required env arguments');
  }
  const changedFilesArray = ALL_CHANGED_FILES.split(' ');
  console.info(`changedFiles: ${JSON.stringify(changedFilesArray)}`);
  await purgeCloudflare({
    host: HOST,
    zoneId: ZONEID,
    apiToken: APITOKEN,
    plan: PLAN,
  }, changedFilesArray);
} catch (error) {
  console.error(error.message);
}