const CF_CODE_TAG = '<ref>--<site>--<org>_code';

/**
 * Purges a Cloudflare production CDN
 * @param {Object} creds purge credentials
 */
async function purgeCloudflare(creds) {
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
  // Purge by tag
  body.tags = [CF_CODE_TAG];
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
    HOST,
    ZONEID,
    APITOKEN,
    PLAN,
    DELAY,
  } = process.env;
  if (!HOST || !ZONEID || !APITOKEN || !PLAN) {
    throw new Error('missing required env arguments');
  }
  // const changedFilesArray = ALL_CHANGED_FILES?.split(' ');
  // console.info(`changedFiles: ${JSON.stringify(changedFilesArray)}`);
  setTimeout(async () => {
    // Code to execute after the delay or 1 second by default
    await purgeCloudflare({
      host: HOST,
      zoneId: ZONEID,
      apiToken: APITOKEN,
      plan: PLAN,
    });
  }, DELAY || 1000); // 10000 milliseconds = 10 seconds
} catch (error) {
  console.error(error.message);
}
