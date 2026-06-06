#!/usr/bin/env node
/* eslint-disable no-console */

import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { spawn } from 'node:child_process';
// This tool keeps its own package.json under tools/scheduling-dashboard/.
// eslint-disable-next-line import/no-unresolved, import/no-extraneous-dependencies
import {
  GetObjectCommand,
  ListObjectsV2Command,
  S3Client,
} from '@aws-sdk/client-s3';

const DEFAULT_BUCKET = 'helix-snapshot-scheduler';
const COMPLETED_PREFIX = 'completed/';
const FAILED_PREFIX = 'failed/';
const SCHEDULE_KEY = 'schedule.json';
const LIST_PAGE_SIZE = 1000;
const GET_CONCURRENCY = 16;
const DEFAULT_DAYS = 90;
const TOP_N = 20;
const RECENT_FAILURES = 50;

const SCRIPT_FILE = fileURLToPath(import.meta.url);
const SCRIPT_DIR = path.dirname(SCRIPT_FILE);

const startedAt = Date.now();

function ts() {
  const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);
  return `[scheduling-dashboard ${new Date().toISOString()} +${elapsed}s]`;
}

function log(msg) {
  console.log(`${ts()} ${msg}`);
}

function logErr(msg) {
  console.error(`${ts()} ${msg}`);
}

function printUsage() {
  console.log(`
Usage:
  node tools/scheduling-dashboard/index.js \\
    [--bucket helix-snapshot-scheduler] \\
    [--output-dir ./tools/scheduling-dashboard/output] \\
    [--days 90 | --days all] \\
    [--env-file /path/to/.env] \\
    [--open]

Credentials:
  Set CLOUDFLARE_R2_ACCOUNT_ID, CLOUDFLARE_R2_ACCESS_KEY_ID, and
  CLOUDFLARE_R2_SECRET_ACCESS_KEY either as environment variables, or
  via a .env file. The CLI auto-loads tools/scheduling-dashboard/.env
  if present, or pass --env-file <path> to point at another file.
  Real environment variables always win over .env values.
`);
}

function parseDotenv(text) {
  const out = {};
  text.split(/\r?\n/).forEach((rawLine) => {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) return;
    const noExport = line.startsWith('export ') ? line.slice('export '.length) : line;
    const eq = noExport.indexOf('=');
    if (eq <= 0) return;
    const key = noExport.slice(0, eq).trim();
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) return;
    let value = noExport.slice(eq + 1).trim();
    const firstChar = value.charAt(0);
    const lastChar = value.charAt(value.length - 1);
    if (
      value.length >= 2
      && (firstChar === '"' || firstChar === "'")
      && firstChar === lastChar
    ) {
      value = value.slice(1, -1);
      if (firstChar === '"') {
        value = value.replace(/\\n/g, '\n').replace(/\\r/g, '\r').replace(/\\t/g, '\t');
      }
    } else {
      const hashIdx = value.indexOf(' #');
      if (hashIdx >= 0) value = value.slice(0, hashIdx).trim();
    }
    out[key] = value;
  });
  return out;
}

function loadEnvFile(filePath, { required = false } = {}) {
  let text;
  try {
    text = fs.readFileSync(filePath, 'utf-8');
  } catch (err) {
    if (err.code === 'ENOENT') {
      if (required) throw new Error(`env file not found: ${filePath}`);
      return 0;
    }
    throw err;
  }
  const parsed = parseDotenv(text);
  let applied = 0;
  Object.entries(parsed).forEach(([k, v]) => {
    if (process.env[k] === undefined) {
      process.env[k] = v;
      applied += 1;
    }
  });
  log(`Loaded ${applied} variable(s) from ${filePath}`);
  return applied;
}

function parseArgs(argv) {
  const args = {
    bucket: DEFAULT_BUCKET,
    outputDir: path.resolve(SCRIPT_DIR, 'output'),
    days: DEFAULT_DAYS,
    open: false,
    help: false,
    envFile: undefined,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    switch (arg) {
      case '--bucket':
        args.bucket = argv[i + 1];
        i += 1;
        break;
      case '--output-dir':
        args.outputDir = path.resolve(argv[i + 1]);
        i += 1;
        break;
      case '--days': {
        const raw = argv[i + 1];
        i += 1;
        if (raw === 'all') {
          args.days = 'all';
        } else {
          const n = Number.parseInt(raw, 10);
          if (!Number.isFinite(n) || n <= 0) {
            throw new Error(`--days expects a positive integer or "all"; received ${raw}`);
          }
          args.days = n;
        }
        break;
      }
      case '--open':
        args.open = true;
        break;
      case '--env-file':
        args.envFile = argv[i + 1];
        i += 1;
        break;
      case '--help':
      case '-h':
        args.help = true;
        break;
      default:
        if (arg.startsWith('--')) {
          throw new Error(`Unknown option: ${arg}`);
        }
        break;
    }
  }

  return args;
}

function requireEnv() {
  const required = [
    'CLOUDFLARE_R2_ACCOUNT_ID',
    'CLOUDFLARE_R2_ACCESS_KEY_ID',
    'CLOUDFLARE_R2_SECRET_ACCESS_KEY',
  ];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
  return {
    accountId: process.env.CLOUDFLARE_R2_ACCOUNT_ID,
    accessKeyId: process.env.CLOUDFLARE_R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.CLOUDFLARE_R2_SECRET_ACCESS_KEY,
  };
}

function createR2Client({ accountId, accessKeyId, secretAccessKey }) {
  return new S3Client({
    region: 'auto',
    endpoint: `https://${accountId}.r2.cloudflarestorage.com`,
    credentials: { accessKeyId, secretAccessKey },
  });
}

async function listAllKeys(s3, bucket, prefix) {
  const keys = [];
  let token;
  do {
    // eslint-disable-next-line no-await-in-loop
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      MaxKeys: LIST_PAGE_SIZE,
      ContinuationToken: token,
    }));
    (res.Contents || []).forEach((obj) => {
      if (obj.Key && obj.Key !== prefix) keys.push(obj.Key);
    });
    token = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (token);
  return keys;
}

async function streamToString(stream) {
  if (!stream) return '';
  if (typeof stream.transformToString === 'function') {
    return stream.transformToString('utf-8');
  }
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on('data', (c) => chunks.push(c));
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    stream.on('error', reject);
  });
}

async function getJson(s3, bucket, key) {
  const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const body = await streamToString(res.Body);
  if (!body) return null;
  try {
    return JSON.parse(body);
  } catch (err) {
    logErr(`Failed to parse JSON for ${key}: ${err.message}`);
    return null;
  }
}

async function runWithConcurrency(items, concurrency, worker) {
  const results = new Array(items.length);
  let nextIdx = 0;
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    for (;;) {
      const i = nextIdx;
      nextIdx += 1;
      if (i >= items.length) return;
      try {
        // eslint-disable-next-line no-await-in-loop
        results[i] = await worker(items[i], i);
      } catch (err) {
        logErr(`Worker error on item ${items[i]}: ${err.message}`);
        results[i] = null;
      }
    }
  });
  await Promise.all(workers);
  return results;
}

async function fetchAndFlatten(s3, bucket, keys, label) {
  if (!keys.length) {
    log(`No ${label} files found.`);
    return [];
  }
  log(`Fetching ${keys.length} ${label} file(s)...`);
  const parsed = await runWithConcurrency(keys, GET_CONCURRENCY, (key) => getJson(s3, bucket, key));
  const records = [];
  parsed.forEach((data, idx) => {
    if (!data) return;
    if (Array.isArray(data)) {
      data.forEach((rec) => {
        if (rec && typeof rec === 'object') {
          records.push({ ...rec, __sourceKey: keys[idx] });
        }
      });
    } else if (typeof data === 'object') {
      records.push({ ...data, __sourceKey: keys[idx] });
    }
  });
  log(`Parsed ${records.length} ${label} record(s) from ${keys.length} file(s).`);
  return records;
}

function parseSchedule(scheduleJson) {
  if (!scheduleJson || typeof scheduleJson !== 'object') return [];
  const out = [];
  Object.entries(scheduleJson).forEach(([orgSiteKey, entries]) => {
    if (!entries || typeof entries !== 'object') return;
    const sepIdx = orgSiteKey.indexOf('--');
    const org = sepIdx >= 0 ? orgSiteKey.slice(0, sepIdx) : orgSiteKey;
    const site = sepIdx >= 0 ? orgSiteKey.slice(sepIdx + 2) : '';
    Object.entries(entries).forEach(([name, value]) => {
      if (!value || typeof value !== 'object') return;
      out.push({
        org,
        site,
        name,
        type: value.type || 'unknown',
        scheduledPublish: value.scheduledPublish || null,
        approved: typeof value.approved === 'boolean' ? value.approved : null,
        userId: value.userId || null,
      });
    });
  });
  return out;
}

function toDateStr(iso) {
  if (!iso) return null;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function dayOffsetUTC(date, offset) {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  d.setUTCDate(d.getUTCDate() + offset);
  return d;
}

function dateStrUTC(d) {
  return d.toISOString().slice(0, 10);
}

function computeWindow(days, completed, failed) {
  const today = new Date();
  const todayUTC = new Date(Date.UTC(
    today.getUTCFullYear(),
    today.getUTCMonth(),
    today.getUTCDate(),
  ));
  let from;
  if (days === 'all') {
    let minIso = null;
    [...completed, ...failed].forEach((r) => {
      const iso = r.publishedAt || r.failedAt || r.timestamp || r.scheduledPublish;
      if (iso && (!minIso || iso < minIso)) minIso = iso;
    });
    if (minIso) {
      const m = new Date(minIso);
      from = new Date(Date.UTC(m.getUTCFullYear(), m.getUTCMonth(), m.getUTCDate()));
    } else {
      from = dayOffsetUTC(todayUTC, -DEFAULT_DAYS + 1);
    }
  } else {
    from = dayOffsetUTC(todayUTC, -(days - 1));
  }
  return { from, to: todayUTC };
}

function isInWindow(iso, fromStr, toStr) {
  if (!iso) return false;
  const d = iso.slice(0, 10);
  return d >= fromStr && d <= toStr;
}

function topNEntries(map, n, sortKey = 'total') {
  return Object.entries(map)
    .map(([key, value]) => ({ key, ...value }))
    .sort((a, b) => (b[sortKey] || 0) - (a[sortKey] || 0))
    .slice(0, n);
}

function buildAggregates(completed, failed, pending, days) {
  const { from, to } = computeWindow(days, completed, failed);
  const fromStr = dateStrUTC(from);
  const toStr = dateStrUTC(to);

  const dayKeys = [];
  const daySeriesMap = new Map();
  for (let d = new Date(from); d <= to; d = dayOffsetUTC(d, 1)) {
    const key = dateStrUTC(d);
    dayKeys.push(key);
    daySeriesMap.set(key, { date: key, completed: 0, failed: 0 });
  }

  const byOrgSite = {};
  const ensureOrgSite = (org, site) => {
    const k = `${org || '(unknown)'}/${site || '(unknown)'}`;
    if (!byOrgSite[k]) {
      byOrgSite[k] = {
        org: org || '(unknown)',
        site: site || '(unknown)',
        completed: 0,
        failed: 0,
        pending: 0,
        total: 0,
      };
    }
    return byOrgSite[k];
  };

  const byUser = {};
  const ensureUser = (userId) => {
    if (!byUser[userId]) {
      byUser[userId] = {
        userId, failed: 0, pending: 0, total: 0,
      };
    }
    return byUser[userId];
  };

  const failureReasons = {};

  let completedTotal = 0;
  let failedTotal = 0;

  completed.forEach((rec) => {
    const dStr = toDateStr(rec.publishedAt) || toDateStr(rec.scheduledPublish);
    if (!dStr || !isInWindow(`${dStr}T00:00:00Z`, fromStr, toStr)) return;
    completedTotal += 1;
    const bucket = daySeriesMap.get(dStr);
    if (bucket) bucket.completed += 1;
    const os = ensureOrgSite(rec.org, rec.site);
    os.completed += 1;
    os.total += 1;
  });

  failed.forEach((rec) => {
    const dStr = toDateStr(rec.failedAt)
      || toDateStr(rec.timestamp)
      || toDateStr(rec.scheduledPublish);
    if (!dStr || !isInWindow(`${dStr}T00:00:00Z`, fromStr, toStr)) return;
    failedTotal += 1;
    const bucket = daySeriesMap.get(dStr);
    if (bucket) bucket.failed += 1;
    const os = ensureOrgSite(rec.org, rec.site);
    os.failed += 1;
    os.total += 1;
    const reason = rec.reason || 'unknown';
    failureReasons[reason] = (failureReasons[reason] || 0) + 1;
    if (rec.userId) {
      const u = ensureUser(rec.userId);
      u.failed += 1;
      u.total += 1;
    }
  });

  let pendingApproved = 0;
  let pendingUnapproved = 0;
  let pendingApprovalUnknown = 0;
  const pendingByType = { snapshot: 0, page: 0, unknown: 0 };
  pending.forEach((rec) => {
    const os = ensureOrgSite(rec.org, rec.site);
    os.pending += 1;
    os.total += 1;
    if (rec.userId) {
      const u = ensureUser(rec.userId);
      u.pending += 1;
      u.total += 1;
    }
    if (rec.approved === true) pendingApproved += 1;
    else if (rec.approved === false) pendingUnapproved += 1;
    else pendingApprovalUnknown += 1;
    const t = rec.type === 'snapshot' || rec.type === 'page' ? rec.type : 'unknown';
    pendingByType[t] += 1;
  });

  const dailySeries = dayKeys.map((k) => daySeriesMap.get(k));

  const distinctOrgs = new Set();
  const distinctSites = new Set();
  Object.values(byOrgSite).forEach((v) => {
    if (v.org && v.org !== '(unknown)') distinctOrgs.add(v.org);
    if (v.org && v.site) distinctSites.add(`${v.org}/${v.site}`);
  });
  const distinctUsers = new Set(Object.keys(byUser));

  const recentFailures = [...failed]
    .filter((r) => r.failedAt || r.timestamp)
    .sort((a, b) => {
      const ai = a.failedAt || a.timestamp || '';
      const bi = b.failedAt || b.timestamp || '';
      return bi.localeCompare(ai);
    })
    .slice(0, RECENT_FAILURES)
    .map((r) => ({
      org: r.org,
      site: r.site,
      path: r.path,
      userId: r.userId || null,
      reason: r.reason || 'unknown',
      failedAt: r.failedAt || r.timestamp || null,
      type: r.type || null,
    }));

  const pendingSorted = [...pending].sort((a, b) => {
    const ai = a.scheduledPublish || '';
    const bi = b.scheduledPublish || '';
    return ai.localeCompare(bi);
  });

  const total = completedTotal + failedTotal;
  const successRate = total > 0 ? completedTotal / total : null;

  return {
    summary: {
      windowDays: days === 'all' ? 'all' : days,
      windowFrom: fromStr,
      windowTo: toStr,
      completedTotal,
      failedTotal,
      successRate,
      distinctOrgs: distinctOrgs.size,
      distinctSites: distinctSites.size,
      distinctUsers: distinctUsers.size,
      pendingTotal: pending.length,
      pendingApproved,
      pendingUnapproved,
      pendingApprovalUnknown,
      generatedAt: new Date().toISOString(),
    },
    dailySeries,
    topOrgSite: topNEntries(byOrgSite, TOP_N, 'total'),
    topUsers: topNEntries(byUser, TOP_N, 'total'),
    failureReasons: Object.entries(failureReasons)
      .map(([reason, count]) => ({ reason, count }))
      .sort((a, b) => b.count - a.count),
    pendingByType,
    pending: pendingSorted,
    recentFailures,
  };
}

function renderHtml(template, aggregates) {
  const json = JSON.stringify(aggregates)
    .replace(/</g, '\\u003c')
    .replace(/>/g, '\\u003e')
    .replace(/&/g, '\\u0026')
    .replace(/\u2028/g, '\\u2028')
    .replace(/\u2029/g, '\\u2029');
  if (!template.includes('__DATA__')) {
    throw new Error('template.html is missing the __DATA__ placeholder');
  }
  // Use the function form so $-sequences in JSON are not interpreted as replacement patterns.
  return template.replace('__DATA__', () => json);
}

function openInBrowser(filePath) {
  const { platform } = process;
  let cmd;
  let args;
  if (platform === 'darwin') {
    cmd = 'open';
    args = [filePath];
  } else if (platform === 'win32') {
    cmd = 'cmd';
    args = ['/c', 'start', '', filePath];
  } else {
    cmd = 'xdg-open';
    args = [filePath];
  }
  try {
    const child = spawn(cmd, args, { detached: true, stdio: 'ignore' });
    child.unref();
  } catch (err) {
    logErr(`Could not open browser automatically: ${err.message}`);
  }
}

async function main(argv = process.argv.slice(2)) {
  let args;
  try {
    args = parseArgs(argv);
  } catch (err) {
    logErr(err.message);
    printUsage();
    process.exitCode = 2;
    return;
  }
  if (args.help) {
    printUsage();
    return;
  }

  try {
    if (args.envFile) {
      loadEnvFile(path.resolve(args.envFile), { required: true });
    } else {
      loadEnvFile(path.resolve(SCRIPT_DIR, '.env'));
    }
  } catch (err) {
    logErr(err.message);
    process.exitCode = 2;
    return;
  }

  let env;
  try {
    env = requireEnv();
  } catch (err) {
    logErr(err.message);
    printUsage();
    process.exitCode = 2;
    return;
  }

  log(`Bucket: ${args.bucket}`);
  log(`Output dir: ${args.outputDir}`);
  log(`Window: ${args.days === 'all' ? 'all-time' : `${args.days} day(s)`}`);

  const s3 = createR2Client(env);

  log('Listing completed/ and failed/ keys...');
  const [completedKeys, failedKeys] = await Promise.all([
    listAllKeys(s3, args.bucket, COMPLETED_PREFIX),
    listAllKeys(s3, args.bucket, FAILED_PREFIX),
  ]);
  log(`Found ${completedKeys.length} completed file(s) and ${failedKeys.length} failed file(s).`);

  log('Fetching schedule.json...');
  let scheduleJson = null;
  try {
    scheduleJson = await getJson(s3, args.bucket, SCHEDULE_KEY);
  } catch (err) {
    logErr(`Could not fetch ${SCHEDULE_KEY}: ${err.message}`);
  }

  const [completed, failed] = await Promise.all([
    fetchAndFlatten(s3, args.bucket, completedKeys, 'completed'),
    fetchAndFlatten(s3, args.bucket, failedKeys, 'failed'),
  ]);
  const pending = parseSchedule(scheduleJson);
  log(`Parsed ${pending.length} pending schedule entries.`);

  log('Aggregating...');
  const aggregates = buildAggregates(completed, failed, pending, args.days);

  log('Rendering dashboard.html...');
  const templatePath = path.resolve(SCRIPT_DIR, 'template.html');
  const template = await fs.promises.readFile(templatePath, 'utf-8');
  const html = renderHtml(template, aggregates);

  await fs.promises.mkdir(args.outputDir, { recursive: true });
  const outFile = path.resolve(args.outputDir, 'dashboard.html');
  await fs.promises.writeFile(outFile, html, 'utf-8');
  log(`Wrote ${outFile}`);

  if (args.open) {
    log('Opening in browser...');
    openInBrowser(outFile);
  }
}

function isMainModule() {
  if (!process.argv[1]) return false;
  return import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href;
}

if (isMainModule()) {
  main().catch((err) => {
    logErr(err.stack || err.message || String(err));
    process.exitCode = 1;
  });
}

export {
  parseArgs,
  parseSchedule,
  buildAggregates,
  renderHtml,
};
