#!/usr/bin/env node
/* eslint-disable no-console */

import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath, pathToFileURL } from 'node:url';
// This tool keeps its own package.json under tools/scheduling-export/.
// eslint-disable-next-line import/no-unresolved, import/no-extraneous-dependencies
import {
  GetObjectCommand,
  ListObjectsV2Command,
  S3Client,
} from '@aws-sdk/client-s3';

const DEFAULT_BUCKET = 'helix-snapshot-scheduler';
const COMPLETED_PREFIX = 'completed/';
const FAILED_PREFIX = 'failed/';
const LIST_PAGE_SIZE = 1000;
const GET_CONCURRENCY = 16;
const DEFAULT_DAYS = 120;

const SCRIPT_FILE = fileURLToPath(import.meta.url);
const SCRIPT_DIR = path.dirname(SCRIPT_FILE);

const startedAt = Date.now();

function ts() {
  const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);
  return `[scheduling-export ${new Date().toISOString()} +${elapsed}s]`;
}

function log(msg) { console.log(`${ts()} ${msg}`); }
function logErr(msg) { console.error(`${ts()} ${msg}`); }

function printUsage() {
  console.log(`
Usage:
  node tools/scheduling-export/index.js \\
    [--bucket helix-snapshot-scheduler] \\
    [--output-dir ./tools/scheduling-export/output] \\
    [--days 120 | --days all] \\
    [--source both | completed | failed] \\
    [--env-file /path/to/.env]

Writes a single CSV that combines completed and failed publishes
(default) into one sheet, ordered newest first by publishedAt for
completed rows and failedAt for failed rows. A "status" column
distinguishes the two. Pending entries (still in schedule.json) are
not included.

Credentials:
  Set CLOUDFLARE_R2_ACCOUNT_ID, CLOUDFLARE_R2_ACCESS_KEY_ID, and
  CLOUDFLARE_R2_SECRET_ACCESS_KEY either as environment variables or
  via a .env file. The CLI auto-loads tools/scheduling-export/.env or
  tools/scheduling-dashboard/.env if present, or pass --env-file.
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

function autoLoadEnv() {
  const candidates = [
    path.resolve(SCRIPT_DIR, '.env'),
    path.resolve(SCRIPT_DIR, '..', 'scheduling-dashboard', '.env'),
  ];
  const found = candidates.find((c) => fs.existsSync(c));
  if (found) loadEnvFile(found);
}

function parseArgs(argv) {
  const args = {
    bucket: DEFAULT_BUCKET,
    outputDir: path.resolve(SCRIPT_DIR, 'output'),
    days: DEFAULT_DAYS,
    source: 'both',
    envFile: undefined,
    help: false,
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
      case '--source': {
        const raw = String(argv[i + 1] || '').toLowerCase();
        i += 1;
        if (!['completed', 'failed', 'both'].includes(raw)) {
          throw new Error(`--source must be completed | failed | both; received ${raw}`);
        }
        args.source = raw;
        break;
      }
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
    const sourceKey = keys[idx];
    if (Array.isArray(data)) {
      data.forEach((rec) => {
        if (rec && typeof rec === 'object') {
          records.push({ ...rec, sourceFile: sourceKey });
        }
      });
    } else if (typeof data === 'object') {
      records.push({ ...data, sourceFile: sourceKey });
    }
  });
  log(`Parsed ${records.length} ${label} record(s) from ${keys.length} file(s).`);
  return records;
}

function dayOffsetUTC(date, offset) {
  const d = new Date(Date.UTC(
    date.getUTCFullYear(),
    date.getUTCMonth(),
    date.getUTCDate(),
  ));
  d.setUTCDate(d.getUTCDate() + offset);
  return d;
}

function dateStrUTC(d) { return d.toISOString().slice(0, 10); }

function computeWindow(days) {
  const now = new Date();
  const todayUTC = new Date(Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate(),
  ));
  if (days === 'all') {
    return {
      from: null,
      to: todayUTC,
      fromStr: null,
      toStr: dateStrUTC(todayUTC),
    };
  }
  const from = dayOffsetUTC(todayUTC, -(days - 1));
  return {
    from,
    to: todayUTC,
    fromStr: dateStrUTC(from),
    toStr: dateStrUTC(todayUTC),
  };
}

const COMBINED_PREFERRED = [
  'status',
  'org',
  'site',
  'path',
  'type',
  'scheduledPublish',
  'publishedAt',
  'publishedBy',
  'failedAt',
  'timestamp',
  'reason',
  'userId',
  'messageId',
];

function pickRecordDate(rec) {
  if (rec.status === 'completed') {
    return rec.publishedAt || rec.scheduledPublish || null;
  }
  return rec.failedAt || rec.timestamp || rec.scheduledPublish || null;
}

function filterByWindow(records, window) {
  if (window.fromStr === null) {
    return records.filter((r) => !!pickRecordDate(r));
  }
  return records.filter((r) => {
    const iso = pickRecordDate(r);
    if (!iso) return false;
    const d = String(iso).slice(0, 10);
    return d >= window.fromStr && d <= window.toStr;
  });
}

function computeColumns(records) {
  const seen = new Set(COMBINED_PREFERRED);
  const extras = [];
  records.forEach((rec) => {
    Object.keys(rec).forEach((k) => {
      if (k === 'sourceFile') return;
      if (!seen.has(k)) { seen.add(k); extras.push(k); }
    });
  });
  extras.sort();
  return [...COMBINED_PREFERRED, ...extras, 'sourceFile'];
}

function csvCell(value) {
  if (value === null || value === undefined) return '';
  let s;
  if (value instanceof Date) s = value.toISOString();
  else if (typeof value === 'object') s = JSON.stringify(value);
  else s = String(value);
  if (/[",\r\n]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function recordsToCsv(records, columns) {
  const sorted = [...records].sort((a, b) => {
    const av = pickRecordDate(a) || '';
    const bv = pickRecordDate(b) || '';
    return bv.localeCompare(av);
  });
  const lines = [];
  lines.push(columns.map(csvCell).join(','));
  sorted.forEach((rec) => {
    const row = columns.map((c) => csvCell(rec[c]));
    lines.push(row.join(','));
  });
  log(`Sorted ${sorted.length} record(s) by event time desc.`);
  return `${lines.join('\r\n')}\r\n`;
}

async function fetchKind(s3, bucket, kind) {
  const prefix = kind === 'completed' ? COMPLETED_PREFIX : FAILED_PREFIX;
  log(`Listing ${prefix}...`);
  const keys = await listAllKeys(s3, bucket, prefix);
  log(`Found ${keys.length} ${kind} file(s).`);
  const records = await fetchAndFlatten(s3, bucket, keys, kind);
  return records.map((r) => ({ status: kind, ...r }));
}

function fileNameForSource(source, window) {
  const fromTag = window.fromStr || 'all';
  const stem = source === 'both' ? 'scheduling' : source;
  return `${stem}_${fromTag}_to_${window.toStr}.csv`;
}

async function exportCombined(s3, bucket, source, window, outputDir) {
  const kinds = source === 'both' ? ['completed', 'failed'] : [source];
  const all = [];
  // Sequential per-kind so logs read top to bottom; each fetch already runs internal concurrency.
  await kinds.reduce(async (prev, kind) => {
    await prev;
    const recs = await fetchKind(s3, bucket, kind);
    all.push(...recs);
  }, Promise.resolve());

  const filtered = filterByWindow(all, window);
  const completedCount = filtered.filter((r) => r.status === 'completed').length;
  const failedCount = filtered.filter((r) => r.status === 'failed').length;
  log(`Kept ${filtered.length} record(s) in window (${completedCount} completed, ${failedCount} failed).`);

  const columns = computeColumns(filtered);
  const csv = recordsToCsv(filtered, columns);
  const outFile = path.resolve(outputDir, fileNameForSource(source, window));
  await fs.promises.mkdir(outputDir, { recursive: true });
  // UTF-8 BOM so Excel auto-detects encoding for non-ASCII characters (e.g. accented emails).
  await fs.promises.writeFile(outFile, `\ufeff${csv}`, 'utf-8');
  log(`Wrote ${filtered.length} row(s) to ${outFile}`);
  return {
    outFile, count: filtered.length, completedCount, failedCount, columns,
  };
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
  if (args.help) { printUsage(); return; }

  try {
    if (args.envFile) {
      loadEnvFile(path.resolve(args.envFile), { required: true });
    } else {
      autoLoadEnv();
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

  const window = computeWindow(args.days);
  log(`Bucket: ${args.bucket}`);
  log(`Output dir: ${args.outputDir}`);
  log(`Source: ${args.source}`);
  log(`Window: ${window.fromStr || '(open-start)'} \u2192 ${window.toStr} (${args.days === 'all' ? 'all-time' : `${args.days} day(s)`})`);

  const s3 = createR2Client(env);
  const summary = await exportCombined(
    s3,
    args.bucket,
    args.source,
    window,
    args.outputDir,
  );

  log('Done.');
  log(`  ${summary.count} row(s) (${summary.completedCount} completed, ${summary.failedCount} failed) -> ${summary.outFile}`);
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
  computeWindow,
  filterByWindow,
  computeColumns,
  recordsToCsv,
  csvCell,
};
