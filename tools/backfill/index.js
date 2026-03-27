#!/usr/bin/env node
/* eslint-disable no-console */

import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { pathToFileURL } from 'node:url';
import zlib from 'node:zlib';
// This tool keeps its own package.json under tools/backfill/.
// eslint-disable-next-line import/no-unresolved, import/no-extraneous-dependencies
import {
  HeadObjectCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';

const INDEX_FILE = '.index';
const MAX_OBJECT_SIZE = 512 * 1024;
const LIST_OBJECTS_PAGE_SIZE = 1000;
const HEAD_REQUEST_CONCURRENCY = 32;
const CONTENT_BUS_BUCKET = 'helix-content-bus';
const MEDIA_LOG_BUCKET = 'helix-media-logs';
const REDIRECT_REPORT_FILE = 'redirect-recovery-report.json';
const HASHED_MEDIA_PATH_REGEX = /\/media_([0-9a-f]+)\.[a-z0-9]+$/i;
const PREVIEW_MEDIA_PATH_REGEX = /\.(png|jpe?g|gif|webp|avif|svg|ico|mp4|mov|webm|avi|m4v|mkv)$/i;
const loggingState = {
  runStartedAt: 0,
};

function formatDuration(durationMs) {
  const safeMs = Math.max(0, Math.trunc(durationMs));
  const hours = Math.floor(safeMs / 3600000);
  const minutes = Math.floor((safeMs % 3600000) / 60000);
  const seconds = Math.floor((safeMs % 60000) / 1000);
  const milliseconds = safeMs % 1000;
  return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}.${String(milliseconds).padStart(3, '0')}`;
}

function startRunLogging(startedAt = Date.now()) {
  loggingState.runStartedAt = startedAt;
}

function getLogPrefix() {
  const nowMs = Date.now();
  const timestamp = new Date(nowMs).toISOString();
  const elapsedSuffix = loggingState.runStartedAt
    ? ` +${formatDuration(nowMs - loggingState.runStartedAt)}`
    : '';
  return `[backfill ${timestamp}${elapsedSuffix}]`;
}

function logStage(message) {
  console.log(`${getLogPrefix()} ${message}`);
}

function logError(message) {
  console.error(`${getLogPrefix()} ${message}`);
}

function printUsage() {
  console.log(`
Usage:
  node tools/backfill/index.js \\
    --bundle /path/to/medialog-import-bundle.json \\
    --content-bus-id <folder/prefix> [--bucket helix-media-logs] [--dry-run] [--output-dir /path]

Options:
  --bundle            Path to the exported medialog import bundle JSON.
  --bucket            Optional compatibility argument. Must be helix-media-logs if provided.
  --content-bus-id    Required. Target folder/prefix within the bucket.
  --region            Optional AWS region override.
  --output-dir        Optional output directory for generated artifacts and reports.
  --dry-run           Build artifacts and reports locally, but do not upload to S3.
  --help              Show this help.

Behavior:
  - Redirect recovery reads only from helix-content-bus/<contentBusId>/preview/.
  - If no existing .index is present, imports all valid entries.
  - If an existing .index is present, only prepends imported entries strictly older than the
    earliest existing medialog entry.
  - Entries at or after that existing boundary are treated as already recorded and skipped.
  - Entries with validation or packaging issues are written to unmerged-errors.json.
`);
}

function normalizeString(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function normalizePrefix(value) {
  return normalizeString(value).replace(/^\/+|\/+$/g, '');
}

function parseArgs(argv) {
  const args = {
    dryRun: false,
    region: '',
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    switch (arg) {
      case '--bundle':
        args.bundle = argv[i + 1];
        i += 1;
        break;
      case '--bucket':
        args.bucket = argv[i + 1];
        i += 1;
        break;
      case '--content-bus-id':
        args.contentBusId = argv[i + 1];
        i += 1;
        break;
      case '--region':
        args.region = argv[i + 1];
        i += 1;
        break;
      case '--output-dir':
        args.outputDir = argv[i + 1];
        i += 1;
        break;
      case '--dry-run':
        args.dryRun = true;
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

  args.bundle = normalizeString(args.bundle);
  args.bucket = normalizeString(args.bucket);
  args.contentBusId = normalizePrefix(args.contentBusId);
  args.region = normalizeString(args.region);
  args.outputDir = normalizeString(args.outputDir);

  return args;
}

function resolveMediaLogBucket(bucketArg) {
  const bucket = normalizeString(bucketArg);
  if (!bucket) {
    return {
      bucket: MEDIA_LOG_BUCKET,
      compatibilityArgUsed: false,
    };
  }

  if (bucket !== MEDIA_LOG_BUCKET) {
    throw new Error(`--bucket is fixed to ${MEDIA_LOG_BUCKET}; received ${bucket}`);
  }

  return {
    bucket: MEDIA_LOG_BUCKET,
    compatibilityArgUsed: true,
  };
}

function getPathnameFromRef(pathOrUrl) {
  if (!pathOrUrl || typeof pathOrUrl !== 'string') {
    return '';
  }

  try {
    return new URL(pathOrUrl).pathname || '';
  } catch {
    return pathOrUrl.split(/[?#]/, 1)[0];
  }
}

function normalizePathname(pathOrUrl) {
  const pathname = getPathnameFromRef(pathOrUrl)
    .replace(/\/+/g, '/')
    .trim();
  if (!pathname) {
    return '';
  }
  return pathname.startsWith('/') ? pathname : `/${pathname}`;
}

function extractHashedMediaPathname(pathOrUrl) {
  const pathname = normalizePathname(pathOrUrl);
  return HASHED_MEDIA_PATH_REGEX.test(pathname) ? pathname : '';
}

function normalizeMediaHash(value) {
  const hash = normalizeString(value).toLowerCase();
  return /^[0-9a-f]+$/i.test(hash) ? hash : '';
}

function extractMediaHash(pathOrUrl) {
  const pathname = normalizePathname(pathOrUrl);
  const match = pathname.match(HASHED_MEDIA_PATH_REGEX);
  return match?.[1]?.toLowerCase() || '';
}

function isHashedMediaReference(pathOrUrl) {
  return !!extractHashedMediaPathname(pathOrUrl);
}

function normalizeOriginalFilenameValue(value) {
  const normalizedValue = normalizeString(value);
  if (!normalizedValue) {
    return '';
  }

  return normalizePathname(normalizedValue) || normalizedValue;
}

function getEntryMediaHash(entry) {
  return normalizeMediaHash(entry?.mediaHash)
    || extractMediaHash(entry?.path)
    || extractMediaHash(entry?.originalFilename);
}

function needsOriginalFilenameRecovery(entry) {
  return isHashedMediaReference(entry?.path)
    && (!entry.originalFilename || isHashedMediaReference(entry.originalFilename));
}

function previewKeyToOriginalPath(key, contentBusId) {
  const prefix = `${contentBusId}/preview`;
  if (!key.startsWith(prefix)) {
    return '';
  }
  const remainder = key.slice(prefix.length);
  if (!remainder) {
    return '';
  }
  return remainder.startsWith('/') ? remainder : `/${remainder}`;
}

function isPreviewMediaCandidateKey(key, contentBusId) {
  const originalPath = previewKeyToOriginalPath(key, contentBusId);
  if (!originalPath || originalPath.endsWith('/')) {
    return false;
  }
  if (isHashedMediaReference(originalPath)) {
    return false;
  }
  return PREVIEW_MEDIA_PATH_REGEX.test(originalPath);
}

function choosePreferredOriginalPath(current, candidate) {
  if (!current) {
    return candidate;
  }
  if (candidate.length !== current.length) {
    return candidate.length < current.length ? candidate : current;
  }
  return candidate.localeCompare(current) < 0 ? candidate : current;
}

function normalizeTimestampMs(value) {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : NaN;
  }
  if (typeof value === 'string' && value) {
    const ts = Date.parse(value);
    return Number.isNaN(ts) ? NaN : ts;
  }
  return NaN;
}

function formatTimestamp(timestamp) {
  return new Date(timestamp).toISOString().substring(0, 19).replace(/[T:]/g, '-');
}

function createTimestampRange(entries) {
  if (!entries.length) {
    return { first: null, last: null };
  }
  return {
    first: new Date(entries[0].timestamp).toISOString(),
    last: new Date(entries[entries.length - 1].timestamp).toISOString(),
  };
}

function compareEntries(a, b) {
  const tsDiff = a.timestamp - b.timestamp;
  if (tsDiff !== 0) return tsDiff;
  const operationDiff = (a.operation || '').localeCompare(b.operation || '');
  if (operationDiff !== 0) return operationDiff;
  const pathDiff = (a.path || '').localeCompare(b.path || '');
  if (pathDiff !== 0) return pathDiff;
  const resourcePathDiff = (a.resourcePath || '').localeCompare(b.resourcePath || '');
  if (resourcePathDiff !== 0) return resourcePathDiff;
  const contentTypeDiff = (a.contentType || '').localeCompare(b.contentType || '');
  if (contentTypeDiff !== 0) return contentTypeDiff;
  const userDiff = (a.user || '').localeCompare(b.user || '');
  if (userDiff !== 0) return userDiff;
  return (a.sourceOrder || 0) - (b.sourceOrder || 0);
}

function validateEntry(entry, index) {
  if (!entry || typeof entry !== 'object') {
    return {
      valid: false,
      error: {
        index,
        reason: 'entry must be an object',
        entry,
      },
    };
  }

  const errors = [];
  const timestamp = normalizeTimestampMs(entry.timestamp);

  if (!Number.isFinite(timestamp)) {
    errors.push('invalid timestamp');
  }
  if (!entry.operation || typeof entry.operation !== 'string') {
    errors.push('missing operation');
  }
  if (!entry.path || typeof entry.path !== 'string') {
    errors.push('missing path');
  }
  if (!entry.resourcePath || typeof entry.resourcePath !== 'string') {
    errors.push('missing resourcePath');
  }
  if (!entry.contentType || typeof entry.contentType !== 'string') {
    errors.push('missing contentType');
  }

  if (errors.length) {
    return {
      valid: false,
      error: {
        index,
        reason: errors.join(', '),
        entry,
      },
    };
  }

  const normalizedOriginalFilename = normalizeOriginalFilenameValue(entry.originalFilename);
  return {
    valid: true,
    entry: {
      ...entry,
      ...(normalizedOriginalFilename
        ? { originalFilename: normalizedOriginalFilename }
        : {}),
      timestamp,
      sourceOrder: index,
    },
  };
}

function toStoredEntry(entry) {
  const { sourceOrder, ...storedEntry } = entry;
  return storedEntry;
}

async function streamToBuffer(stream) {
  if (typeof stream?.transformToByteArray === 'function') {
    const bytes = await stream.transformToByteArray();
    return Buffer.from(bytes);
  }

  const chunks = [];
  // eslint-disable-next-line no-restricted-syntax
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

async function getObjectText(s3, bucket, key) {
  try {
    const result = await s3.send(new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    }));
    const buffer = await streamToBuffer(result.Body);
    return buffer.toString('utf8');
  } catch (err) {
    if (err?.$metadata?.httpStatusCode === 404 || err?.name === 'NoSuchKey') {
      return null;
    }
    throw err;
  }
}

async function getObjectEntries(s3, bucket, key) {
  try {
    const result = await s3.send(new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    }));
    const gzBuffer = await streamToBuffer(result.Body);
    return JSON.parse(zlib.gunzipSync(gzBuffer).toString('utf8'));
  } catch (err) {
    if (err?.$metadata?.httpStatusCode === 404 || err?.name === 'NoSuchKey') {
      return null;
    }
    throw err;
  }
}

async function listObjectKeys(s3, bucket, prefix) {
  async function fetchPage(continuationToken) {
    const result = await s3.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      MaxKeys: LIST_OBJECTS_PAGE_SIZE,
      ContinuationToken: continuationToken,
    }));
    const keys = (result.Contents || [])
      .map((item) => item?.Key)
      .filter(Boolean);
    if (!result.IsTruncated || !result.NextContinuationToken) {
      return keys;
    }

    return [
      ...keys,
      ...await fetchPage(result.NextContinuationToken),
    ];
  }

  return fetchPage(undefined);
}

async function headObjectMetadata(s3, bucket, key) {
  const result = await s3.send(new HeadObjectCommand({
    Bucket: bucket,
    Key: key,
  }));
  return result.Metadata || {};
}

async function mapWithConcurrency(items, concurrency, mapper) {
  if (!items.length) {
    return [];
  }

  const maxConcurrency = Math.max(1, Math.min(concurrency, items.length));
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < items.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      // Each worker intentionally processes one async task at a time.
      // eslint-disable-next-line no-await-in-loop
      results[currentIndex] = await mapper(items[currentIndex], currentIndex);
    }
  }

  await Promise.all(Array.from({ length: maxConcurrency }, () => worker()));
  return results;
}

function createRedirectRecoveryReport(contentBusId) {
  return {
    enabled: true,
    bucket: CONTENT_BUS_BUCKET,
    prefix: `${contentBusId}/preview/`,
    candidateEntryCount: 0,
    listedObjectCount: 0,
    candidateKeyCount: 0,
    headRequestCount: 0,
    matchedRedirectCount: 0,
    discardedNonMediaRedirectCount: 0,
    resolvedCount: 0,
    unresolvedCount: 0,
    unresolved: [],
    collisionCount: 0,
    collisions: [],
    errorCount: 0,
    errors: [],
  };
}

function shouldWriteRedirectRecoveryReport(report) {
  return report
    && (
      report.unresolvedCount > 0
      || report.collisionCount > 0
      || report.errorCount > 0
    );
}

async function recoverOriginalFilenamesFromPreviewRedirects(s3, contentBusId, entries) {
  const report = createRedirectRecoveryReport(contentBusId);
  const candidateEntries = entries.filter(needsOriginalFilenameRecovery);
  report.candidateEntryCount = candidateEntries.length;

  if (!candidateEntries.length) {
    logStage('Skipping preview redirect recovery; no hashed originalFilename candidates were found.');
    return { entries, report };
  }

  const targetMediaHashes = new Set(
    candidateEntries
      .map(getEntryMediaHash)
      .filter(Boolean),
  );

  logStage(
    `Starting preview redirect scan in ${CONTENT_BUS_BUCKET}/${report.prefix} `
      + `(ListObjectsV2 page size ${LIST_OBJECTS_PAGE_SIZE}, `
      + `HEAD concurrency ${HEAD_REQUEST_CONCURRENCY}).`,
  );
  const listedKeys = await listObjectKeys(s3, CONTENT_BUS_BUCKET, report.prefix);
  report.listedObjectCount = listedKeys.length;

  const candidateKeys = listedKeys.filter((key) => isPreviewMediaCandidateKey(key, contentBusId));
  report.candidateKeyCount = candidateKeys.length;
  report.headRequestCount = candidateKeys.length;
  logStage(
    `Listed ${report.listedObjectCount} preview object(s); `
      + `${report.candidateKeyCount} candidate media path object(s) will be checked via HEAD `
      + `with concurrency ${Math.min(HEAD_REQUEST_CONCURRENCY, Math.max(candidateKeys.length, 1))}.`,
  );

  const redirectsByMediaHash = new Map();
  const collisions = new Map();
  const redirectTargetsByMediaHash = new Map();

  const headResults = await mapWithConcurrency(
    candidateKeys,
    HEAD_REQUEST_CONCURRENCY,
    async (key) => ({
      key,
      metadata: await headObjectMetadata(s3, CONTENT_BUS_BUCKET, key),
    }),
  );

  headResults.forEach(({ key, metadata }) => {
    const redirectLocation = metadata['redirect-location'];
    if (!redirectLocation) {
      return;
    }

    const targetMediaHash = extractMediaHash(redirectLocation);
    if (!targetMediaHash) {
      report.discardedNonMediaRedirectCount += 1;
      return;
    }
    if (!targetMediaHashes.has(targetMediaHash)) {
      return;
    }

    const originalPath = previewKeyToOriginalPath(key, contentBusId);
    const targetPathname = extractHashedMediaPathname(redirectLocation);
    report.matchedRedirectCount += 1;
    const existing = redirectsByMediaHash.get(targetMediaHash);
    const redirectTargets = redirectTargetsByMediaHash.get(targetMediaHash) || [];
    if (targetPathname && !redirectTargets.includes(targetPathname)) {
      redirectTargets.push(targetPathname);
      redirectTargetsByMediaHash.set(targetMediaHash, redirectTargets);
    }
    if (!existing) {
      redirectsByMediaHash.set(targetMediaHash, originalPath);
      collisions.set(targetMediaHash, [originalPath]);
      return;
    }

    if (existing === originalPath) {
      return;
    }

    const paths = collisions.get(targetMediaHash) || [existing];
    if (!paths.includes(originalPath)) {
      paths.push(originalPath);
    }
    collisions.set(targetMediaHash, paths);
    redirectsByMediaHash.set(
      targetMediaHash,
      choosePreferredOriginalPath(existing, originalPath),
    );
  });

  report.collisions = [...collisions.entries()]
    .filter(([, originalPaths]) => originalPaths.length > 1)
    .map(([mediaHash, originalPaths]) => ({
      mediaHash,
      chosenOriginalFilename: redirectsByMediaHash.get(mediaHash),
      originalPaths: [...originalPaths].sort((a, b) => a.length - b.length || a.localeCompare(b)),
      redirectTargets: [...(redirectTargetsByMediaHash.get(mediaHash) || [])]
        .sort((a, b) => a.localeCompare(b)),
    }));
  report.collisionCount = report.collisions.length;

  let resolvedCount = 0;
  const unresolved = [];
  const recoveredEntries = entries.map((entry) => {
    if (!needsOriginalFilenameRecovery(entry)) {
      return entry;
    }

    const mediaHash = getEntryMediaHash(entry);
    const originalFilename = redirectsByMediaHash.get(mediaHash);
    const fallbackOriginalFilename = normalizeOriginalFilenameValue(
      entry.originalFilename || entry.path,
    );
    if (!originalFilename) {
      unresolved.push({
        path: entry.path,
        mediaHash,
        originalFilename: fallbackOriginalFilename,
      });
      if (!fallbackOriginalFilename || entry.originalFilename === fallbackOriginalFilename) {
        return entry;
      }
      return {
        ...entry,
        originalFilename: fallbackOriginalFilename,
      };
    }

    if (entry.originalFilename !== originalFilename) {
      resolvedCount += 1;
    }
    return {
      ...entry,
      originalFilename,
    };
  });

  report.resolvedCount = resolvedCount;
  report.unresolved = unresolved;
  report.unresolvedCount = unresolved.length;

  logStage(
    'Preview redirect recovery complete: '
      + `${report.resolvedCount} entry update(s), `
      + `${report.unresolvedCount} unresolved, `
      + `${report.collisionCount} collision group(s), `
      + `${report.discardedNonMediaRedirectCount} non-media redirect(s) discarded.`,
  );

  return {
    entries: recoveredEntries,
    report,
  };
}

async function readExistingIndex(s3, bucket, prefix) {
  const rawIndex = await getObjectText(s3, bucket, `${prefix}/${INDEX_FILE}`);
  if (!rawIndex) {
    return {
      rawIndex: '',
      files: [],
    };
  }

  return {
    rawIndex,
    files: rawIndex.split('\n').map((line) => line.trim()).filter(Boolean),
  };
}

async function getEarliestExistingState(s3, bucket, prefix, existingFiles) {
  async function inspectFile(index) {
    if (index >= existingFiles.length) {
      return {
        basename: '',
        firstTimestamp: null,
      };
    }

    const basename = existingFiles[index];
    const key = `${prefix}/${basename}.gz`;
    const entries = await getObjectEntries(s3, bucket, key);
    if (!Array.isArray(entries) || entries.length === 0) {
      return inspectFile(index + 1);
    }

    const firstTimestamp = normalizeTimestampMs(entries[0].timestamp);
    if (!Number.isFinite(firstTimestamp)) {
      return inspectFile(index + 1);
    }

    return {
      basename,
      firstTimestamp,
    };
  }

  return inspectFile(0);
}

function gzipEntries(serializedEntries) {
  const gzipBuffer = zlib.gzipSync(serializedEntries);
  return { gzipBuffer };
}

function measureChunk(entries, startIndex, entryCount, measurementCache) {
  if (measurementCache.has(entryCount)) {
    return measurementCache.get(entryCount);
  }

  const chunkEntries = entries.slice(startIndex, startIndex + entryCount);
  const serializedEntries = JSON.stringify(chunkEntries);
  const measurement = {
    entryCount,
    entries: chunkEntries,
    serializedEntries,
    gzipBuffer: gzipEntries(serializedEntries).gzipBuffer,
  };
  measurementCache.set(entryCount, measurement);
  return measurement;
}

function createChunkBasename(firstTimestamp, chunkIndex, serializedEntries) {
  const hash = crypto
    .createHash('sha1')
    .update(serializedEntries)
    .digest('hex')
    .slice(0, 8)
    .toUpperCase();
  return `${formatTimestamp(firstTimestamp)}-${String(chunkIndex).padStart(6, '0')}-${hash}`;
}

function buildArtifacts(entries) {
  const artifacts = [];
  const packagingErrors = [];
  let chunkIndex = 0;
  let startIndex = 0;
  let preferredChunkEntryCount = 1;

  const appendArtifact = (measurement) => {
    const basename = createChunkBasename(
      measurement.entries[0].timestamp,
      chunkIndex,
      measurement.serializedEntries,
    );
    artifacts.push({
      basename,
      entries: measurement.entries,
      gzipBuffer: measurement.gzipBuffer,
      firstTimestamp: measurement.entries[0].timestamp,
      lastTimestamp: measurement.entries[measurement.entries.length - 1].timestamp,
    });
    chunkIndex += 1;
  };

  while (startIndex < entries.length) {
    const remainingEntryCount = entries.length - startIndex;
    const measurementCache = new Map();
    const measure = (entryCount) => measureChunk(entries, startIndex, entryCount, measurementCache);

    const singleEntryChunk = measure(1);
    if (singleEntryChunk.gzipBuffer.length > MAX_OBJECT_SIZE) {
      packagingErrors.push({
        reason: `single entry exceeds medialog object size limit (${singleEntryChunk.gzipBuffer.length} bytes gzip)`,
        entry: entries[startIndex],
      });
      startIndex += 1;
      preferredChunkEntryCount = 1;
      continue;
    }

    let bestChunk = singleEntryChunk;
    let low = 1;
    let high = remainingEntryCount + 1;
    let probe = Math.max(2, Math.min(preferredChunkEntryCount, remainingEntryCount));

    while (probe > low && probe < high) {
      const candidateChunk = measure(probe);
      if (candidateChunk.gzipBuffer.length <= MAX_OBJECT_SIZE) {
        bestChunk = candidateChunk;
        low = probe;

        if (probe === remainingEntryCount) {
          break;
        }

        const nextProbe = Math.min(remainingEntryCount, probe * 2);
        if (nextProbe === probe) {
          break;
        }
        probe = nextProbe;
        continue;
      }

      high = probe;
      break;
    }

    while (low + 1 < high) {
      const mid = Math.floor((low + high) / 2);
      const candidateChunk = measure(mid);
      if (candidateChunk.gzipBuffer.length <= MAX_OBJECT_SIZE) {
        bestChunk = candidateChunk;
        low = mid;
      } else {
        high = mid;
      }
    }

    appendArtifact(bestChunk);
    preferredChunkEntryCount = bestChunk.entryCount;
    startIndex += bestChunk.entryCount;
  }

  return { artifacts, packagingErrors };
}

function createSummary({
  args,
  bundle,
  outputDir,
  existingState,
  mergeableEntries,
  alreadyRecordedEntries,
  validationErrors,
  packagingErrors,
  artifacts,
  uploaded,
  redirectRecovery,
}) {
  const unmergedErrors = [...validationErrors, ...packagingErrors];

  return {
    generatedAt: new Date().toISOString(),
    bundlePath: args.bundle,
    target: {
      bucket: args.bucket,
      prefix: args.contentBusId,
    },
    dryRun: args.dryRun,
    bundleSummary: bundle.summary || {},
    existingMedialog: {
      hasIndex: existingState.files.length > 0,
      fileCount: existingState.files.length,
      earliestExistingTimestamp: existingState.earliestTimestamp
        ? new Date(existingState.earliestTimestamp).toISOString()
        : null,
      earliestExistingBasename: existingState.earliestBasename || null,
    },
    importPlan: {
      validEntryCount: mergeableEntries.length + alreadyRecordedEntries.length,
      mergeableCount: mergeableEntries.length,
      alreadyRecordedSkippedCount: alreadyRecordedEntries.length,
      alreadyRecordedRange: createTimestampRange(alreadyRecordedEntries),
      unmergedErrorCount: unmergedErrors.length,
    },
    redirectRecovery: redirectRecovery || createRedirectRecoveryReport(args.contentBusId),
    output: {
      directory: outputDir,
      uploaded,
      files: artifacts.map((artifact) => ({
        basename: artifact.basename,
        entryCount: artifact.entries.length,
        gzipBytes: artifact.gzipBuffer.length,
        firstTimestamp: new Date(artifact.firstTimestamp).toISOString(),
        lastTimestamp: new Date(artifact.lastTimestamp).toISOString(),
      })),
    },
  };
}

function writeJson(filepath, value) {
  fs.writeFileSync(filepath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function writeLocalArtifacts({
  outputDir,
  artifacts,
  newIndexContents,
  existingIndex,
  summary,
  unmergedErrors,
  redirectRecoveryReport,
}) {
  fs.mkdirSync(outputDir, { recursive: true });

  artifacts.forEach((artifact) => {
    fs.writeFileSync(path.join(outputDir, `${artifact.basename}.gz`), artifact.gzipBuffer);
  });

  if (newIndexContents) {
    fs.writeFileSync(path.join(outputDir, INDEX_FILE), `${newIndexContents}\n`, 'utf8');
  }

  if (existingIndex) {
    fs.writeFileSync(path.join(outputDir, 'existing-index.txt'), existingIndex, 'utf8');
  }

  writeJson(path.join(outputDir, 'import-summary.json'), summary);

  if (shouldWriteRedirectRecoveryReport(redirectRecoveryReport)) {
    writeJson(path.join(outputDir, REDIRECT_REPORT_FILE), redirectRecoveryReport);
  }

  if (unmergedErrors.length) {
    writeJson(path.join(outputDir, 'unmerged-errors.json'), unmergedErrors);
  }
}

async function uploadArtifacts({
  s3,
  bucket,
  prefix,
  artifacts,
  newIndexContents,
}) {
  logStage(`Uploading ${artifacts.length} media-log artifact(s) to ${bucket}/${prefix}`);
  await artifacts.reduce(
    (promise, artifact) => promise.then(() => s3.send(new PutObjectCommand({
      Bucket: bucket,
      Key: `${prefix}/${artifact.basename}.gz`,
      Body: artifact.gzipBuffer,
      ContentType: 'application/json',
      ContentEncoding: 'gzip',
      Metadata: {
        'last-event-time': formatTimestamp(artifact.lastTimestamp),
      },
    }))),
    Promise.resolve(),
  );
  logStage(`Uploaded ${artifacts.length} media-log artifact(s) to ${bucket}/${prefix}`);

  if (newIndexContents) {
    logStage(`Uploading media-log ${INDEX_FILE} to ${bucket}/${prefix}/${INDEX_FILE}`);
    await s3.send(new PutObjectCommand({
      Bucket: bucket,
      Key: `${prefix}/${INDEX_FILE}`,
      Body: newIndexContents,
      ContentType: 'text/plain',
    }));
    logStage(`Uploaded media-log ${INDEX_FILE} to ${bucket}/${prefix}/${INDEX_FILE}`);
  }
}

function writeSummaryArtifact({
  outputDir,
  artifacts,
  newIndexContents,
  existingIndex,
  summary,
  unmergedErrors,
  redirectRecoveryReport,
}) {
  writeLocalArtifacts({
    outputDir,
    artifacts,
    newIndexContents,
    existingIndex,
    summary,
    unmergedErrors,
    redirectRecoveryReport,
  });
}

function printSummary(summary, artifacts) {
  console.log(`Bucket: ${summary.target.bucket}`);
  console.log(`Target prefix (contentBusId): ${summary.target.prefix}`);
  console.log(
    `Earliest existing medialog timestamp: ${
      summary.existingMedialog.earliestExistingTimestamp || 'none'
    }`,
  );
  console.log(`Mergeable entries: ${summary.importPlan.mergeableCount}`);
  console.log(`Already recorded/skipped: ${summary.importPlan.alreadyRecordedSkippedCount}`);
  if (
    summary.importPlan.alreadyRecordedRange.first
    || summary.importPlan.alreadyRecordedRange.last
  ) {
    console.log(
      `Already recorded range: ${summary.importPlan.alreadyRecordedRange.first || 'n/a'} -> `
        + `${summary.importPlan.alreadyRecordedRange.last || 'n/a'}`,
    );
  }
  console.log(`True unmerged errors: ${summary.importPlan.unmergedErrorCount}`);
  console.log(`Redirect recovery resolved: ${summary.redirectRecovery.resolvedCount}`);
  console.log(`Redirect recovery unresolved: ${summary.redirectRecovery.unresolvedCount}`);
  console.log(`Redirect collisions: ${summary.redirectRecovery.collisionCount}`);
  console.log(`Output directory: ${summary.output.directory}`);
  console.log(`Artifacts generated: ${artifacts.length}`);
}

async function main(argv = process.argv.slice(2)) {
  const runStartedAt = Date.now();
  startRunLogging(runStartedAt);
  const finishRun = (code, statusMessage) => {
    logStage(`${statusMessage} Total duration: ${formatDuration(Date.now() - runStartedAt)}.`);
    return code;
  };

  let args;
  try {
    args = parseArgs(argv);
  } catch (err) {
    logError(err.message);
    printUsage();
    return finishRun(1, 'Run failed.');
  }

  if (args.help) {
    printUsage();
    return finishRun(0, 'Run complete.');
  }

  if (!args.bundle) {
    logError('Missing required --bundle');
    printUsage();
    return finishRun(1, 'Run failed.');
  }

  if (!args.contentBusId) {
    logError('Missing required --content-bus-id');
    return finishRun(1, 'Run failed.');
  }

  const { bucket: mediaLogBucket, compatibilityArgUsed } = resolveMediaLogBucket(args.bucket);
  args = {
    ...args,
    bucket: mediaLogBucket,
  };

  const bundlePath = path.resolve(args.bundle);
  const outputDir = path.resolve(
    args.outputDir
      || path.join(
        path.dirname(bundlePath),
        `${path.basename(bundlePath, path.extname(bundlePath))}-artifacts`,
      ),
  );

  if (compatibilityArgUsed) {
    logStage(`--bucket accepted for compatibility; media-log bucket is fixed to ${mediaLogBucket}.`);
  }

  logStage(`Loading bundle from ${bundlePath}`);
  const bundle = JSON.parse(fs.readFileSync(bundlePath, 'utf8'));
  if (!Array.isArray(bundle.entries)) {
    throw new Error('Bundle does not contain an entries array');
  }
  logStage(`Loaded bundle with ${bundle.entries.length} raw entr${bundle.entries.length === 1 ? 'y' : 'ies'}.`);

  logStage('Validating bundle entries...');
  const validationErrors = [];
  const validEntries = [];
  bundle.entries.forEach((entry, index) => {
    const validated = validateEntry(entry, index);
    if (!validated.valid) {
      validationErrors.push(validated.error);
      return;
    }
    validEntries.push(validated.entry);
  });
  validEntries.sort(compareEntries);
  logStage(`Validation complete: ${validEntries.length} valid, ${validationErrors.length} invalid.`);

  const s3 = new S3Client(args.region ? { region: args.region } : {});
  logStage(`Reading existing media-log index from ${mediaLogBucket}/${args.contentBusId}/${INDEX_FILE}`);
  const existingIndex = await readExistingIndex(s3, mediaLogBucket, args.contentBusId);
  const earliestExistingState = await getEarliestExistingState(
    s3,
    mediaLogBucket,
    args.contentBusId,
    existingIndex.files,
  );

  if (existingIndex.files.length > 0 && !Number.isFinite(earliestExistingState.firstTimestamp)) {
    throw new Error(
      `Existing medialog index found at ${mediaLogBucket}/${args.contentBusId}, `
      + 'but the earliest existing timestamp could not be determined safely.',
    );
  }
  logStage(
    `Existing media-log boundary: ${
      Number.isFinite(earliestExistingState.firstTimestamp)
        ? new Date(earliestExistingState.firstTimestamp).toISOString()
        : 'none'
    }`,
  );

  let redirectRecoveryReport = createRedirectRecoveryReport(args.contentBusId);
  let enrichedEntries = validEntries;
  try {
    const recovered = await recoverOriginalFilenamesFromPreviewRedirects(
      s3,
      args.contentBusId,
      validEntries,
    );
    enrichedEntries = recovered.entries;
    redirectRecoveryReport = recovered.report;
  } catch (err) {
    redirectRecoveryReport = createRedirectRecoveryReport(args.contentBusId);
    redirectRecoveryReport.errorCount = 1;
    redirectRecoveryReport.errors = [{
      message: err.message,
    }];
    logStage(`Preview redirect recovery failed; aborting before upload: ${err.message}`);

    const failureSummary = createSummary({
      args: { ...args, bundle: bundlePath },
      bundle,
      outputDir,
      existingState: {
        files: existingIndex.files,
        earliestTimestamp: earliestExistingState.firstTimestamp,
        earliestBasename: earliestExistingState.basename,
      },
      mergeableEntries: [],
      alreadyRecordedEntries: [],
      validationErrors,
      packagingErrors: [],
      artifacts: [],
      uploaded: false,
      redirectRecovery: redirectRecoveryReport,
    });

    logStage(`Writing failure reports to ${outputDir}`);
    writeSummaryArtifact({
      outputDir,
      artifacts: [],
      newIndexContents: '',
      existingIndex: existingIndex.rawIndex,
      summary: failureSummary,
      unmergedErrors: validationErrors,
      redirectRecoveryReport,
    });
    printSummary(failureSummary, []);
    return finishRun(1, 'Run failed before upload.');
  }

  const existingBoundary = earliestExistingState.firstTimestamp;
  const mergeableEntries = [];
  const alreadyRecordedEntries = [];

  logStage('Applying existing-history boundary to validated entries...');
  enrichedEntries.forEach((entry) => {
    const storedEntry = toStoredEntry(entry);
    if (Number.isFinite(existingBoundary) && entry.timestamp >= existingBoundary) {
      alreadyRecordedEntries.push(storedEntry);
      return;
    }
    mergeableEntries.push(storedEntry);
  });
  logStage(
    `Existing-history boundary applied: ${mergeableEntries.length} mergeable, `
      + `${alreadyRecordedEntries.length} already recorded/skipped.`,
  );

  logStage(`Packaging ${mergeableEntries.length} mergeable entr${mergeableEntries.length === 1 ? 'y' : 'ies'} into media-log artifacts...`);
  const { artifacts, packagingErrors } = buildArtifacts(mergeableEntries);
  const importableEntries = artifacts.flatMap((artifact) => artifact.entries);
  const newBasenames = artifacts.map((artifact) => artifact.basename);
  const newIndexContents = [...newBasenames, ...existingIndex.files].join('\n');
  const uploaded = false;
  logStage(`Artifact packaging complete: ${artifacts.length} artifact(s), ${packagingErrors.length} packaging error(s).`);

  let summary = createSummary({
    args: { ...args, bundle: bundlePath },
    bundle,
    outputDir,
    existingState: {
      files: existingIndex.files,
      earliestTimestamp: earliestExistingState.firstTimestamp,
      earliestBasename: earliestExistingState.basename,
    },
    mergeableEntries: importableEntries,
    alreadyRecordedEntries,
    validationErrors,
    packagingErrors,
    artifacts,
    uploaded,
    redirectRecovery: redirectRecoveryReport,
  });

  const unmergedErrors = [...validationErrors, ...packagingErrors];
  logStage(`Writing local artifacts and reports to ${outputDir}`);
  writeSummaryArtifact({
    outputDir,
    artifacts,
    newIndexContents,
    existingIndex: existingIndex.rawIndex,
    summary,
    unmergedErrors,
    redirectRecoveryReport,
  });

  printSummary(summary, artifacts);

  if (args.dryRun) {
    logStage('Dry run enabled. No remote writes performed.');
    return finishRun(
      unmergedErrors.length ? 2 : 0,
      unmergedErrors.length ? 'Dry run complete with unmerged errors.' : 'Dry run complete.',
    );
  }

  if (!artifacts.length) {
    logStage('No mergeable entries remain after applying the existing-history boundary. No upload performed.');
    return finishRun(
      unmergedErrors.length ? 2 : 0,
      unmergedErrors.length
        ? 'Run complete with unmerged errors and no upload.'
        : 'Run complete with no upload needed.',
    );
  }

  await uploadArtifacts({
    s3,
    bucket: mediaLogBucket,
    prefix: args.contentBusId,
    artifacts,
    newIndexContents,
  });

  summary = createSummary({
    args: { ...args, bundle: bundlePath },
    bundle,
    outputDir,
    existingState: {
      files: existingIndex.files,
      earliestTimestamp: earliestExistingState.firstTimestamp,
      earliestBasename: earliestExistingState.basename,
    },
    mergeableEntries: importableEntries,
    alreadyRecordedEntries,
    validationErrors,
    packagingErrors,
    artifacts,
    uploaded: true,
    redirectRecovery: redirectRecoveryReport,
  });
  logStage(`Writing final import summary to ${outputDir}`);
  writeSummaryArtifact({
    outputDir,
    artifacts,
    newIndexContents,
    existingIndex: existingIndex.rawIndex,
    summary,
    unmergedErrors,
    redirectRecoveryReport,
  });

  logStage('Upload complete.');
  return finishRun(
    unmergedErrors.length ? 2 : 0,
    unmergedErrors.length ? 'Run complete with unmerged errors.' : 'Run complete.',
  );
}

export {
  CONTENT_BUS_BUCKET,
  MEDIA_LOG_BUCKET,
  REDIRECT_REPORT_FILE,
  buildArtifacts,
  resolveMediaLogBucket,
  normalizePathname,
  extractHashedMediaPathname,
  extractMediaHash,
  isHashedMediaReference,
  normalizeOriginalFilenameValue,
  needsOriginalFilenameRecovery,
  previewKeyToOriginalPath,
  isPreviewMediaCandidateKey,
  choosePreferredOriginalPath,
  createRedirectRecoveryReport,
  recoverOriginalFilenamesFromPreviewRedirects,
  uploadArtifacts,
  main,
};

function isDirectExecution() {
  if (!process.argv[1]) {
    return false;
  }
  return import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href;
}

if (isDirectExecution()) {
  main().then((code) => {
    process.exit(code);
  }).catch((err) => {
    logError(err.stack || err.message);
    if (loggingState.runStartedAt) {
      logError(`Run failed. Total duration: ${formatDuration(Date.now() - loggingState.runStartedAt)}.`);
    }
    process.exit(1);
  });
}
