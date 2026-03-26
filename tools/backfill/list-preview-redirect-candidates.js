#!/usr/bin/env node
/* eslint-disable no-console */

import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { CONTENT_BUS_BUCKET, isPreviewMediaCandidateKey } from './index.js';

function printUsage() {
  console.log(`
Usage:
  node tools/backfill/list-preview-redirect-candidates.js --content-bus-id <contentBusId> [--sample-size 32] [--region us-east-1]

Options:
  --content-bus-id   Required. Content bus prefix to inspect.
  --sample-size      Optional. Number of candidate keys to print. Default: 32.
  --region           Optional AWS region override. Default: us-east-1.
  --help             Show this help.

Behavior:
  - Reads only from ${CONTENT_BUS_BUCKET}/${'<contentBusId>'}/preview/
  - Lists preview objects and applies the same candidate-key filter as the backfill CLI
  - Prints a JSON summary with counts, extension breakdown, and a sample of candidate keys
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
    region: 'us-east-1',
    sampleSize: 32,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    switch (arg) {
      case '--content-bus-id':
        args.contentBusId = argv[i + 1];
        i += 1;
        break;
      case '--sample-size':
        args.sampleSize = Number.parseInt(argv[i + 1], 10);
        i += 1;
        break;
      case '--region':
        args.region = argv[i + 1];
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

  args.contentBusId = normalizePrefix(args.contentBusId);
  args.region = normalizeString(args.region) || 'us-east-1';

  return args;
}

async function listObjectKeys(s3, bucket, prefix) {
  async function fetchPage(continuationToken) {
    const result = await s3.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
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

async function main(argv = process.argv.slice(2)) {
  let args;
  try {
    args = parseArgs(argv);
  } catch (err) {
    console.error(err.message);
    printUsage();
    return 1;
  }

  if (args.help) {
    printUsage();
    return 0;
  }

  if (!args.contentBusId) {
    console.error('Missing required --content-bus-id');
    printUsage();
    return 1;
  }

  if (!Number.isInteger(args.sampleSize) || args.sampleSize < 1) {
    console.error('--sample-size must be a positive integer');
    return 1;
  }

  const prefix = `${args.contentBusId}/preview/`;
  const s3 = new S3Client({ region: args.region });
  const listedKeys = await listObjectKeys(s3, CONTENT_BUS_BUCKET, prefix);
  const candidateKeys = listedKeys
    .filter((key) => isPreviewMediaCandidateKey(key, args.contentBusId));

  const byExt = candidateKeys.reduce((acc, key) => {
    const ext = (key.match(/\.([a-z0-9]+)$/i)?.[1] || '').toLowerCase() || 'unknown';
    acc[ext] = (acc[ext] || 0) + 1;
    return acc;
  }, {});

  console.log(JSON.stringify({
    bucket: CONTENT_BUS_BUCKET,
    prefix,
    listedObjectCount: listedKeys.length,
    candidateKeyCount: candidateKeys.length,
    sample: candidateKeys.slice(0, args.sampleSize).map((key) => key.slice(prefix.length)),
    byExt,
  }, null, 2));

  return 0;
}

main().then((code) => {
  process.exit(code);
}).catch((err) => {
  console.error(err.stack || err.message);
  process.exit(1);
});
