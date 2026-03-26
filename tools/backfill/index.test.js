import assert from 'node:assert/strict';
import test from 'node:test';
import { HeadObjectCommand, ListObjectsV2Command, PutObjectCommand } from '@aws-sdk/client-s3';

import {
  CONTENT_BUS_BUCKET,
  MEDIA_LOG_BUCKET,
  resolveMediaLogBucket,
  normalizePathname,
  extractHashedMediaPathname,
  extractMediaHash,
  normalizeOriginalFilenameValue,
  needsOriginalFilenameRecovery,
  previewKeyToOriginalPath,
  isPreviewMediaCandidateKey,
  recoverOriginalFilenamesFromPreviewRedirects,
  uploadArtifacts,
} from './index.js';

function createMockS3({ contents = [], metadataByKey = new Map(), headHandler } = {}) {
  const commands = [];

  return {
    commands,
    client: {
      async send(command) {
        commands.push(command);

        if (command instanceof ListObjectsV2Command) {
          return {
            Contents: contents.map((Key) => ({ Key })),
            IsTruncated: false,
          };
        }

        if (command instanceof HeadObjectCommand) {
          if (headHandler) {
            return headHandler(command, commands);
          }
          return {
            Metadata: metadataByKey.get(command.input.Key) || {},
          };
        }

        if (command instanceof PutObjectCommand) {
          return {};
        }

        throw new Error(`Unexpected command: ${command.constructor.name}`);
      },
    },
  };
}

test('resolveMediaLogBucket keeps media-log bucket fixed', () => {
  assert.deepStrictEqual(resolveMediaLogBucket(''), {
    bucket: MEDIA_LOG_BUCKET,
    compatibilityArgUsed: false,
  });
  assert.deepStrictEqual(resolveMediaLogBucket('helix-media-logs'), {
    bucket: MEDIA_LOG_BUCKET,
    compatibilityArgUsed: true,
  });
  assert.throws(
    () => resolveMediaLogBucket('other-bucket'),
    /--bucket is fixed to helix-media-logs/,
  );
});

test('pathname helpers detect hashed media references and recovery candidates', () => {
  assert.strictEqual(
    normalizePathname('https://main--repo--owner.aem.page/media_abc123.png?width=200#foo'),
    '/media_abc123.png',
  );
  assert.strictEqual(
    extractHashedMediaPathname('https://main--repo--owner.aem.page/media_abc123.png?width=200#foo'),
    '/media_abc123.png',
  );
  assert.strictEqual(
    extractMediaHash('https://main--repo--owner.aem.page/images/media_abc123.png?width=200#foo'),
    'abc123',
  );
  assert.strictEqual(
    normalizeOriginalFilenameValue('https://main--repo--owner.aem.page/media_abc123.png?width=200#foo'),
    '/media_abc123.png',
  );
  assert.strictEqual(extractHashedMediaPathname('/folder/sample.png'), '');
  assert.strictEqual(
    needsOriginalFilenameRecovery({
      path: 'https://main--repo--owner.aem.page/media_abc123.png',
      originalFilename: 'https://main--repo--owner.aem.page/media_abc123.png#width=20',
    }),
    true,
  );
  assert.strictEqual(
    needsOriginalFilenameRecovery({
      path: 'https://main--repo--owner.aem.page/media_abc123.png',
      originalFilename: '/folder/sample.png',
    }),
    false,
  );
});

test('preview key helpers keep only original asset paths', () => {
  assert.strictEqual(
    previewKeyToOriginalPath('foo-id/preview/path/to/sample.png', 'foo-id'),
    '/path/to/sample.png',
  );
  assert.strictEqual(
    isPreviewMediaCandidateKey('foo-id/preview/path/to/sample.png', 'foo-id'),
    true,
  );
  assert.strictEqual(
    isPreviewMediaCandidateKey('foo-id/preview/path/to/media_abc123.png', 'foo-id'),
    false,
  );
  assert.strictEqual(
    isPreviewMediaCandidateKey('foo-id/preview/path/to/index.md', 'foo-id'),
    false,
  );
});

test('recoverOriginalFilenamesFromPreviewRedirects uses only list/head and discards non-media redirects', async () => {
  const contentBusId = 'foo-id';
  const previewKeys = [
    `${contentBusId}/preview/parent/sample.png`,
    `${contentBusId}/preview/parent/external.png`,
    `${contentBusId}/preview/parent/media_deadbeef.png`,
    `${contentBusId}/preview/index.md`,
  ];
  const metadataByKey = new Map([
    [`${contentBusId}/preview/parent/sample.png`, {
      'redirect-location': '/parent/media_abc123.png?width=200#foo',
    }],
    [`${contentBusId}/preview/parent/external.png`, {
      'redirect-location': 'https://www.adobe.com/',
    }],
  ]);
  const { client, commands } = createMockS3({
    contents: previewKeys,
    metadataByKey,
  });

  const candidateEntries = [{
    path: 'https://main--repo--owner.aem.page/media_abc123.avif#width=200',
    originalFilename: 'https://main--repo--owner.aem.page/media_abc123.avif#width=200',
    mediaHash: 'abc123',
    timestamp: 1,
  }];
  const { entries, report } = await recoverOriginalFilenamesFromPreviewRedirects(
    client,
    contentBusId,
    candidateEntries,
  );

  assert.strictEqual(report.bucket, CONTENT_BUS_BUCKET);
  assert.strictEqual(report.candidateEntryCount, 1);
  assert.strictEqual(report.listedObjectCount, 4);
  assert.strictEqual(report.candidateKeyCount, 2);
  assert.strictEqual(report.headRequestCount, 2);
  assert.strictEqual(report.discardedNonMediaRedirectCount, 1);
  assert.strictEqual(report.resolvedCount, 1);
  assert.strictEqual(report.unresolvedCount, 0);
  assert.strictEqual(entries[0].originalFilename, '/parent/sample.png');
  assert.ok(commands.every((command) => (
    command instanceof ListObjectsV2Command || command instanceof HeadObjectCommand
  )));
  assert.strictEqual(commands[0].input.MaxKeys, 1000);
});

test('recoverOriginalFilenamesFromPreviewRedirects resolves collisions deterministically', async () => {
  const contentBusId = 'foo-id';
  const target = '/assets/media_abc123.png';
  const previewKeys = [
    `${contentBusId}/preview/longer/path/sample.png`,
    `${contentBusId}/preview/a.png`,
    `${contentBusId}/preview/b.png`,
  ];
  const metadataByKey = new Map(previewKeys.map((key) => [key, {
    'redirect-location': target,
  }]));
  const { client } = createMockS3({
    contents: previewKeys,
    metadataByKey,
  });

  const candidateEntries = [{
    path: `https://main--repo--owner.aem.page${target}`,
    originalFilename: `https://main--repo--owner.aem.page${target}`,
    mediaHash: 'abc123',
    timestamp: 1,
  }];
  const { entries, report } = await recoverOriginalFilenamesFromPreviewRedirects(
    client,
    contentBusId,
    candidateEntries,
  );

  assert.strictEqual(entries[0].originalFilename, '/a.png');
  assert.strictEqual(report.collisionCount, 1);
  assert.deepStrictEqual(report.collisions[0], {
    mediaHash: 'abc123',
    chosenOriginalFilename: '/a.png',
    originalPaths: ['/a.png', '/b.png', '/longer/path/sample.png'],
    redirectTargets: [target],
  });
});

test('recoverOriginalFilenamesFromPreviewRedirects keeps unresolved hashed entries as clean pathnames', async () => {
  const contentBusId = 'foo-id';
  const { client } = createMockS3({
    contents: [],
  });

  const candidateEntries = [{
    path: 'https://main--repo--owner.aem.page/media_deadbeef.avif?width=750&format=avif',
    originalFilename: 'https://main--repo--owner.aem.page/media_deadbeef.avif?width=750&format=avif',
    mediaHash: 'deadbeef',
    timestamp: 1,
  }];
  const { entries, report } = await recoverOriginalFilenamesFromPreviewRedirects(
    client,
    contentBusId,
    candidateEntries,
  );

  assert.strictEqual(entries[0].originalFilename, '/media_deadbeef.avif');
  assert.strictEqual(report.resolvedCount, 0);
  assert.strictEqual(report.unresolvedCount, 1);
  assert.deepStrictEqual(report.unresolved[0], {
    path: 'https://main--repo--owner.aem.page/media_deadbeef.avif?width=750&format=avif',
    mediaHash: 'deadbeef',
    originalFilename: '/media_deadbeef.avif',
  });
});

test('recoverOriginalFilenamesFromPreviewRedirects checks HEAD metadata concurrently', async () => {
  const contentBusId = 'foo-id';
  const previewKeys = Array.from({ length: 12 }, (_, index) => `${contentBusId}/preview/path/${index}.png`);
  let inFlightHeadRequests = 0;
  let maxInFlightHeadRequests = 0;
  const { client } = createMockS3({
    contents: previewKeys,
    headHandler: async (command) => {
      inFlightHeadRequests += 1;
      maxInFlightHeadRequests = Math.max(maxInFlightHeadRequests, inFlightHeadRequests);
      await new Promise((resolve) => {
        setTimeout(resolve, 10);
      });
      inFlightHeadRequests -= 1;
      const fileStem = command.input.Key.match(/\/(\d+)\.png$/)?.[1];
      return {
        Metadata: {
          'redirect-location': `/media_${fileStem}.png`,
        },
      };
    },
  });

  const candidateEntries = previewKeys.map((key, index) => ({
    path: `https://main--repo--owner.aem.page/media_${index}.png`,
    originalFilename: `https://main--repo--owner.aem.page/media_${index}.png`,
    mediaHash: String(index),
    timestamp: index,
    resourcePath: previewKeyToOriginalPath(key, contentBusId),
  }));

  const { report } = await recoverOriginalFilenamesFromPreviewRedirects(
    client,
    contentBusId,
    candidateEntries,
  );

  assert.strictEqual(report.resolvedCount, previewKeys.length);
  assert.ok(maxInFlightHeadRequests > 1);
});

test('uploadArtifacts writes only media-log put operations', async () => {
  const { client, commands } = createMockS3();

  await uploadArtifacts({
    s3: client,
    bucket: MEDIA_LOG_BUCKET,
    prefix: 'foo-id',
    artifacts: [{
      basename: '2026-03-26-000000-000000-AAAA0000',
      gzipBuffer: Buffer.from('gzip-data'),
      lastTimestamp: Date.UTC(2026, 2, 26, 12, 0, 0),
    }],
    newIndexContents: '2026-03-26-000000-000000-AAAA0000',
  });

  assert.strictEqual(commands.length, 2);
  assert.ok(commands.every((command) => command instanceof PutObjectCommand));
  assert.deepStrictEqual(commands.map((command) => command.input.Bucket), [
    MEDIA_LOG_BUCKET,
    MEDIA_LOG_BUCKET,
  ]);
  assert.deepStrictEqual(commands.map((command) => command.input.Key), [
    'foo-id/2026-03-26-000000-000000-AAAA0000.gz',
    'foo-id/.index',
  ]);
});
