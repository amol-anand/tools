# Backfill

One-time medialog importer for historical media-library backfill bundles.

The tool reads an exported medialog bundle JSON, validates and sorts the entries,
builds medialog `.gz` objects plus `.index`, and uploads them to S3.

## Install

```bash
cd /Users/amol/Documents/git-repos/amol-anand/tools/tools/backfill
npm install
```

## Inputs

- `--bundle`: path to the exported medialog bundle JSON
- `--bucket`: target S3 bucket
- `--content-bus-id`: folder/prefix inside the bucket
- `--region`: optional AWS region override
- `--output-dir`: optional local output directory for generated artifacts and reports
- `--dry-run`: generate artifacts locally without uploading to S3

## AWS Credentials

The tool uses the AWS SDK default credential chain. Common options are:

- `AWS_PROFILE=<profile-name>`
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`

Examples:

```bash
AWS_PROFILE=my-profile \
node tools/backfill/index.js \
  --bundle /path/to/medialog-import-bundle.json \
  --bucket helix-media-logs \
  --content-bus-id <contentBusId> \
  --dry-run
```

```bash
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
AWS_SESSION_TOKEN=... \
node tools/backfill/index.js \
  --bundle /path/to/medialog-import-bundle.json \
  --bucket helix-media-logs \
  --content-bus-id <contentBusId>
```

## Behavior

- If no existing `.index` is found, all valid entries are imported.
- If an existing `.index` is found, the tool prepends only entries that are strictly older than the earliest existing medialog entry.
- Entries at or after the earliest existing timestamp are treated as already recorded and skipped.
- Validation or packaging failures are reported separately in `unmerged-errors.json`.
- Data files are uploaded first and `.index` is uploaded last.

## Output

By default, artifacts are written next to the bundle in a sibling folder named like:

```text
<bundle-name>-artifacts/
```

That folder contains:

- generated medialog `.gz` files
- `.index`
- `import-summary.json`
- `existing-index.txt` when an index already exists in S3
- `unmerged-errors.json` when any entries fail validation or packaging

## Typical Workflow

1. Export a medialog bundle from the media-library backfill tool.
2. Run this CLI with `--dry-run`.
3. Review `import-summary.json`, especially `earliestExistingTimestamp`, skipped counts, and any unmerged errors.
4. Re-run without `--dry-run` to upload to S3.

## Help

```bash
node tools/backfill/index.js --help
```
