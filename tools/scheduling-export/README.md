# Scheduling Export

CLI that reads Cloudflare R2 (`helix-snapshot-scheduler`) and writes a
single CSV combining every successfully published page and every
failed publish inside a configurable trailing window, ordered newest
first. A `status` column (`completed` / `failed`) distinguishes them.
Opens cleanly in Excel, Numbers, and Google Sheets.

The companion to [`tools/scheduling-dashboard/`](../scheduling-dashboard/),
which produces an interactive HTML dashboard.

## Install

```bash
cd tools/scheduling-export
npm install
```

## Credentials

Provide three values (R2 token with read access to the bucket):

- `CLOUDFLARE_R2_ACCOUNT_ID`
- `CLOUDFLARE_R2_ACCESS_KEY_ID`
- `CLOUDFLARE_R2_SECRET_ACCESS_KEY`

The CLI auto-loads, in order:

1. `tools/scheduling-export/.env`
2. `tools/scheduling-dashboard/.env` (so you don't need a duplicate
   file if you already configured the dashboard)

You can also pass `--env-file /path/to/.env`. Real environment
variables always win over `.env` values. Both `.env` paths are
gitignored.

## Usage

```bash
node tools/scheduling-export/index.js \
  [--bucket helix-snapshot-scheduler] \
  [--output-dir ./tools/scheduling-export/output] \
  [--days 120 | --days all] \
  [--source both | completed | failed] \
  [--env-file /path/to/.env]
```

### Flags

- `--bucket` - defaults to `helix-snapshot-scheduler`.
- `--output-dir` - defaults to `tools/scheduling-export/output/`.
- `--days N` - trailing window in days. Default `120`. Pass
  `--days all` to include everything in the bucket.
- `--source` - `both` (default), `completed`, or `failed`. With
  `both`, completed and failed records share a single CSV with a
  `status` column. With `completed` or `failed`, only that kind is
  written (still in one file). Pending (still-in-`schedule.json`)
  entries are never included.
- `--env-file <path>` - explicit `.env` path. Overrides auto-load.

### Filtering & ordering

A record is kept when its event timestamp falls inside the window:

- For completed rows: `publishedAt` (falling back to
  `scheduledPublish` if missing).
- For failed rows: `failedAt` (falling back to `timestamp`, then
  `scheduledPublish`).

The full CSV is sorted by that same per-row event timestamp, newest
first - so completed and failed rows are interleaved chronologically.

### Output

A single file in `output-dir`:

- `--source both` (default) -> `scheduling_<from>_to_<to>.csv`
- `--source completed` -> `completed_<from>_to_<to>.csv`
- `--source failed` -> `failed_<from>_to_<to>.csv`

`<from>` is `all` when `--days all` is used, otherwise the inclusive
window start. `<to>` is today's UTC date.

The CSV is UTF-8 with a BOM (so Excel auto-detects encoding), uses
RFC-4180 quoting (fields containing `,`, `"`, CR, or LF are quoted and
embedded `"` are doubled), and CRLF line endings.

### Columns

Preferred columns appear first, then any extra fields the records
carry (alphabetical), with `sourceFile` always last:

```
status, org, site, path, type, scheduledPublish, publishedAt,
publishedBy, failedAt, timestamp, reason, userId, messageId,
...extras..., sourceFile
```

`status` is always `completed` or `failed`. Cells absent on a given
row come out empty. The header is always written so the shape is
stable across runs even if some columns are unused.

## Examples

Last 120 days of completed and failed publishes in a single CSV (the
default - what you usually want):

```bash
node tools/scheduling-export/index.js
```

Just successful publishes:

```bash
node tools/scheduling-export/index.js --source completed
```

Everything we have, completed and failed combined:

```bash
node tools/scheduling-export/index.js --days all
```

Custom output location:

```bash
node tools/scheduling-export/index.js \
  --days 120 \
  --output-dir ~/Desktop/snapshot-exports
```
