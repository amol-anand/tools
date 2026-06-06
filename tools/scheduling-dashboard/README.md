# Scheduling Dashboard

CLI that reads Cloudflare R2 (`helix-snapshot-scheduler`) and produces a
self-contained static HTML dashboard summarizing the scheduled-publish
service: completions vs failures over time, top orgs/sites, top users,
failure reasons, and the current pending schedule.

The dashboard is a single HTML file you open with `file://` - no server,
no auth, no upload. Re-run the CLI to refresh.

## Install

```bash
cd tools/scheduling-dashboard
npm install
```

## Credentials

The CLI talks to R2 via the S3-compatible API. Create an R2 API token
with read access to the bucket and provide three values:

- `CLOUDFLARE_R2_ACCOUNT_ID`
- `CLOUDFLARE_R2_ACCESS_KEY_ID`
- `CLOUDFLARE_R2_SECRET_ACCESS_KEY`

You can supply them in either of two ways:

1. Real environment variables in your shell, or
2. A `.env` file. The CLI auto-loads
   `tools/scheduling-dashboard/.env` if it exists, or you can point it
   at another path with `--env-file /path/to/.env`. Real environment
   variables always take precedence over values in the file.

The `.env` file format is `KEY=value`, one per line. Lines starting
with `#` are comments. `tools/scheduling-dashboard/.env` is gitignored.

Example `tools/scheduling-dashboard/.env`:

```
CLOUDFLARE_R2_ACCOUNT_ID=...
CLOUDFLARE_R2_ACCESS_KEY_ID=...
CLOUDFLARE_R2_SECRET_ACCESS_KEY=...
```

## Usage

```bash
node tools/scheduling-dashboard/index.js \
  [--bucket helix-snapshot-scheduler] \
  [--output-dir ./tools/scheduling-dashboard/output] \
  [--days 90 | --days all] \
  [--env-file /path/to/.env] \
  [--open]
```

### Flags

- `--bucket` - defaults to `helix-snapshot-scheduler`.
- `--output-dir` - defaults to `tools/scheduling-dashboard/output/`. The
  CLI writes `dashboard.html` here.
- `--days N` - how many trailing days to include in the timeseries,
  per-day buckets, and "in-window" totals. Default `90`. Pass
  `--days all` to include everything found in the bucket.
- `--env-file <path>` - load credentials from a specific `.env` file.
  When omitted, the CLI auto-loads
  `tools/scheduling-dashboard/.env` if it exists.
- `--open` - after generating, open `dashboard.html` in the default
  browser (uses `open` on macOS, `xdg-open` on Linux, `start` on
  Windows).

### Example

With a local `tools/scheduling-dashboard/.env`:

```bash
node tools/scheduling-dashboard/index.js --days 30 --open
```

Without an `.env` file, exporting in the shell:

```bash
CLOUDFLARE_R2_ACCOUNT_ID=... \
CLOUDFLARE_R2_ACCESS_KEY_ID=... \
CLOUDFLARE_R2_SECRET_ACCESS_KEY=... \
node tools/scheduling-dashboard/index.js --days 30 --open
```

## What the dashboard shows

- Top stats: completed, failed, success rate, pending (with
  unapproved sub-count), distinct orgs / sites / users, window covered.
- Stacked bar chart of completed vs failed per day across the window.
- Horizontal stacked bar of top org/site by total volume (completed +
  failed + currently pending).
- Doughnut of failure reasons (in window).
- Horizontal stacked bar of top users by failed + pending.
- Doughnut of pending schedule by `type` (snapshot / page / unknown).
- Filterable table of the current pending schedule with org, site,
  name, type, scheduled time, approval state, and userId.
- Table of recent failures with org/site/path/userId/reason/failedAt.

## Data sources and limitations

The tool reads three things from the bucket:

- `completed/*.json` - one array of completion records per file.
  Records look like
  `{ org, site, path, scheduledPublish, publishedAt, publishedBy }`.
  These do **not** carry a `userId`, so user-level attribution is only
  available for failed and pending records. Org/site-level attribution
  works across all three sources.
- `failed/*.json` - one array per file. Records look like
  `{ org, site, path, scheduledPublish, type, userId, messageId, timestamp, failedAt, reason }`.
- `schedule.json` (root) - nested object keyed by `"<org>--<site>"`,
  each mapping to entries
  `{ type?, scheduledPublish, approved?, userId? }`. Legacy entries may
  be missing `type`, `approved`, or `userId`; the dashboard surfaces
  these as `unknown`.

## Output

`output/dashboard.html` - self-contained HTML that loads Chart.js from
a CDN and embeds the aggregated JSON inline. No other files are
written. The output dir is gitignored.

## Refresh strategy

Each run is a full refresh: the CLI re-lists and re-fetches every
object under `completed/` and `failed/`, plus `schedule.json`. This
keeps the tool simple and means the dashboard always reflects the
bucket exactly. For typical "every few days" usage with a few hundred
small JSON files this finishes in seconds.
