# Turkiye Real-Estate Crawler (Prototype)

Prototype crawler that can run for any `city/district` in Turkiye, store results in local SQLite, and publish to Cloudflare Pages + D1.

## Included Sources

- `emlakjet` (primary working source for city/district-wide crawling)
- `sahibinden` (currently often challenge/blocked in this prototype mode)
- `hepsiemlak` (browser-rendered Playwright mode; still frequently challenge-blocked)
- `atasehirsatilik` (Ataşehir-specific extra source)
- `turyap_251316` (Ataşehir office page, filtered by address)

## Local Usage

```bash
npm install
```

Run default area:

```bash
npm run crawl
```

Run specific city/district:

```bash
npm run crawl -- --city=Ankara --district=Cankaya --sources=emlakjet
```

Control detail fetch cap for Emlakjet listing pages (feature extraction):

```bash
npm run crawl -- --city=Istanbul --district=Atasehir --sources=emlakjet --emlakjet-detail-max=350
```

Control Hepsiemlak paging and challenge wait:

```bash
npm run crawl -- --city=Istanbul --district=Atasehir --sources=hepsiemlak --hepsiemlak-max-pages=24 --hepsiemlak-detail-max=60 --hepsiemlak-wait-ms=45000
```

Quick multi-source run:

```bash
npm run crawl:quick
```

## Local Storage (SQLite)

By default, each run is persisted to:

- `data/real-estate.sqlite`

Disable SQLite writes:

```bash
npm run crawl -- --no-sqlite
```

Custom DB path:

```bash
npm run crawl -- --sqlite-path=data/real-estate-v2.sqlite
```

Schema:

- `db/schema.sql`
- `db/migrations/0001_init.sql`
- `db/migrations/0002_listing_features.sql`
- `db/migrations/0003_system_logs.sql`
- `db/migrations/0004_listing_legal_usage_signals.sql`
- `db/migrations/0005_deal_feedback.sql`

## Output Files

Each run writes:

- `output/run-<tag>/results.json`
- `output/run-<tag>/summary.json`
- `output/run-<tag>/listings.csv`

Latest snapshots:

- `output/latest-results.json`
- `output/latest-summary.json`
- `output/latest-listings.csv`

## Cloudflare Pages + D1

1. Create D1 database:

```bash
wrangler d1 create real-estate-crawler
```

2. Put the returned `database_id` into `wrangler.toml`.

3. Apply migrations:

```bash
npm run db:remote:migrate
```

4. Configure Pages env vars:

- `INGEST_API_TOKEN` (for `/api/ingest`)
- Optional for UI-triggered workflow dispatch:
  - `CRAWL_REQUEST_TOKEN`
  - `GITHUB_TOKEN`
  - `GITHUB_OWNER`
  - `GITHUB_REPO`
  - `GITHUB_WORKFLOW_FILE` (default `crawl.yml`)
  - `GITHUB_REF` (default `main`)

## Simple UI

Open `/` after deployment (or `wrangler pages dev public`).

UI features:

- select any city in Turkiye
- select district in that city
- start scan with one button
- auto-refresh results from D1
- view top undervalued listings
- optional technical log panel for troubleshooting

City/district options come from `public/tr-locations.json` (81 cities / 973 districts).

## Pages Functions API

- `GET /api/health`
- `GET /api/runs?city=Istanbul&district=Atasehir&limit=20`
- `GET /api/listings?city=Istanbul&district=Atasehir&active=1&limit=50`
- `GET /api/deals?city=Istanbul&district=Atasehir&limit=25&min_discount=0.12&min_confidence=0.35&min_comps=6`
- `GET /api/logs?city=Istanbul&district=Atasehir&limit=50`
- `POST /api/ingest`
- `POST /api/crawl-request`

## Cloudflare Logs (Worker Runtime)

Tail production function logs from terminal:

```bash
HOME=$PWD npx wrangler pages deployment tail --project-name real-estate-crawler --environment production --format pretty
```

Tail with JSON output:

```bash
HOME=$PWD npx wrangler pages deployment tail --project-name real-estate-crawler --environment production --format json
```

## GitHub Actions Workflow (`.github/workflows/crawl.yml`)

This project includes:

- `workflow_dispatch` for manual/API trigger (used by UI)
- `schedule` with `cron: "30 */6 * * *"`

That schedule is effectively a cron job in GitHub Actions (runs every 6 hours, in UTC).

Workflow secrets:

- `CF_INGEST_URL` (optional; if missing, push step is skipped)
- `CF_INGEST_TOKEN` (optional; needed only when ingest endpoint requires auth)

Token note for `/api/crawl-request`:

- `GITHUB_TOKEN` on Pages side should be a PAT/GitHub App token that can dispatch workflows in the target repository.

## Push Existing Local Result to Cloudflare

```bash
CF_INGEST_URL="https://<your-pages-domain>/api/ingest" \
CF_INGEST_TOKEN="<INGEST_API_TOKEN>" \
npm run push:cf
```

Optional payload path:

```bash
node src/push-to-cloudflare.js output/run-<tag>/results.json
```
