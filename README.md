# Stats — EarthScope/ringserver

This is the `stats` branch of [EarthScope/ringserver](../../tree/main).
It is an **orphan branch** (no shared history with `main`) that archives
GitHub traffic data.  The GitHub API only retains traffic history for
**14 days**; this branch is the long-term record.

Files here are written automatically by a scheduled workflow and
**must not be edited by hand**.

---

## Links

- [Source code (main branch)](../../tree/main)
- [Workflow that writes this data](../../blob/main/.github/workflows/github-traffic-snapshot.yml)
- [Setup guide — how to replicate on another repo](../../blob/main/doc/github-stats-setup.md)

---

## What's in this branch

All files are [JSONL](https://jsonlines.org/) — one JSON object per line.

### `clones.jsonl`

One record per UTC day.  Updated **idempotently** (re-running the same day
overwrites that day's record, so a missed run recovers automatically on the
next successful run via the 14-day API backfill).

```json
{"date":"2026-05-18","count":12,"uniques":7}
```

### `views.jsonl`

Same structure as `clones.jsonl`.

```json
{"date":"2026-05-18","count":84,"uniques":31}
```

### `popular-paths.jsonl`

One record per snapshot run (append-only).  Top 10 paths as reported by the
API on that day.

```json
{"snapshot_date":"2026-05-18","paths":[{"path":"/EarthScope/ringserver","title":"EarthScope/ringserver","count":42,"uniques":18}]}
```

### `referrers.jsonl`

One record per snapshot run (append-only).  Top 10 referrers.

```json
{"snapshot_date":"2026-05-18","referrers":[{"referrer":"github.com","count":30,"uniques":12}]}
```

### `releases.jsonl`

One record per snapshot run (append-only).  Cumulative `download_count` for
every release asset at the time of the snapshot.

```json
{"snapshot_date":"2026-05-18","releases":[{"tag":"v4.0.1","assets":[{"name":"ringserver-4.0.1-linux-x86_64.tar.gz","download_count":143,"updated_at":"2026-04-01T12:00:00Z"}]}]}
```

---

## How updates happen

A GitHub Actions workflow on `main` runs **daily at ~06:17 UTC** and can be
triggered manually via `workflow_dispatch`.  It checks out this branch into a
subdirectory, runs the Python snapshot script, then commits and pushes any
changes here.

`clones.jsonl` and `views.jsonl` are updated idempotently by date.  The other
three files are append-only.

---

> **Do not edit files in this branch by hand.**  The workflow rewrites
> same-day rows in `clones.jsonl` and `views.jsonl`; manual edits to those
> rows will be silently overwritten on the next run.
