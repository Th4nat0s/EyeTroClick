# Agent Guidelines

## Project

`EyeTroClick` is the authoritative ClickHouse backend for Telegram messages
collected by the EyeTro ecosystem. It exposes Flask APIs for insertion, search,
analysis, translation, and bounded replication.

Companion repositories:

- `eyetroduit`: control plane, metadata database, and proxy API.
- `ChannelDumper`: distributed Telegram collectors and bulk message importers.
- `DarkTroSync`: local ClickHouse replication client.

Message bodies belong in EyeTroClick, not in the Eyetroduit metadata database.
Canonical Telegram channel IDs stored in ClickHouse are positive integers.

## Code Map

- `db_svr.py`: legacy Flask application and public API routes.
- `replication_manifest.py`: durable SQLite commit-range manifest.
- `replication_api.py`: authenticated full and incremental replication exports.
- `sync_last_ids.py`: synchronization of channel message watermarks.
- `gn_config.yaml.sample`: runtime configuration reference.
- `tests/`: unit and integration tests using fake ClickHouse clients.

## Replication Invariants

- `/insert_records` must remain compatible with the legacy string-encoded JSON
  payload and regular JSON objects.
- Commit master messages to ClickHouse before recording their ranges in SQLite.
- Return HTTP 200 only after both ClickHouse and SQLite commits succeed.
- Import retries must reuse the same `batch_id`; manifest writes must remain
  idempotent across retries and lost responses.
- Keep the manifest compact: one range per channel and accepted batch, never
  one SQLite row per message.
- SQLite must use WAL, `synchronous=FULL`, bounded waits, and short transactions.
- Replication exports must enforce the rolling 24-calendar-month cutoff using
  the Telegram emission date.
- Use bounded keyset pagination and configured ClickHouse execution timeouts.
  Do not introduce `OFFSET` or `FINAL` into replication queries.
- Preserve physical source versions. Destination systems are responsible for
  idempotent deduplication.
- Never derive replication scope from Eyetroduit collection flags or tags.

## Engineering Rules

- Keep changes scoped and preserve existing API compatibility. More than ten
  ChannelDumper instances may insert concurrently.
- All new or modified functions need concise docstrings when their purpose or
  side effects are not self-evident.
- New Python code must be Black-formatted and compatible with Python 3.8.
- Pylint must pass for touched Python code. New code must score `10.00/10` and
  must not add warnings to legacy modules.
- Every behavior change requires focused tests, including failure and retry
  paths when persistence ordering is involved.
- Use parameterized ClickHouse and SQLite queries for external values.
- Do not add secrets to the repository. Configure the replication bearer token
  through `REPLICATION_API_KEY` in production.
- Do not commit runtime databases, WAL files, bytecode, or local configuration.

## Verification

Run from the repository root:

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m unittest discover -s tests -v
black --check -t py38 replication_manifest.py replication_api.py tests
PYLINTHOME=/tmp/pylint-eye .venv/bin/pylint \
  replication_manifest.py replication_api.py tests
git diff --check
```
