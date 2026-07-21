# Release Notes

## Unreleased

- Add a WAL-backed SQLite manifest for committed channel message ranges.
- Add authenticated, bounded full and incremental replication export APIs.
- Preserve legacy bulk insert payloads while adding idempotent batch identities.
- Enforce a rolling 24-calendar-month replication window and targeted channel exports.
- Bound ClickHouse export execution time and add passive SQLite WAL maintenance.
