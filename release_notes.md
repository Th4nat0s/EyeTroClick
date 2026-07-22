# Release Notes

## Unreleased

- Add targeted ClickHouse metadata-boundary repair for eYeTr0du1t daily maintenance.

- Include all retained published message history in channel monthly statistics.
- Keep message IDs and timestamps available to Eyetroduit channel message views.
- Add a WAL-backed SQLite manifest for committed channel message ranges.
- Add authenticated, bounded full and incremental replication export APIs.
- Preserve legacy bulk insert payloads while adding idempotent batch identities.
- Enforce a rolling 24-calendar-month replication window and targeted channel exports.
- Bound ClickHouse export execution time and add passive SQLite WAL maintenance.
