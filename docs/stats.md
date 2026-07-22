# Statistics routes

EyeTroClick exposes channel and global statistics. Published-message series
use `date`; collected-message series use `insert_date`. Dates are returned in
ClickHouse response format and counts are grouped by day, hour, or month.

## `GET /get_stats_chan`

Query parameter:

```text
chan_name=<channel name or numeric channel id>
```

Response keys:

- `stats`: `true` when channel rows were found, otherwise `false`.
- `daily`: 31 daily buckets based on published `date`.
- `hourly`: 24 hourly buckets based on published `date`.
- `monthly`: all retained published-message months, newest first.

Daily and hourly buckets are zero-filled when no messages exist in their recent
windows. Monthly data has no rolling 24-month limit, so older channels remain
visible in the “All posts collected” view.

Example:

```bash
curl 'http://127.0.0.1:6000/get_stats_chan?chan_name=1001385580796'
```

## `GET /get_stats`

Returns aggregate statistics for compatibility with existing consumers. The
payload includes summary counts, collected-message series, published-message
series, top channels, and database field statistics.

## `GET /get_stats/<section>`

Returns one independent section:

- `summary`: total messages and distinct channels.
- `collected`: daily, hourly, and monthly series grouped by `insert_date`.
- `published`: daily, hourly, and monthly series grouped by published `date`.
- `database`: top channels and database field statistics.

Example:

```bash
curl 'http://127.0.0.1:6000/get_stats/published'
```
