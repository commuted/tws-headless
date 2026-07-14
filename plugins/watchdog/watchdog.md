# Watchdog Plugin

Operational anomaly detection and alert egress. The engine's built-in
instrumentation is forensic — it records what happened. The watchdog notices
when an expected thing **fails to happen**, and carries every alert beyond
the log file to somewhere a human will see it.

Loaded and **started automatically** by `run_engine` — a watchdog that must
be switched on manually is a watchdog that's off. Its checks no-op safely
until IB connects.

## Checks

A monitor thread wakes every `check_interval_seconds` (default 60) and runs:

| Check | Detects | Default threshold |
|-------|---------|-------------------|
| **Bar-feed staleness** | A keepUpToDate live-bar subscription that has stopped delivering (e.g. lost across a TWS reconnect) while strategies may be holding positions with no exit signals | 600 s without a bar, during RTH |
| **Stuck orders** | Any order with no fill or terminal status (covers the overnight MOC window on wall clock) | 1800 s |
| **Reconciliation drift** | Position/cash divergence between plugin ledgers and the real account, without waiting for the next engine restart | every 3600 s |

Each condition alerts **once** until it recovers (staleness/stuck dedupe),
so a persistent fault doesn't flood the sink.

RTH is America/New_York, weekdays 09:30–16:00. Market holidays are not
modeled — a staleness alert on a holiday is a tolerable false positive.
Set `rth_only: false` to check around the clock.

## Alert egress

The watchdog subscribes to the MessageBus `alerts` channel and sinks **all**
alerts — its own, the executive's (`ib_error`, `reconnected`), and any
plugin's (e.g. gld_usd_swap's `stuck_order`, `order_terminal`,
`bar_parse_failure`):

- **`{plugin_dir}/alerts.jsonl`** — one JSON object per line, account-scoped
  (paper and live alerts never comingle). Point a tailer, cron job, or log
  shipper at this file.
- **Webhook** (optional) — set `webhook_url` and each alert is POSTed as
  JSON (Slack/Discord/ntfy/etc. behind a small adapter, or any incident
  endpoint). Delivery is drained by the monitor thread (lag up to
  `check_interval_seconds`); failures are logged and dropped, never retried
  into a pile-up. The file sink always runs regardless.

### Alert format

```json
{
  "received_at": "2026-07-13T21:14:02+00:00",
  "channel": "alerts",
  "publisher": "watchdog",
  "message_type": "alert",
  "payload": {
    "kind": "stale_feed",
    "message": "Live-bar feed for GLD (req_id=143) has delivered no bars for 12 min...",
    "plugin": "watchdog",
    "timestamp": "2026-07-13T21:14:02+00:00",
    "symbol": "GLD",
    "req_id": 143,
    "seconds_since_last_bar": 720
  }
}
```

Known `kind` values: `stale_feed`, `stuck_order`, `reconciliation_drift`
(watchdog); `ib_error`, `reconnected` (executive); `stuck_order`,
`order_terminal`, `ib_error`, `bar_parse_failure` (gld_usd_swap).

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `check_interval_seconds` | 60 | Monitor wake interval (min 5) |
| `bar_staleness_seconds` | 600 | Silence threshold per live-bar feed (min 60) |
| `order_stuck_seconds` | 1800 | Unresolved-order threshold (min 60) |
| `reconcile_interval_seconds` | 3600 | Holdings/account drift check interval (min 300) |
| `rth_only` | true | Only check feed staleness during NY regular trading hours |
| `webhook_url` | "" | POST each alert as JSON; empty = file sink only |

## Commands

```bash
# Runtime status: monitor running, counters, currently-stale feeds
./ibctl.py plugin request watchdog get_status

# Run all checks immediately
./ibctl.py plugin request watchdog check_now

# Tail recent alerts
./ibctl.py plugin request watchdog get_alerts '{"count": 20}'

# Configure
./ibctl.py plugin request watchdog set_parameter '{"key": "webhook_url", "value": "https://example.com/hook"}'
./ibctl.py plugin request watchdog set_parameter '{"key": "bar_staleness_seconds", "value": 900}'
./ibctl.py plugin request watchdog set_parameter '{"key": "rth_only", "value": false}'
```

Parameters persist across restarts (saved to `state.json` on every change
and on stop/freeze).

## What this does NOT cover

- **Engine death** — a dead process can't self-report. Pair the file sink
  with an external liveness check (cron that alerts if `alerts.jsonl`'s
  parent log stream goes quiet, systemd watchdog, or a heartbeat ping).
- **Market holidays** — staleness checks assume every NY weekday is a
  trading day.
- **Strategy-level wrongness** — it detects operational anomalies (dead
  feeds, stuck orders, ledger drift), not bad trading decisions.
