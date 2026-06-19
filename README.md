# logs-to-spans — OpenTelemetry Collector Connector

<img width="459" height="238" alt="logs-to-spans" src="https://github.com/user-attachments/assets/674194ca-7573-43b7-91dd-b70a5a6bb419" />

**Convert log records into trace spans** by grouping logs that share a common attribute value (e.g. `userID=123`). Each group becomes a single trace, with every log ordered by timestamp becoming a span in that trace.

> **This connector is not included in any official OpenTelemetry Collector distribution.** You must build your own custom Collector using the [OpenTelemetry Collector Builder (OCB)](https://opentelemetry.io/docs/collector/extend/ocb/). The builder binary is available as a downloadable asset from [OpenTelemetry Collector releases](https://github.com/open-telemetry/opentelemetry-collector-releases/tags) (look for `cmd/builder` tags). Docker images can be built using the [Dockerfile](#containerize-your-collector-distribution) example below. A ready-to-use [`builder-config.yaml`](builder-config.yaml) is included in this repo.

## Why?

Standard log aggregators treat each line as an independent event — you lose the _relationship_ between log lines that belong to the same user session, request, or workflow.

This connector reconstructs that relationship:
- Logs with `userID=abc`, `user=abc`, or `user_id=abc` are grouped into one trace
- Each log becomes a span with a start time equal to the log timestamp
- Span duration = time delta to the next log line (last span uses a configurable default)
- Spans form a parent-child chain preserving execution order
- Result: a single trace you can visualise in Jaeger, Zipkin, or any OTLP-compatible backend

Use it when you have _unstructured_ or _semi-structured_ logs that you want to explore as traces without modifying your application.

## How it works

```
                    ┌──────────────────────┐
                    │     filelog / otlp    │
                    └──────────┬───────────┘
                               │ log records
                               ▼
                    ┌──────────────────────┐
                    │   logs_to_spans      │
                    │   connector          │
                    │                      │
                    │  1. Extract key from  │
                    │     each log body     │
                    │  2. Group by value    │
                    │     (e.g. "123")      │
                    │  3. Flush after       │
                    │     timeout           │
                    │  4. Convert group →   │
                    │     trace with spans  │
                    └──────────┬───────────┘
                               │ trace spans
                               ▼
                    ┌──────────────────────┐
                    │   otlp / debug       │
                    │   exporter           │
                    └──────────────────────┘
```

### Key extraction

The connector tries two strategies in order:

1. **Structured (Map) body** — if the log body is a JSON object, it looks for top-level keys matching `group_by_keys`
2. **Unstructured (string) body** — falls back to regex `key=(\S+)` to extract values

Logs that don't match any key are silently dropped (or you can split them into a separate pipeline — see [Filtering unmatched logs](#filtering-unmatched-logs)).

### Duration extraction

The connector reads explicit span durations from log attributes by checking each key in `duration_keys` in order. Supported values: Go duration strings (`"5s"`, `"2m30s"`), integers (seconds), or floats (seconds).

Logs without a valid duration attribute fall back to the default behaviour: each span's duration is the time delta to the next log, and the last span in a trace uses `end_span_duration`.

## Configuration

### Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `service_name` | string | `"logs-to-spans"` | Value of the `service.name` resource attribute on produced spans. |
| `timeout` | duration | `5s` | **Inactivity timeout.** Resets every time a new log arrives for a group. When no new logs arrive for this long, the group is flushed and converted to a trace. |
| `max_wait` | duration | `30s` | **Absolute max wait.** Maximum time from the *first* log in a group before it is force-flushed — regardless of ongoing activity. Prevents groups with continuous log streams from never being emitted. |
| `max_logs_per_trace` | int | `100` | **Max logs per trace.** Maximum number of log records in a single group/trace. When the limit is reached, the current group is flushed early and a new group starts. Set to `0` for no limit. Traces are connected via [span links](https://opentelemetry.io/docs/concepts/signals/traces/#span-links). |
| `group_by_keys` | string list | `[]` | Keys to extract from each log body and group by (tried in order). See [Key extraction](#key-extraction). |
| `duration_keys` | string list | `[]` | Log attribute names to read an explicit span duration from (tried in order). Accepts Go duration strings, integers (seconds), or floats (seconds). When set, overrides the auto-calculated duration for that span. |
| `end_span_duration` | duration | `500ms` | Duration assigned to the **last** span in each trace when no explicit duration is available. |
| `unmatched_behaviour` | string | `"drop"` | What to do with logs that don't match any `group_by_keys`: `"drop"` (silently discard) or `"pass_through"` (forward unchanged to a separate pipeline). |

> **`timeout` vs `max_wait`:** `timeout` is a *sliding* inactivity window — it resets every time a new log arrives. `max_wait` is a *fixed* deadline from the moment the group is created. A group is flushed when *either* timer fires first.

### Example

```yaml
connectors:
  logs_to_spans:
    service_name: my-app
    timeout: 5s
    max_wait: 30s
    max_logs_per_trace: 100
    group_by_keys:
      - user
      - userID
      - user_id
    duration_keys:
      - duration
      - time
      - time-spent
    end_span_duration: 500ms
    unmatched_behaviour: drop
```

### Pipeline wiring

```yaml
service:
  pipelines:
    logs:
      receivers: [filelog]
      exporters: [logs_to_spans]
    traces:
      receivers: [logs_to_spans]
      exporters: [otlp]
```

### Filtering unmatched logs

If you want unmatched logs to go to a separate pipeline instead of being dropped, use the `filterprocessor` with `include`/`exclude`:

```yaml
processors:
  filter/include_matched:
    logs:
      include:
        match_type: regexp
        bodies: [".*(user|userID|user_id)=\\S+.*"]
  filter/exclude_matched:
    logs:
      exclude:
        match_type: regexp
        bodies: [".*(user|userID|user_id)=\\S+.*"]

service:
  pipelines:
    logs/matched:
      receivers: [filelog]
      processors: [filter/include_matched]
      exporters: [logs_to_spans]
    logs/unmatched:
      receivers: [filelog]
      processors: [filter/exclude_matched]
      exporters: [debug]
    traces:
      receivers: [logs_to_spans]
      exporters: [otlp]
```

## Grouping non k=v logs

The connector extracts group keys using a `key=value` pattern by default. If your logs use a different format (e.g. space-separated, colon-delimited), you have two options:

### Option 1: Transform processor (recommended)

Use the `transform` processor to rewrite log bodies into `key=value` format before they reach the connector:

```yaml
processors:
  transform/normalize_logs:
    log_statements:
      - context: log
        statements:
          # "user 123" → "user=123"
          - replace_pattern(body, `\buser\s+(\S+)`, `user=${1}`)
          # "user: 123" → "user=123"
          - replace_pattern(body, `\buser:\s+(\S+)`, `user=${1}`)
```

Pipeline wiring:

```yaml
service:
  pipelines:
    logs:
      receivers: [filelog]
      processors: [transform/normalize_logs]
      exporters: [logs_to_spans]
    traces:
      receivers: [logs_to_spans]
      exporters: [otlp]
```

This keeps the connector simple — the transform processor handles all format normalization upstream.

### Option 2: Filelog receiver operators

If you control the filelog receiver config, you can use `regex_parser` to extract structured fields. However, the connector only reads from the log body (not attributes), so the extracted value needs to end up in the body for `group_by_keys` to find it.

This is more involved than the transform processor and loses the original log message in the body, so **the transform processor is generally the better choice**.

## Including in your own collector build

Add this connector to your OCB `builder-config.yaml`:

```yaml
connectors:
  - gomod: "github.com/agardnerIT/logs-to-spans-otel v0.4.0"
    name: "logs_to_spans"

exporters:
  - gomod: "go.opentelemetry.io/collector/exporter/debugexporter v0.154.0"
  - gomod: "go.opentelemetry.io/collector/exporter/otlpexporter v0.154.0"

receivers:
  - gomod: "go.opentelemetry.io/collector/receiver/otlpreceiver v0.154.0"
  - gomod: "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver v0.154.0"

processors:
  - gomod: "go.opentelemetry.io/collector/processor/batchprocessor v0.154.0"
  - gomod: "github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor v0.154.0"

providers:
  - gomod: "go.opentelemetry.io/collector/confmap/provider/envprovider v1.48.0"
  - gomod: "go.opentelemetry.io/collector/confmap/provider/fileprovider v1.48.0"
  - gomod: "go.opentelemetry.io/collector/confmap/provider/yamlprovider v1.48.0"
```

Build:

```sh
ocb --config builder-config.yaml
```

> **Local development:** if you're testing changes before publishing, add `path: "."` alongside the `gomod` entry to point OCB at your local checkout.

## Development

### Prerequisites

- Go 1.25+
- OpenTelemetry Collector Builder (`ocb`) — available at `~/tools/ocb` or [install from releases](https://github.com/open-telemetry/opentelemetry-collector-releases)

### Tests

```sh
go test -v -race -count=1 ./...
```

The test suite covers:
- Key extraction from structured (Map) and unstructured (string) bodies
- Grouping logs into a single trace
- Multiple groups producing separate traces
- Timestamp ordering (regardless of insertion order)
- Configurable end-span duration
- Flush-on-shutdown
- Service name propagation
- `max_logs_per_trace` limit, span links, and chain behaviour

### Quick start with filelog

```sh
# Build the collector
ocb --config builder-config.yaml

# Run
./otelcol-dist/otelcol-logs-to-spans --config collector.yaml
```

The included `collector.yaml` and `input.log` let you exercise the full pipeline immediately. Spans appear in the debug exporter output and, if Jaeger is running on `127.0.0.1:4317`, in Jaeger UI at `localhost:16686`.

## Project structure

```
.
├── LICENSE              # Apache 2.0
├── README.md
├── builder-config.yaml  # OCB manifest
├── collector.yaml       # Example collector config
├── input.log            # Sample log file for testing
├── go.mod / go.sum
├── config.go            # Config struct and defaults
├── factory.go           # OTEL connector factory
├── connector.go         # Core implementation
└── connector_test.go    # Reusable test harness
```

## Changelog

### v0.4.0

- `max_logs_per_trace` config option to cap group size — prevents unbounded trace growth from high-volume sessions (default: 100, set to `0` for no limit)
- When the limit is reached, the current group is flushed early and a new group starts with fresh timers
- Span links connect consecutive traces from the same group, preserving the relationship across splits
- 8 new tests covering limit flush, exact boundary, below limit, zero=no-limit, span links, chain, separate groups, and timeout interaction

### v0.3.0

- Configurable span duration via `duration_keys` — read explicit duration from log attributes (string, int, or double), falling back to `Timestamp` then `ObservedTimestamp`
- Compile group-by regex once at startup instead of per log for better performance
- 20+ new tests covering duration parsing, timestamp priority, regex compilation, config validation, and edge cases

### v0.2.0

- Initial public release
- Core connector: `logs_to_spans` (Logs → Traces)
- Key extraction from structured (Map) and unstructured (string) bodies
- Per-group inactivity timer and max-wait timer (prevents starvation)
- Configurable group-by keys, timeout, end-span duration, service name
- Parent-child span chains preserving log order
- Flush-on-shutdown
- 15 tests covering grouping, sorting, duration, shutdown, key extraction, and service name
- OCB builder config for custom collector distributions

## License

Apache 2.0 — see [LICENSE](LICENSE).
