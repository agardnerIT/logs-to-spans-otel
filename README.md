# logs-to-spans — OpenTelemetry Collector Connector

**Convert log records into trace spans** by grouping logs that share a common attribute value (e.g. `userID=123`). Each group becomes a single trace, with every log ordered by timestamp becoming a span in that trace.

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

## Configuration

```yaml
connectors:
  logs_to_spans:
    service_name: my-app        # service.name on produced spans (default: "logs-to-spans")
    timeout: 5s                 # inactivity timeout before flushing a group
    max_wait: 30s               # absolute max time from first log (prevents starvation)
    group_by_keys:              # keys to extract and group by (tried in order)
      - user
      - userID
      - user_id
    end_span_duration: 500ms   # duration of the final span in each trace
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

## Including in your own collector build

Add this connector to your OCB `builder-config.yaml`:

```yaml
connectors:
  - gomod: "github.com/agardnerIT/logs-to-spans-otel v0.2.0"
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
