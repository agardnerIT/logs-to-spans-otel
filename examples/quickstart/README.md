# logs-to-spans quickstart

Builds a small collector that reads `app.log`, turns its log lines into traces
with the `logs_to_spans` connector, prints them with the debug exporter and
ships them to Jaeger with the `otlp_http` exporter. The three resulting traces
show up in the Jaeger UI at <http://localhost:16686> under the service
`logs-to-spans`.

`app.log` contains plain-text and JSON lines grouped by `user_id` and
`session_id`. The final line has no group key and is dropped. Its dates are
fixed, so the collector rebases them onto now — see [Timestamps](#timestamps).

## 1. Start Jaeger

```sh
docker run --rm --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 5778:5778 \
  -p 9411:9411 \
  cr.jaegertracing.io/jaegertracing/jaeger:2.22.0
```

The collector sends spans to Jaeger's OTLP/HTTP endpoint on `4318`. The UI is
on `16686`.

## 2. Run the collector

### In Docker

The image builds the collector with OCB, so no local Go toolchain is needed.
From this directory:

```sh
docker build -t logs-to-spans-quickstart .
docker run --rm \
  --add-host=host.docker.internal:host-gateway \
  -e OTLP_HTTP_ENDPOINT=http://host.docker.internal:4318 \
  logs-to-spans-quickstart
```

The collector container reaches the Jaeger container through the host, hence
the endpoint override (see below). `--add-host` makes `host.docker.internal`
resolve on Linux; Docker Desktop provides it already.

`collector.yaml`, `manifest.yaml` and `app.log` are copied into the image at
build time, so `docker build` again after editing any of them — a running
container keeps the config it was built with. If traces do not appear, check
what the container actually loaded before suspecting Jaeger:

```sh
# the spans the collector's debug exporter emitted, with their timestamps
docker logs <container>
```

### Without Docker

Build the collector from `manifest.yaml`, then start it:

```sh
ocb --config manifest.yaml
./otelcol-dist/otelcol-logs-to-spans-quickstart --config collector.yaml
```

The collector builds with OCB v0.162.0 and the published `v0.6.0` connector.
To use local connector changes, add a `replaces` entry pointing at this
repository, as shown in the root [`builder-config.yaml`](../../builder-config.yaml).

Either way, three traces appear in the debug output after the 2s timeout and in
Jaeger. Press Ctrl+C to stop the collector. If Jaeger is not running the debug
exporter still prints the spans; the `otlp_http` exporter logs the connection
failure and retries.

## Timestamps

`app.log` has fixed dates. Used as-is they put the spans years in the past,
outside the **Last 1 hour** window Jaeger opens with, and any date baked into a
sample file drifts further out of range as it ages. Editing the dates before
each demo fixes the symptom but nothing keeps it fixed.

`collector.yaml` therefore rebases the file's timeline onto now in a `transform`
processor:

```yaml
processors:
  transform/rebase:
    log_statements:
      - context: log
        statements:
          - set(log.time, Now() - Duration("10s") - (Time("2026-01-15T10:00:00Z", "%Y-%m-%dT%H:%M:%SZ") - log.time)) where log.time_unix_nano != 0
```

`Time(...) - log.time` is how far into the file a line sits, and subtracting that
from `Now()` shifts every record by the same amount. The relative spacing — the
waterfall in Jaeger — survives, while the whole trace lands seconds before now.
`Now()` is wall-clock time at processing, so every run produces a fresh trace.
Plain `set(log.time, Now())` would also keep the spans in range, but it stamps
every record with the same instant and collapses each trace to
`end_span_duration`.

Two constants matter if you replace `app.log`:

- the `Time("2026-01-15T10:00:00Z", ...)` anchor must be the file's first
  timestamp, or earlier. It only turns "how far into the file" into a
  duration, so it never needs updating while the file keeps its dates.
- `Duration("10s")` must exceed the total span of the file (7s here), otherwise
  the later spans start in the future and Jaeger's lookback search will not
  return them.

Records with no timestamp are left alone (`log.time_unix_nano != 0` guards the
statement) and fall back to `ObservedTimestamp`, the moment the receiver read the
line, so a file mixing dated and undated lines still works.

The `move` operator in `collector.yaml` is unrelated to the dates: it promotes
the matched remainder to the record body so span names read
`user_id=123 opened cart` rather than
`2026-01-15T10:00:00Z INFO user_id=123 opened cart`.

## How the OTLP/HTTP exporter is wired

`collector.yaml` adds the `otlp_http` exporter (module `otlphttpexporter`), which
speaks OTLP over HTTP/protobuf to `/v1/traces`. In collector v0.162 the
component type is `otlp_http`; the older name `otlphttp` still resolves:

```yaml
exporters:
  otlp_http:
    endpoint: ${env:OTLP_HTTP_ENDPOINT:-http://127.0.0.1:4318}
    tls:
      insecure: true
```

The endpoint defaults to Jaeger on the same host and can be overridden with
`OTLP_HTTP_ENDPOINT`, which is how the Docker run above reaches the host. Both
exporters sit in the traces pipeline, so the spans are printed locally and
exported:

```yaml
service:
  pipelines:
    traces:
      receivers: [logs_to_spans]
      exporters: [debug, otlp_http]
```

`tls.insecure: true` is right for a plain `http://` endpoint like this one. For
an `https://` backend drop it and add any auth under `headers:`. To send to a
collector or vendor instead of Jaeger, point `OTLP_HTTP_ENDPOINT` (or the
default) at its OTLP/HTTP endpoint — no other change is needed.
