# logs-to-spans quickstart

Builds a small collector that reads `app.log` and turns its log lines into
traces with the `logs_to_spans` connector. The debug exporter prints the spans
to the console.

`app.log` contains plain-text and JSON lines grouped by `user_id` and
`session_id`. The final line has no group key and is dropped.

## Run in Docker

The image builds the collector with OCB, so no local Go toolchain is needed:

```sh
docker build -t logs-to-spans-quickstart .
docker run --rm logs-to-spans-quickstart
```

Three traces appear after the 2s timeout. Press Ctrl+C to stop the collector.

## Run without Docker

Build the collector from `manifest.yaml`, then start it:

```sh
ocb --config manifest.yaml
./otelcol-dist/otelcol-logs-to-spans-quickstart --config collector.yaml
```

The collector builds with OCB v0.162.0 and the published `v0.5.0` connector.
To use local connector changes, add a `replaces` entry pointing at this
repository, as shown in the root [`builder-config.yaml`](../../builder-config.yaml).
