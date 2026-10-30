FROM golang:1.26 AS builder

WORKDIR /build
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.162.0

COPY manifest.yaml .
RUN /go/bin/builder --config manifest.yaml

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=builder /build/otelcol-dist/otelcol-logs-to-spans-quickstart /app/collector
COPY collector.yaml app.log /app/

ENTRYPOINT ["/app/collector"]
CMD ["--config", "/app/collector.yaml"]
