# syntax=docker/dockerfile:1
# Builds the quickstart collector from *this* working tree, not from the last
# published module version, so the image always demonstrates the code in the
# checkout (see the note on DIGEST pinning below).
#
# Both base images are pinned by digest, with the human-readable tag kept in the
# comment. A tag is mutable: rebuilding the same commit would otherwise produce
# a different image, and a compromised or mistagged upstream tag would flow
# straight into the published quickstart image.
FROM golang:1.26@sha256:978e0a4e3d953662577d784471db27742f543ab09eead06839a838188c73314b AS builder

# GOTOOLCHAIN=local stops the builder from silently downloading a different Go
# toolchain mid-build; -mod=readonly stops it from rewriting go.mod/go.sum.
ENV GOTOOLCHAIN=local \
    GOFLAGS=-mod=readonly \
    CGO_ENABLED=0

WORKDIR /build
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.165.0

# The connector source. manifest.yaml replaces the module with this tree.
COPY go.mod go.sum ./
COPY *.go ./
COPY internal ./internal
COPY examples/quickstart/manifest.yaml ./manifest.yaml
RUN /go/bin/builder --config manifest.yaml

# distroless/static-debian12:nonroot, pinned by digest. The :nonroot tag runs as
# uid 65532, which is why no USER line is needed - keep the digest if you change
# the tag, because the "nonroot" property comes from it.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

WORKDIR /app
COPY --from=builder /build/otelcol-dist/otelcol-logs-to-spans-quickstart /app/collector
COPY examples/quickstart/collector.yaml examples/quickstart/app.log /app/

ENTRYPOINT ["/app/collector"]
CMD ["--config", "/app/collector.yaml"]

# No HEALTHCHECK: distroless has no shell, wget or curl, and the collector
# binary has no self-probe subcommand. Use the health_check extension plus an
# external probe (Kubernetes readinessProbe, or `docker inspect --format
# '{{.State.Health.Status}}'` against a sidecar) if you need one. The collector
# already exits non-zero on a fatal configuration error at startup.
