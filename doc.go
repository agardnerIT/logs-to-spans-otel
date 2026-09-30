// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package logs_to_spans converts log records into trace spans. It groups log
// records that share a common extracted value (an attribute or a key in the
// body) and emits each group as one trace, with every log record becoming a
// span in that trace.
//
// The package is a connector: it consumes logs and produces traces. See the
// repository README for the configuration reference and behaviour.
//
// At donation to opentelemetry-collector-contrib this package moves to
// connector/logstospansconnector and the import path below changes to
// github.com/open-telemetry/opentelemetry-collector-contrib/connector/logstospansconnector.
package logs_to_spans // import "github.com/agardnerIT/logs-to-spans-otel"
