// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs_to_spans

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/agardnerIT/logs-to-spans-otel/internal/metadata"
)

// Instrument names as exposed on the collector's internal telemetry endpoint.
// They mirror the telemetry.metrics keys in metadata.yaml (mdatagen prefixes
// each generated name with otelcol_); the README "Produced metrics" table and
// the tests document exactly these.
const (
	metricLogsIngested     = "otelcol_connector_logs_to_spans_logs_ingested"
	metricTracesCreated    = "otelcol_connector_logs_to_spans_traces_created"
	metricUnmatchedDropped = "otelcol_connector_logs_to_spans_unmatched_dropped"
	metricGroupsEvicted    = "otelcol_connector_logs_to_spans_groups_evicted"
	metricActiveGroups     = "otelcol_connector_logs_to_spans_active_groups"
)

// newTelemetry builds the connector's internal instruments from the
// mdatagen-generated TelemetryBuilder and registers the active_groups
// observable gauge. activeGroups is polled on each collection, so the
// connector does not have to maintain a separate running count.
func newTelemetry(settings component.TelemetrySettings, activeGroups func() int64) (*metadata.TelemetryBuilder, error) {
	// Tests and embedders may construct the connector without a MeterProvider;
	// the collector always sets one. Fall back to a no-op meter instead of
	// dereferencing a nil interface.
	if settings.MeterProvider == nil {
		settings.MeterProvider = noop.NewMeterProvider()
	}

	telemetry, err := metadata.NewTelemetryBuilder(settings)
	if err != nil {
		return nil, err
	}

	err = telemetry.RegisterConnectorLogsToSpansActiveGroupsCallback(func(_ context.Context, observer metric.Int64Observer) error {
		observer.Observe(activeGroups())
		return nil
	})
	if err != nil {
		return nil, err
	}

	return telemetry, nil
}
