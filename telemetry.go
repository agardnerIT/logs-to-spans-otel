package logs_to_spans

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Instrument names as exposed on the collector's internal telemetry endpoint.
// They are user-visible output: the README "Produced metrics" table documents
// exactly these, and #17 turns them into mdatagen-generated instruments.
const (
	metricLogsIngested     = "otelcol_connector_logs_to_spans_logs_ingested"
	metricTracesCreated    = "otelcol_connector_logs_to_spans_traces_created"
	metricUnmatchedDropped = "otelcol_connector_logs_to_spans_unmatched_dropped"
	metricGroupsEvicted    = "otelcol_connector_logs_to_spans_groups_evicted"
	metricActiveGroups     = "otelcol_connector_logs_to_spans_active_groups"

	telemetryScopeName = "github.com/agardnerIT/logs-to-spans-otel"
)

// telemetry holds the connector's internal instruments. Wire them from
// component.TelemetrySettings.MeterProvider so they land on the collector's
// own metrics pipeline rather than the telemetry data being converted.
type telemetry struct {
	logsIngested     metric.Int64Counter
	tracesCreated    metric.Int64Counter
	unmatchedDropped metric.Int64Counter
	groupsEvicted    metric.Int64Counter
}

// newTelemetry builds the instruments. activeGroups is polled by the
// active_groups gauge on each collection, so the connector does not have to
// maintain a separate running count.
func newTelemetry(settings component.TelemetrySettings, activeGroups func() int64) (*telemetry, error) {
	// Tests and embedders may construct the connector without a MeterProvider;
	// the collector always sets one. Fall back to a no-op meter instead of
	// dereferencing a nil interface.
	meterProvider := settings.MeterProvider
	if meterProvider == nil {
		meterProvider = noop.NewMeterProvider()
	}
	meter := meterProvider.Meter(telemetryScopeName)

	logsIngested, err := meter.Int64Counter(
		metricLogsIngested,
		metric.WithDescription("Number of log records consumed by the connector."),
		metric.WithUnit("{log_record}"),
	)
	if err != nil {
		return nil, err
	}

	tracesCreated, err := meter.Int64Counter(
		metricTracesCreated,
		metric.WithDescription("Number of traces emitted by the connector."),
		metric.WithUnit("{trace}"),
	)
	if err != nil {
		return nil, err
	}

	unmatchedDropped, err := meter.Int64Counter(
		metricUnmatchedDropped,
		metric.WithDescription("Number of log records dropped because no group_by_keys or group_by_attributes entry matched."),
		metric.WithUnit("{log_record}"),
	)
	if err != nil {
		return nil, err
	}

	groupsEvicted, err := meter.Int64Counter(
		metricGroupsEvicted,
		metric.WithDescription("Number of groups flushed early because max_groups was reached."),
		metric.WithUnit("{group}"),
	)
	if err != nil {
		return nil, err
	}

	_, err = meter.Int64ObservableGauge(
		metricActiveGroups,
		metric.WithDescription("Number of log groups currently buffered by the connector."),
		metric.WithUnit("{group}"),
		metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
			observer.Observe(activeGroups())
			return nil
		}),
	)
	if err != nil {
		return nil, err
	}

	return &telemetry{
		logsIngested:     logsIngested,
		tracesCreated:    tracesCreated,
		unmatchedDropped: unmatchedDropped,
		groupsEvicted:    groupsEvicted,
	}, nil
}
