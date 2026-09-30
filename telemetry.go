package logs_to_spans

import (
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

	telemetryScopeName = "github.com/agardnerIT/logs-to-spans-otel"
)

// telemetry holds the connector's internal instruments. Wire them from
// component.TelemetrySettings.MeterProvider so they land on the collector's
// own metrics pipeline rather than the telemetry data being converted.
type telemetry struct {
	logsIngested     metric.Int64Counter
	tracesCreated    metric.Int64Counter
	unmatchedDropped metric.Int64Counter
}

func newTelemetry(settings component.TelemetrySettings) (*telemetry, error) {
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

	return &telemetry{
		logsIngested:     logsIngested,
		tracesCreated:    tracesCreated,
		unmatchedDropped: unmatchedDropped,
	}, nil
}
