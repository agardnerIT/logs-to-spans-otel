package logs_to_spans

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
)

// newMetricsTestSettings returns settings backed by a manual reader so tests
// can assert on the recorded counter values.
func newMetricsTestSettings(t *testing.T) (connector.Settings, *metric.ManualReader) {
	t.Helper()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	return connector.Settings{
		ID: component.MustNewID(TypeStr),
		TelemetrySettings: component.TelemetrySettings{
			Logger:        zap.NewNop(),
			MeterProvider: mp,
		},
	}, reader
}

func createMetricsTestConnector(
	t *testing.T,
	cfg *Config,
	sink *consumertest.TracesSink,
) (connector.Logs, *metric.ManualReader) {
	t.Helper()
	settings, reader := newMetricsTestSettings(t)
	conn, err := NewFactory().CreateLogsToTraces(context.Background(), settings, cfg, sink)
	require.NoError(t, err)
	return conn, reader
}

// collectInt64Counters reads every recorded int64 sum, keyed by metric name.
func collectInt64Counters(t *testing.T, reader *metric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	counters := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				var total int64
				for _, dp := range sum.DataPoints {
					total += dp.Value
				}
				counters[m.Name] = total
			}
		}
	}
	return counters
}

func metricsTestConfig() *Config {
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.Timeout = 50 * time.Millisecond
	cfg.MaxWait = time.Second
	return cfg
}

func TestMetricsLogsIngestedAndTracesCreated(t *testing.T) {
	sink := newTestSink()
	conn, reader := createMetricsTestConnector(t, metricsTestConfig(), sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	sendLogs(t, conn, []plog.LogRecord{
		newLogRecord("user=123 first", now, "INFO"),
		newLogRecord("user=123 second", now.Add(time.Second), "INFO"),
		newLogRecord("user=123 third", now.Add(2*time.Second), "INFO"),
	})

	time.Sleep(250 * time.Millisecond)

	counters := collectInt64Counters(t, reader)
	assert.Equal(t, int64(3), counters[metricLogsIngested])
	assert.Equal(t, int64(1), counters[metricTracesCreated])
	assert.Equal(t, int64(0), counters[metricUnmatchedDropped])
}

func TestMetricsUnmatchedDropped(t *testing.T) {
	sink := newTestSink()
	conn, reader := createMetricsTestConnector(t, metricsTestConfig(), sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	sendLogs(t, conn, []plog.LogRecord{
		newLogRecord("user=123 matched", now, "INFO"),
		newLogRecord("no key here", now, "INFO"),
		newLogRecord("still nothing", now, "INFO"),
	})

	time.Sleep(250 * time.Millisecond)

	counters := collectInt64Counters(t, reader)
	assert.Equal(t, int64(3), counters[metricLogsIngested])
	assert.Equal(t, int64(2), counters[metricUnmatchedDropped],
		"unmatched records are dropped, and that must be observable")
	assert.Equal(t, int64(1), counters[metricTracesCreated])
}

func TestMetricsTracesCreatedCountsEveryGroup(t *testing.T) {
	sink := newTestSink()
	conn, reader := createMetricsTestConnector(t, metricsTestConfig(), sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	sendLogs(t, conn, []plog.LogRecord{
		newLogRecord("user=1 one", now, "INFO"),
		newLogRecord("user=2 two", now, "INFO"),
		newLogRecord("user=3 three", now, "INFO"),
	})

	time.Sleep(250 * time.Millisecond)

	counters := collectInt64Counters(t, reader)
	assert.Equal(t, int64(3), counters[metricTracesCreated])
	require.Len(t, sink.AllTraces(), 3)
}

// The meter is optional outside the collector: constructing the connector
// without one must not panic.
func TestMetricsNilMeterProviderFallsBackToNoop(t *testing.T) {
	sink := newTestSink()
	settings := connector.Settings{
		ID: component.MustNewID(TypeStr),
		TelemetrySettings: component.TelemetrySettings{
			Logger: zap.NewNop(),
		},
	}
	conn, err := NewFactory().CreateLogsToTraces(context.Background(), settings, metricsTestConfig(), sink)
	require.NoError(t, err)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	sendLogs(t, conn, []plog.LogRecord{
		newLogRecord("user=123 matched", now, "INFO"),
		newLogRecord("unmatched", now, "INFO"),
	})
	require.NoError(t, conn.Shutdown(context.Background()))
}
