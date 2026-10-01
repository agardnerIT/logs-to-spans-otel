// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs_to_spans

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// benchBatchSize is the number of structured records fed to the connector per
// ConsumeLogs call in the benchmarks below.
const benchBatchSize = 100

// newStructuredBenchmarkLogs builds a batch of log records with a Map body, the
// shape a JSON log receiver produces. Body extraction for this shape is the
// cost issue #15 moved out of the connector's global mutex: valueToString calls
// Value.AsString(), which serializes the whole body to a string.
func newStructuredBenchmarkLogs(n int) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "benchmark")
	sl := rl.ScopeLogs().AppendEmpty()
	for i := range n {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, int64(i))))
		lr.SetSeverityText("INFO")
		body := lr.Body().SetEmptyMap()
		body.PutStr("user", "u-1234")
		body.PutStr("message", "request completed")
		body.PutStr("path", "/api/v1/orders")
		body.PutStr("method", "GET")
		body.PutInt("status", 200)
		body.PutDouble("duration_ms", 12.5)
		body.PutStr("request_id", "3f1c9b0e-0f2a-4a6a-9d4e-2b7c5a8e1f00")
		body.PutStr("client_ip", "203.0.113.7")
		body.PutStr("user_agent", "benchmark/1.0")
		body.PutStr("host", "api-7")
	}
	return ld
}

func newBenchmarkConnector(b *testing.B) *logsToSpansConnector {
	b.Helper()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	// Keep the groups bounded and stop the timers firing mid-benchmark, so the
	// measurement is the steady-state per-record path rather than unbounded
	// growth or a timer flush.
	cfg.Timeout = time.Hour
	cfg.MaxWait = time.Hour
	cfg.MaxLogsPerTrace = 1000

	factory := NewFactory()
	// A nop traces consumer, not a recording sink: consumertest.TracesSink takes
	// its own mutex, which would serialize the parallel benchmark and hide what
	// the connector's own lock does.
	conn, err := factory.CreateLogsToTraces(b.Context(), newTestSettings(), cfg, consumertest.NewNop())
	require.NoError(b, err)
	return conn.(*logsToSpansConnector)
}

// BenchmarkConsumeLogsStructured measures single-goroutine throughput on a Map
// body. It is the baseline: with one caller the mutex is uncontended, so it
// mostly shows the extraction cost itself.
func BenchmarkConsumeLogsStructured(b *testing.B) {
	conn := newBenchmarkConnector(b)
	defer func() { require.NoError(b, conn.Shutdown(b.Context())) }()

	ld := newStructuredBenchmarkLogs(benchBatchSize)
	ctx := b.Context()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, conn.ConsumeLogs(ctx, ld))
	}
	b.ReportMetric(float64(b.N*benchBatchSize)/b.Elapsed().Seconds(), "logs/sec")
}

// BenchmarkConsumeLogsStructuredParallel runs concurrent ConsumeLogs callers
// against one connector. Before issue #15 this is where the global mutex bit:
// the map-body JSON conversion ran inside the critical section shared by all
// groups and timer callbacks, serializing every core. After the fix only the
// map/list splice stays under the lock, so this case now runs faster than the
// single-goroutine baseline instead of slower. It does not scale linearly:
// timer Stop + AfterFunc per record is still serialized by the lock.
func BenchmarkConsumeLogsStructuredParallel(b *testing.B) {
	conn := newBenchmarkConnector(b)
	defer func() { require.NoError(b, conn.Shutdown(b.Context())) }()

	ld := newStructuredBenchmarkLogs(benchBatchSize)
	ctx := b.Context()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := conn.ConsumeLogs(ctx, ld); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.ReportMetric(float64(b.N*benchBatchSize)/b.Elapsed().Seconds(), "logs/sec")
}

// BenchmarkExtractLogRecord isolates the extraction the fix moved out of the
// mutex, so the serialized cost removed from the critical section is visible on
// its own.
func BenchmarkExtractLogRecord(b *testing.B) {
	ld := newStructuredBenchmarkLogs(1)
	lr := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	cfg := createDefaultConfig()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = extractLogRecord(lr, cfg)
	}
}
