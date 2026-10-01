// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs_to_spans

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
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

// newBenchmarkConnector builds a connector for the benchmarks. maxLogsPerTrace
// controls whether the measurement includes max_logs_per_trace split handling:
//
//   - 0 disables the cap, so the benchmark measures the steady-state per-record
//     grouping path (extraction plus the group-map splice inside the connector
//     mutex) with no trace construction. That is the path issue #20 is about.
//   - a positive value makes the group emit a trace and start a linked
//     replacement at the cap. Building that trace allocates pdata, sorts the
//     batch and draws a crypto/rand span ID per span, which dominates the
//     allocation profile and caps 8-core scaling on GC rather than on the
//     connector lock. BenchmarkConsumeLogsStructuredParallelSplits keeps a
//     measurement of that combined shape.
//
// Timeout and max_wait are an hour either way so the reaper never fires
// mid-measurement.
func newBenchmarkConnector(b *testing.B, maxLogsPerTrace int) *logsToSpansConnector {
	b.Helper()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.Timeout = time.Hour
	cfg.MaxWait = time.Hour
	cfg.MaxLogsPerTrace = maxLogsPerTrace

	factory := NewFactory()
	// A nop traces consumer, not a recording sink: consumertest.TracesSink takes
	// its own mutex, which would serialize the parallel benchmark and hide what
	// the connector's own lock does.
	conn, err := factory.CreateLogsToTraces(b.Context(), newTestSettings(), cfg, consumertest.NewNop())
	require.NoError(b, err)
	require.NoError(b, conn.Start(b.Context(), componenttest.NewNopHost()))
	return conn.(*logsToSpansConnector)
}

// drainBenchmarkGroups drops any buffered records before Shutdown. The
// cap-disabled benchmarks accumulate every record in one group; without this
// Shutdown would construct one enormous trace at cleanup. It runs after the
// measurement, so it cannot affect the reported numbers.
func drainBenchmarkGroups(conn *logsToSpansConnector) {
	conn.mu.Lock()
	conn.groups = make(map[string]*logGroup)
	conn.lru.Init()
	conn.mu.Unlock()
}

// BenchmarkConsumeLogsStructured measures single-goroutine throughput on a Map
// body with the grouping path isolated. It is the baseline for the parallel
// benchmark: with one caller the mutex is uncontended, so it mostly shows the
// extraction cost itself.
func BenchmarkConsumeLogsStructured(b *testing.B) {
	conn := newBenchmarkConnector(b, 0)
	defer func() {
		drainBenchmarkGroups(conn)
		require.NoError(b, conn.Shutdown(b.Context()))
	}()

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
// against one connector, with the max_logs_per_trace cap disabled so the
// measurement is the group-map path alone. Before issue #15 the map-body JSON
// conversion ran inside the global mutex and serialized every core; after #15
// that cost moved out but a time.AfterFunc Stop+reset per record kept the lock
// hot. #20 removed the timers entirely (a single reaper owns every deadline),
// made max_groups eviction O(1) off an LRU list and batches a whole group's
// records into one lock acquisition per ConsumeLogs call, so this case now
// scales with the cores instead of sitting just above the single-caller number.
func BenchmarkConsumeLogsStructuredParallel(b *testing.B) {
	conn := newBenchmarkConnector(b, 0)
	defer func() {
		drainBenchmarkGroups(conn)
		require.NoError(b, conn.Shutdown(b.Context()))
	}()

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

// BenchmarkConsumeLogsStructuredParallelSplits is the same parallel workload
// with max_logs_per_trace at 1000, the shape used before #20. Each split now
// builds and emits a trace, and that path (not the connector lock) dominates:
// it allocates pdata per span and draws a crypto/rand span ID per span, so the
// 8-core number is GC-bound. It is kept to show that turning the cap on does
// not regress and to make the allocation boundary explicit.
func BenchmarkConsumeLogsStructuredParallelSplits(b *testing.B) {
	conn := newBenchmarkConnector(b, 1000)
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
