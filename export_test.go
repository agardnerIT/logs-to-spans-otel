// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs_to_spans

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/agardnerIT/logs-to-spans-otel/internal/metadata"
)

// errTracesConsumer always fails, standing in for a downstream exporter that is
// rejecting data (a broken backend, an auth failure, a full queue).
type errTracesConsumer struct {
	calls atomic.Int64
	err   error
}

func (*errTracesConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (c *errTracesConsumer) ConsumeTraces(context.Context, ptrace.Traces) error {
	c.calls.Add(1)
	return c.err
}

// blockingTracesConsumer blocks every call until release is closed, ignoring
// the context, to model an exporter that is wedged rather than merely slow.
type blockingTracesConsumer struct {
	release chan struct{}
	entered chan struct{}
	calls   atomic.Int64
}

func newBlockingTracesConsumer() *blockingTracesConsumer {
	return &blockingTracesConsumer{
		release: make(chan struct{}),
		entered: make(chan struct{}, 16),
	}
}

func (*blockingTracesConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (c *blockingTracesConsumer) ConsumeTraces(context.Context, ptrace.Traces) error {
	c.calls.Add(1)
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-c.release
	return nil
}

// recordingTracesConsumer counts calls and the ResourceSpans each carried, so a
// test can tell "one batched export of three groups" from "three exports".
type recordingTracesConsumer struct {
	mu        sync.Mutex
	calls     int
	resources int
	spans     int
	err       error
}

func (*recordingTracesConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (c *recordingTracesConsumer) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.resources += td.ResourceSpans().Len()
	c.spans += td.SpanCount()
	return c.err
}

func (c *recordingTracesConsumer) snapshot() (calls, resources, spans int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, c.resources, c.spans
}

// createConnectorWithConsumer builds a started connector around a custom traces
// consumer, so a test can control or observe the downstream export.
func createConnectorWithConsumer(t *testing.T, cfg *Config, tc consumer.Traces) *logsToSpansConnector {
	t.Helper()
	conn, err := NewFactory().CreateLogsToTraces(t.Context(), newTestSettings(), cfg, tc)
	require.NoError(t, err)
	require.NoError(t, conn.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() { _ = conn.Shutdown(t.Context()) })
	return conn.(*logsToSpansConnector)
}

func createMetricsConnectorWithConsumer(t *testing.T, cfg *Config, tc consumer.Traces) (*logsToSpansConnector, *metric.ManualReader) {
	t.Helper()
	settings, reader := newMetricsTestSettings(t)
	conn, err := NewFactory().CreateLogsToTraces(t.Context(), settings, cfg, tc)
	require.NoError(t, err)
	require.NoError(t, conn.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() { _ = conn.Shutdown(t.Context()) })
	return conn.(*logsToSpansConnector), reader
}

// counterByReason returns the value of the counter data points that carry
// reason=<reason>.
func counterByReason(t *testing.T, reader *metric.ManualReader, reason string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricRecordsDropped {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				if v, ok := dp.Attributes.Value("reason"); ok && v.AsString() == reason {
					total += dp.Value
				}
			}
		}
	}
	return total
}

// TestConsumeLogsReturnsDownstreamError covers issue #24: a batch that was
// accepted must not be acknowledged when every export built from it failed.
func TestConsumeLogsReturnsDownstreamError(t *testing.T) {
	sink := &errTracesConsumer{err: errors.New("backend is down")}
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.MaxLogsPerTrace = 2
	cfg.Timeout = 10 * time.Second
	cfg.MaxWait = 10 * time.Second
	conn, reader := createMetricsConnectorWithConsumer(t, cfg, sink)

	now := time.Now()
	var records []plog.LogRecord
	for range 10 {
		records = append(records, newLogRecord("user=123 one", now, "INFO"))
	}

	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	for _, lr := range records {
		lr.CopyTo(sl.LogRecords().AppendEmpty())
	}

	err := conn.ConsumeLogs(t.Context(), ld)
	require.Error(t, err, "a failed downstream export must not be reported as success")
	assert.Contains(t, err.Error(), "backend is down")
	assert.Equal(t, int64(1), sink.calls.Load(),
		"the 5 groups split out of this batch share one downstream export")

	assert.Equal(t, int64(10), counterByReason(t, reader, dropReasonDownstreamError))
}

// TestConsumeLogsAfterShutdownReturnsError covers the "stopped" drop path: a
// record that cannot be buffered is an error, not a silent success.
func TestConsumeLogsAfterShutdownReturnsError(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	conn, reader := createMetricsConnectorWithConsumer(t, cfg, &recordingTracesConsumer{})
	require.NoError(t, conn.Shutdown(t.Context()))

	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	newLogRecord("user=123 after shutdown", time.Now(), "INFO").CopyTo(sl.LogRecords().AppendEmpty())

	err := conn.ConsumeLogs(t.Context(), ld)
	require.Error(t, err)
	assert.ErrorIs(t, err, errConnectorStopped)
	assert.Equal(t, int64(1), counterByReason(t, reader, dropReasonStopped))
}

// TestShutdownBatchesGroupsIntoOneExport covers issue #34: shutdown must not
// issue one downstream call per buffered group.
func TestShutdownBatchesGroupsIntoOneExport(t *testing.T) {
	sink := &recordingTracesConsumer{}
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.Timeout = time.Hour
	cfg.MaxWait = time.Hour
	conn := createConnectorWithConsumer(t, cfg, sink)

	now := time.Now()
	sendLogs(t, conn, []plog.LogRecord{
		newLogRecord("user=1 first", now, "INFO"),
		newLogRecord("user=2 second", now, "INFO"),
		newLogRecord("user=3 third", now, "INFO"),
		newLogRecord("user=1 fourth", now, "INFO"),
	})

	require.NoError(t, conn.Shutdown(t.Context()))

	calls, resources, spans := sink.snapshot()
	assert.Equal(t, 1, calls, "3 buffered groups must be one downstream export")
	assert.Equal(t, 3, resources, "each group is one ResourceSpans")
	assert.Equal(t, 4, spans)
}

// TestShutdownReleasesWedgedExport covers the second half of #26: a downstream
// export that never returns must not hang Shutdown forever.
func TestShutdownReleasesWedgedExport(t *testing.T) {
	blocking := newBlockingTracesConsumer()
	t.Cleanup(func() { close(blocking.release) })

	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.Timeout = time.Hour
	cfg.MaxWait = time.Hour
	conn := createConnectorWithConsumer(t, cfg, blocking)
	conn.exportTimeout = 20 * time.Millisecond
	conn.shutdownWait = 200 * time.Millisecond

	sendLogs(t, conn, []plog.LogRecord{newLogRecord("user=123 buffered", time.Now(), "INFO")})

	done := make(chan error, 1)
	go func() { done <- conn.Shutdown(t.Context()) }()

	select {
	case err := <-done:
		require.Error(t, err, "a wedged export must be reported, not hidden")
		assert.Contains(t, err.Error(), "shutdown timeout")
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown hung on a wedged downstream export")
	}
}

// TestReaperDoesNotStallOnSlowExport covers the first half of #26: while one
// export is blocked, deadlines must still be enforced for other groups.
func TestReaperDoesNotStallOnSlowExport(t *testing.T) {
	blocking := newBlockingTracesConsumer()
	t.Cleanup(func() { close(blocking.release) })

	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.Timeout = 30 * time.Millisecond
	cfg.MaxWait = 30 * time.Millisecond
	cfg.MaxRecordBytes = 0
	conn := createConnectorWithConsumer(t, cfg, blocking)
	conn.exportTimeout = time.Minute

	sendLogs(t, conn, []plog.LogRecord{newLogRecord("user=1 first", time.Now(), "INFO")})

	select {
	case <-blocking.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first group was never exported")
	}

	// The reaper goroutine is now free of the blocked export, so the second
	// group's deadline must still fire and reach a second worker.
	sendLogs(t, conn, []plog.LogRecord{newLogRecord("user=2 second", time.Now(), "INFO")})

	select {
	case <-blocking.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the reaper stalled behind a blocked downstream export")
	}
}

// TestShutdownFlushesWithCancelledContext covers the third drop path in #24: a
// shutdown context that is already dead (the collector's shutdown budget was
// spent elsewhere) must not discard buffered records.
func TestShutdownFlushesWithCancelledContext(t *testing.T) {
	sink := &recordingTracesConsumer{}
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.Timeout = time.Hour
	cfg.MaxWait = time.Hour
	conn := createConnectorWithConsumer(t, cfg, sink)

	sendLogs(t, conn, []plog.LogRecord{newLogRecord("user=123 buffered", time.Now(), "INFO")})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, conn.Shutdown(ctx))

	calls, _, spans := sink.snapshot()
	assert.Equal(t, 1, calls)
	assert.Equal(t, 1, spans)
}

// TestNoInfoLogPerEmittedTrace covers issue #29: the per-trace log line is
// Debug, so a busy connector does not flood the collector's logging pipeline or
// leak group keys at a default-on level.
func TestNoInfoLogPerEmittedTrace(t *testing.T) {
	core, observed := observer.New(zapcore.InfoLevel)
	settings := connectortest.NewNopSettings(metadata.Type)
	settings.Logger = zap.New(core)

	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.Timeout = 20 * time.Millisecond

	conn, err := NewFactory().CreateLogsToTraces(t.Context(), settings, cfg, newTestSink())
	require.NoError(t, err)
	require.NoError(t, conn.Start(t.Context(), componenttest.NewNopHost()))

	sendLogs(t, conn, []plog.LogRecord{newLogRecord("user=123 secret-session-id", time.Now(), "INFO")})
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, conn.Shutdown(t.Context()))

	assert.Zero(t, observed.FilterMessage("converted log group to trace").Len(),
		"the per-trace line must not be emitted at Info level")
	assert.Zero(t, observed.Len(), "no Info-level output on the successful path")
}

// TestConsumeLogsDropsNothingOnSuccess is the counterpart to the error tests:
// the healthy path must keep reporting success and must not touch the drop
// counter.
func TestConsumeLogsDropsNothingOnSuccess(t *testing.T) {
	sink := &recordingTracesConsumer{}
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.MaxLogsPerTrace = 2
	conn, reader := createMetricsConnectorWithConsumer(t, cfg, sink)

	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	for range 4 {
		newLogRecord("user=123 fine", time.Now(), "INFO").CopyTo(sl.LogRecords().AppendEmpty())
	}
	require.NoError(t, conn.ConsumeLogs(t.Context(), ld))
	require.NoError(t, conn.Shutdown(t.Context()))

	assert.Zero(t, counterByReason(t, reader, dropReasonDownstreamError))
	assert.Zero(t, counterByReason(t, reader, dropReasonStopped))
	assert.Zero(t, counterByReason(t, reader, dropReasonShutdown))
}

// TestShippedTraceIDKeysTrustBoundary pins the documented default that makes
// attribute-sourced IDs opt-out: an operator who does not trust the log source
// can empty the lists, and no link is produced from attributes.
func TestShippedTraceIDKeysTrustBoundary(t *testing.T) {
	cfg := createDefaultConfig()
	require.NotEmpty(t, cfg.TraceIDKeys)
	require.NotEmpty(t, cfg.SpanIDKeys)
	require.False(t, cfg.RequireSpanID)

	untrusted := createDefaultConfig()
	untrusted.TraceIDKeys = []string{}
	untrusted.SpanIDKeys = []string{}
	untrusted.GroupByKeys = []string{"user"}
	untrusted.Timeout = 20 * time.Millisecond
	require.NoError(t, untrusted.Validate())

	sink := newTestSink()
	conn := createTestConnector(t, untrusted, sink)
	sendLogs(t, conn, []plog.LogRecord{newLogRecordWithAttrs(
		"user=123 forged", time.Now(), "INFO",
		map[string]string{"trace_id": "abababababababababababababababab"},
	)})
	time.Sleep(100 * time.Millisecond)

	traces := flatTraces(sink)
	require.Len(t, traces, 1)
	span := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	assert.Zero(t, span.Links().Len(), "with the attribute lookups disabled nothing can forge a link")
}
