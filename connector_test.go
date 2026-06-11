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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

func newTestSink() *consumertest.TracesSink {
	return &consumertest.TracesSink{}
}

func newTestSettings() connector.Settings {
	return connector.Settings{
		ID: component.MustNewID("logs_to_spans"),
		TelemetrySettings: component.TelemetrySettings{
			Logger: zap.NewNop(),
		},
	}
}

func createTestConnector(t *testing.T, cfg *Config, sink *consumertest.TracesSink) connector.Logs {
	t.Helper()
	factory := NewFactory()
	conn, err := factory.CreateLogsToTraces(context.Background(), newTestSettings(), cfg, sink)
	require.NoError(t, err)
	return conn
}

func newLogRecord(body string, ts time.Time, severity string) plog.LogRecord {
	return newLogRecordWithAttrs(body, ts, severity, nil)
}

func newLogRecordWithAttrs(body string, ts time.Time, severity string, attrs map[string]string) plog.LogRecord {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()

	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(ts))
	lr.Body().SetStr(body)
	if severity != "" {
		lr.SetSeverityText(severity)
	}
	for k, v := range attrs {
		lr.Attributes().PutStr(k, v)
	}
	return lr
}

func sendLogs(t *testing.T, conn connector.Logs, records []plog.LogRecord) {
	t.Helper()
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	for _, lr := range records {
		lr.CopyTo(sl.LogRecords().AppendEmpty())
	}
	err := conn.ConsumeLogs(context.Background(), ld)
	require.NoError(t, err)
}

func TestConfigDefaults(t *testing.T) {
	cfg := createDefaultConfig()
	assert.Equal(t, 5*time.Second, cfg.Timeout)
	assert.Equal(t, 500*time.Millisecond, cfg.EndSpanDuration)
	assert.Equal(t, "drop", cfg.UnmatchedBehaviour)
	assert.Equal(t, "logs-to-spans", cfg.ServiceName)
	assert.Empty(t, cfg.GroupByKeys)
}

func TestExtractGroupKey_Unstructured(t *testing.T) {
	keys := []string{"user", "userID", "user_id"}

	tests := []struct {
		name     string
		body     string
		expected string
	}{
		{"key=value", "INFO Hello world userID=123", "123"},
		{"at end", "some text user=foo", "foo"},
		{"multiple keys", "user=abc userID=def", "abc"},
		{"no match", "INFO Foo bar", ""},
		{"with special chars", "user=abc123 hello", "abc123"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := newLogRecord(tt.body, time.Now(), "INFO")
			got := extractGroupKey(lr, keys)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestExtractGroupKey_Structured(t *testing.T) {
	keys := []string{"user", "user_id"}

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetEmptyMap().PutStr("user", "123")
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	got := extractGroupKey(lr, keys)
	assert.Equal(t, "123", got)
}

func TestExtractGroupKey_StructuredNoMatch(t *testing.T) {
	keys := []string{"userID"}

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetEmptyMap().PutStr("user", "123")
	lr.Body().Map().PutStr("status", "ok")
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	got := extractGroupKey(lr, keys)
	assert.Equal(t, "", got)
}

func TestBasicGrouping(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"userID", "user_id"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecord("Hello userID=123", now, "INFO")
	r2 := newLogRecord("World user_id=123", now.Add(1*time.Second), "ERROR")

	sendLogs(t, conn, []plog.LogRecord{r1, r2})

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1, "expected exactly one trace export")

	td := traces[0]
	assert.Equal(t, 2, td.SpanCount(), "expected 2 spans")

	rs := td.ResourceSpans().At(0)
	ss := rs.ScopeSpans().At(0)
	span0 := ss.Spans().At(0)
	span1 := ss.Spans().At(1)

	assert.Equal(t, span0.TraceID(), span1.TraceID(), "spans must share trace ID")
	assert.Equal(t, span0.SpanID(), span1.ParentSpanID(), "span1 should have span0 as parent")

	val, ok := span0.Attributes().Get("group.key")
	require.True(t, ok)
	assert.Equal(t, "123", val.Str())
}

func TestMultipleGroups(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecord("user=123 first", now, "INFO")
	r2 := newLogRecord("user=456 second", now, "INFO")
	r3 := newLogRecord("user=123 third", now.Add(1*time.Second), "INFO")

	sendLogs(t, conn, []plog.LogRecord{r1, r2, r3})

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	assert.Len(t, traces, 2, "expected two traces (one per group)")

	spans := 0
	for _, td := range traces {
		spans += td.SpanCount()
	}
	assert.Equal(t, 3, spans, "expected 3 total spans across both traces")
}

func TestEndSpanDuration(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.EndSpanDuration = 2 * time.Second
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecord("user=123 first", now, "INFO")
	r2 := newLogRecord("user=123 second", now.Add(1*time.Second), "INFO")

	sendLogs(t, conn, []plog.LogRecord{r1, r2})

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	rs := traces[0].ResourceSpans().At(0)
	ss := rs.ScopeSpans().At(0)
	require.Equal(t, 2, ss.Spans().Len())

	// span0: first log -> second log (1s duration)
	span0 := ss.Spans().At(0)
	span0End := span0.EndTimestamp().AsTime()
	assert.Equal(t, now.Add(1*time.Second), span0End)

	// span1: second log -> +2s (configurable end span duration)
	span1 := ss.Spans().At(1)
	span1End := span1.EndTimestamp().AsTime()
	assert.Equal(t, now.Add(1*time.Second).Add(2*time.Second), span1End)
}

func TestSortByTimestamp(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecord("user=123 third", now.Add(2*time.Second), "INFO")
	r2 := newLogRecord("user=123 first", now, "INFO")
	r3 := newLogRecord("user=123 second", now.Add(1*time.Second), "INFO")

	sendLogs(t, conn, []plog.LogRecord{r1, r2, r3})

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	rs := traces[0].ResourceSpans().At(0)
	ss := rs.ScopeSpans().At(0)
	require.Equal(t, 3, ss.Spans().Len())

	names := make([]string, ss.Spans().Len())
	for i := 0; i < ss.Spans().Len(); i++ {
		names[i] = ss.Spans().At(i).Name()
	}
	assert.Equal(t, []string{"user=123 first", "user=123 second", "user=123 third"}, names)
}

func TestShutdown(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 1 * time.Hour
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	r1 := newLogRecord("user=123 hello", now, "INFO")
	sendLogs(t, conn, []plog.LogRecord{r1})

	conn.Shutdown(context.Background())

	traces := sink.AllTraces()
	require.Len(t, traces, 1, "expected trace export on shutdown")

	// span must exist and record the log body
	rs := traces[0].ResourceSpans().At(0)
	ss := rs.ScopeSpans().At(0)
	require.Equal(t, 1, ss.Spans().Len())

	val, ok := ss.Spans().At(0).Attributes().Get("log.body")
	require.True(t, ok)
	assert.Equal(t, "user=123 hello", val.Str())
}

func TestEmptyGroupByKeys(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	r1 := newLogRecord("user=123 hello", now, "INFO")
	sendLogs(t, conn, []plog.LogRecord{r1})

	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	assert.Empty(t, traces, "no traces should be produced with empty group_by_keys")
}

func TestSeverityAttribute(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	r1 := newLogRecord("user=123 hello", now, "ERROR")
	sendLogs(t, conn, []plog.LogRecord{r1})

	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	rs := traces[0].ResourceSpans().At(0)
	ss := rs.ScopeSpans().At(0)
	require.Equal(t, 1, ss.Spans().Len())

	val, ok := ss.Spans().At(0).Attributes().Get("log.severity")
	require.True(t, ok)
	assert.Equal(t, "ERROR", val.Str())
}

func TestLogBodyAsSpanName(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user_id", "userID"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecord("2026-10-25 10:00:00 ERROR user_id=123 Hello world", now, "ERROR")
	sendLogs(t, conn, []plog.LogRecord{r1})

	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	rs := traces[0].ResourceSpans().At(0)
	ss := rs.ScopeSpans().At(0)
	require.Equal(t, 1, ss.Spans().Len())

	assert.Contains(t, ss.Spans().At(0).Name(), "Hello world")
}

func TestServiceNameOnResource(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	cfg.ServiceName = "my-app"
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecord("user=123 hello", now, "INFO")
	sendLogs(t, conn, []plog.LogRecord{r1})

	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	rs := traces[0].ResourceSpans().At(0)
	svc, ok := rs.Resource().Attributes().Get("service.name")
	require.True(t, ok)
	assert.Equal(t, "my-app", svc.Str())
}

func TestMultipleKeyCandidates(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user", "userID", "user_id"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	r1 := newLogRecord("hello user=456", now, "INFO")
	r2 := newLogRecord("hello userID=456", now.Add(1*time.Second), "INFO")

	sendLogs(t, conn, []plog.LogRecord{r1, r2})

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1, "r1 and r2 should be grouped by value 456")
	assert.Equal(t, 2, traces[0].SpanCount())
}
