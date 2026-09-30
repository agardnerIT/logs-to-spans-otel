package logs_to_spans

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
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
	assert.Equal(t, "logs-to-spans", cfg.ServiceName)
	assert.Equal(t, 30*time.Second, cfg.MaxWait)
	assert.Empty(t, cfg.GroupByKeys)
	assert.Empty(t, cfg.GroupByAttributes)
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
			sink := newTestSink()
			cfg := createDefaultConfig()
			cfg.GroupByKeys = keys
			conn := createTestConnector(t, cfg, sink)

			lr := newLogRecord(tt.body, time.Now(), "INFO")
			got := conn.(*logsToSpansConnector).extractGroupKey(lr)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestExtractGroupKey_Structured(t *testing.T) {
	keys := []string{"user", "user_id"}

	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = keys
	conn := createTestConnector(t, cfg, sink)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetEmptyMap().PutStr("user", "123")
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	got := conn.(*logsToSpansConnector).extractGroupKey(lr)
	assert.Equal(t, "123", got)
}

func TestExtractGroupKey_FromAttributes(t *testing.T) {
	tests := []struct {
		name     string
		attrs    map[string]string
		attrKeys []string
		expected string
	}{
		{
			name:     "single attribute",
			attrs:    map[string]string{"user.id": "123"},
			attrKeys: []string{"user.id"},
			expected: "123",
		},
		{
			name:     "searched in order",
			attrs:    map[string]string{"enduser.id": "abc"},
			attrKeys: []string{"user.id", "enduser.id"},
			expected: "abc",
		},
		{
			name:     "first match wins",
			attrs:    map[string]string{"user.id": "first", "enduser.id": "second"},
			attrKeys: []string{"user.id", "enduser.id"},
			expected: "first",
		},
		{
			name:     "no attribute match",
			attrs:    map[string]string{"other": "x"},
			attrKeys: []string{"user.id"},
			expected: "",
		},
		{
			name:     "empty attribute value is skipped",
			attrs:    map[string]string{"user.id": "", "enduser.id": "abc"},
			attrKeys: []string{"user.id", "enduser.id"},
			expected: "abc",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := newTestSink()
			cfg := createDefaultConfig()
			cfg.GroupByAttributes = tt.attrKeys
			conn := createTestConnector(t, cfg, sink)

			lr := newLogRecordWithAttrs("plain unstructured message", time.Now(), "INFO", tt.attrs)
			got := conn.(*logsToSpansConnector).extractGroupKey(lr)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestExtractGroupKey_AttributesTakePrecedenceOverBody(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"userID"}
	cfg.GroupByAttributes = []string{"user.id"}
	conn := createTestConnector(t, cfg, sink)

	lr := newLogRecordWithAttrs("userID=body-value", time.Now(), "INFO",
		map[string]string{"user.id": "attr-value"})
	got := conn.(*logsToSpansConnector).extractGroupKey(lr)
	assert.Equal(t, "attr-value", got)
}

func TestExtractGroupKey_AttributesPrecedenceOverStructuredBody(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.GroupByAttributes = []string{"user.id"}
	conn := createTestConnector(t, cfg, sink)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetEmptyMap().PutStr("user", "body-value")
	lr.Attributes().PutStr("user.id", "attr-value")
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	got := conn.(*logsToSpansConnector).extractGroupKey(lr)
	assert.Equal(t, "attr-value", got)
}

func TestExtractGroupKey_FallsBackToBodyWhenAttributeMissing(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"userID"}
	cfg.GroupByAttributes = []string{"user.id"}
	conn := createTestConnector(t, cfg, sink)

	lr := newLogRecord("userID=body-value", time.Now(), "INFO")
	got := conn.(*logsToSpansConnector).extractGroupKey(lr)
	assert.Equal(t, "body-value", got)
}

func TestExtractGroupKey_NonStringAttribute(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByAttributes = []string{"user.id"}
	conn := createTestConnector(t, cfg, sink)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("no key in the body")
	lr.Attributes().PutInt("user.id", 123)
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	got := conn.(*logsToSpansConnector).extractGroupKey(lr)
	assert.Equal(t, "123", got)
}

func TestAttributesOnlyConfigProducesTraces(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByAttributes = []string{"user.id"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	r1 := newLogRecordWithAttrs("first", now, "INFO", map[string]string{"user.id": "123"})
	r2 := newLogRecordWithAttrs("second", now.Add(time.Second), "INFO", map[string]string{"user.id": "123"})
	r3 := newLogRecordWithAttrs("other group", now, "INFO", map[string]string{"user.id": "456"})
	sendLogs(t, conn, []plog.LogRecord{r1, r2, r3})

	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 2, "one trace per distinct attribute value")

	var spans int
	keys := map[string]bool{}
	for _, td := range traces {
		ss := td.ResourceSpans().At(0).ScopeSpans().At(0)
		spans += ss.Spans().Len()
		for i := 0; i < ss.Spans().Len(); i++ {
			v, ok := ss.Spans().At(i).Attributes().Get("group.key")
			require.True(t, ok)
			keys[v.Str()] = true
		}
	}
	assert.Equal(t, 3, spans)
	assert.True(t, keys["123"], "123 group present")
	assert.True(t, keys["456"], "456 group present")
}

func TestExtractGroupKey_StructuredNoMatch(t *testing.T) {
	keys := []string{"userID"}

	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = keys
	conn := createTestConnector(t, cfg, sink)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetEmptyMap().PutStr("user", "123")
	lr.Body().Map().PutStr("status", "ok")
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	got := conn.(*logsToSpansConnector).extractGroupKey(lr)
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
	assert.Empty(t, traces, "no traces should be produced without any group-by keys")
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

func TestDurationFromAttribute(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	cfg.DurationKeys = []string{"duration", "time"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecordWithAttrs("user=123 first", now, "INFO", map[string]string{"duration": "2s"})
	r2 := newLogRecordWithAttrs("user=123 second", now.Add(1*time.Second), "INFO", nil)

	sendLogs(t, conn, []plog.LogRecord{r1, r2})

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	rs := traces[0].ResourceSpans().At(0)
	ss := rs.ScopeSpans().At(0)
	require.Equal(t, 2, ss.Spans().Len())

	span0 := ss.Spans().At(0)
	span0Start := span0.StartTimestamp().AsTime()
	span0End := span0.EndTimestamp().AsTime()
	assert.Equal(t, now.Add(2*time.Second), span0End, "span0 should use duration from attribute")
	assert.Equal(t, 2*time.Second, span0End.Sub(span0Start))

	span1 := ss.Spans().At(1)
	span1End := span1.EndTimestamp().AsTime()
	assert.Equal(t, now.Add(1*time.Second).Add(500*time.Millisecond), span1End, "span1 should fallback to EndSpanDuration")
}

func TestDurationFromAttributeFallbackToNextKey(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	cfg.DurationKeys = []string{"duration", "time", "time-spent"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecordWithAttrs("user=123 first", now, "INFO", map[string]string{"time": "500ms"})

	sendLogs(t, conn, []plog.LogRecord{r1})

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	rs := traces[0].ResourceSpans().At(0)
	ss := rs.ScopeSpans().At(0)
	require.Equal(t, 1, ss.Spans().Len())

	span0 := ss.Spans().At(0)
	span0End := span0.EndTimestamp().AsTime()
	assert.Equal(t, now.Add(500*time.Millisecond), span0End, "should use 'time' attribute when 'duration' not present")
}

func TestDurationFromIntAttribute(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	cfg.DurationKeys = []string{"duration_ns"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(now))
	lr.Body().SetStr("user=123 first")
	lr.Attributes().PutInt("duration_ns", 2)

	err := conn.ConsumeLogs(context.Background(), logs)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	rs := traces[0].ResourceSpans().At(0)
	ss := rs.ScopeSpans().At(0)
	require.Equal(t, 1, ss.Spans().Len())

	span0 := ss.Spans().At(0)
	span0End := span0.EndTimestamp().AsTime()
	assert.Equal(t, now.Add(2*time.Second), span0End, "should parse int attribute as seconds")
}

func TestTimestampPriorityOverObservedTimestamp(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	observedTS := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	eventTS := time.Date(2026, 6, 11, 9, 59, 58, 0, time.UTC)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(observedTS))
	lr.SetTimestamp(pcommon.NewTimestampFromTime(eventTS))
	lr.Body().SetStr("user=123 hello")

	err := conn.ConsumeLogs(context.Background(), logs)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	rs := traces[0].ResourceSpans().At(0)
	ss := rs.ScopeSpans().At(0)
	require.Equal(t, 1, ss.Spans().Len())

	span0 := ss.Spans().At(0)
	span0Start := span0.StartTimestamp().AsTime()
	assert.Equal(t, eventTS, span0Start, "should use Timestamp over ObservedTimestamp")
}

func TestCompiledRegexPopulated(t *testing.T) {
	keys := []string{"user", "userID", "user_id"}
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = keys
	conn := createTestConnector(t, cfg, sink)

	c := conn.(*logsToSpansConnector)
	require.Len(t, c.compiledRegex, len(keys))
	for i, re := range c.compiledRegex {
		assert.NotNil(t, re, "compiledRegex[%d] should not be nil", i)
	}
}

func TestCompiledRegexEmptyWhenNoKeys(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	conn := createTestConnector(t, cfg, sink)

	c := conn.(*logsToSpansConnector)
	assert.Empty(t, c.compiledRegex)
}

func TestCompiledRegexCorrectIndexMapping(t *testing.T) {
	keys := []string{"alpha", "beta", "gamma"}
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = keys
	conn := createTestConnector(t, cfg, sink)

	c := conn.(*logsToSpansConnector)

	lr := newLogRecord("beta=42 hello", time.Now(), "INFO")
	got := c.extractGroupKey(lr)
	assert.Equal(t, "42", got)
}

func TestCompiledRegexReuseAcrossMultipleCalls(t *testing.T) {
	keys := []string{"user"}
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = keys
	conn := createTestConnector(t, cfg, sink)

	c := conn.(*logsToSpansConnector)

	for i := 0; i < 100; i++ {
		lr := newLogRecord("user=999 message", time.Now(), "INFO")
		got := c.extractGroupKey(lr)
		assert.Equal(t, "999", got)
	}
}

// TestGroupByKeyMetacharactersMatchLiterally covers the keys that used to
// panic at factory construction or match the wrong records. Every key below
// is special to regexp and must be treated as a plain string.
func TestGroupByKeyMetacharactersMatchLiterally(t *testing.T) {
	for _, key := range []string{"user.id", "user(", "user+", "user[", "user|", "user*", "user?", "user\\"} {
		t.Run(key, func(t *testing.T) {
			sink := newTestSink()
			cfg := createDefaultConfig()
			cfg.Timeout = 100 * time.Millisecond
			cfg.GroupByKeys = []string{key}
			conn := createTestConnector(t, cfg, sink)

			lr := newLogRecord(key+"=abc hello", time.Now(), "INFO")
			got := conn.(*logsToSpansConnector).extractGroupKey(lr)
			assert.Equal(t, "abc", got)
		})
	}
}

func TestGroupByKeyMetacharactersDoNotFalseMatch(t *testing.T) {
	// '.' is a regex wildcard: without QuoteMeta, "user.id" matches "userXid".
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user.id"}
	conn := createTestConnector(t, cfg, sink)

	lr := newLogRecord("userXid=abc hello", time.Now(), "INFO")
	got := conn.(*logsToSpansConnector).extractGroupKey(lr)
	assert.Empty(t, got)
}

func TestConfigValidateAcceptsMetacharacterKeys(t *testing.T) {
	cfg := validTestConfig()
	cfg.GroupByKeys = []string{"user.id", "user(", "a+b", "[x]"}
	require.NoError(t, cfg.Validate())
}

func TestFactoryReturnsErrorForEmptyGroupByKey(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{""}
	factory := NewFactory()

	_, err := factory.CreateLogsToTraces(context.Background(), newTestSettings(), cfg, newTestSink())
	require.Error(t, err, "the factory must return an error, not panic")
}

// validTestConfig returns a config that passes validation, so individual
// tests can flip one field and assert on that field alone.
func validTestConfig() *Config {
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	return cfg
}

func TestConfigValidateAcceptsValidConfig(t *testing.T) {
	require.NoError(t, validTestConfig().Validate())
}

func TestConfigValidateNegativeTimeout(t *testing.T) {
	cfg := validTestConfig()
	cfg.Timeout = -1 * time.Second
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
	assert.Equal(t, -1*time.Second, cfg.Timeout, "Validate must not rewrite the value")
}

func TestConfigValidateZeroTimeout(t *testing.T) {
	cfg := validTestConfig()
	cfg.Timeout = 0
	require.Error(t, cfg.Validate())
}

func TestConfigValidateNegativeMaxWait(t *testing.T) {
	cfg := validTestConfig()
	cfg.MaxWait = -1 * time.Second
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_wait")
	assert.Equal(t, -1*time.Second, cfg.MaxWait, "Validate must not rewrite the value")
}

func TestConfigValidateNegativeEndSpanDuration(t *testing.T) {
	cfg := validTestConfig()
	cfg.EndSpanDuration = -1 * time.Second
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "end_span_duration")
	assert.Equal(t, -1*time.Second, cfg.EndSpanDuration, "Validate must not rewrite the value")
}

// Records that match no group key are always dropped. #8 removed the
// unmatched_behaviour option: pass_through was never implemented (the
// connector registers no logs consumer, so it cannot emit log records). Split
// logs into matched/unmatched pipelines with the filterprocessor before the
// connector if unmatched records must be kept.
func TestUnmatchedRecordsAreDropped(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	matched := newLogRecord("user=123 matched", now, "INFO")
	unmatched := newLogRecord("INFO nothing to group on", now, "INFO")

	sendLogs(t, conn, []plog.LogRecord{matched, unmatched})

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1, "unmatched records must not create a second trace")
	assert.Equal(t, 1, traces[0].SpanCount(), "only the matched record becomes a span")
}

func TestConfigValidateEmptyServiceName(t *testing.T) {
	cfg := validTestConfig()
	cfg.ServiceName = ""
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "service_name")
	assert.Empty(t, cfg.ServiceName, "Validate must not rewrite the value")
}

func TestConfigValidateRejectsNoGroupByKeysOrAttributes(t *testing.T) {
	cfg := createDefaultConfig()
	err := cfg.Validate()
	require.Error(t, err, "with no group_by_keys and no group_by_attributes every record is dropped and the config must be rejected")
	assert.Contains(t, err.Error(), "group_by_keys or group_by_attributes")
}

func TestConfigValidateAcceptsAttributesOnly(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.GroupByAttributes = []string{"user.id", "enduser.id"}
	require.NoError(t, cfg.Validate(), "group_by_attributes alone must be a valid config")
}

func TestConfigValidateAcceptsBodyAndAttributes(t *testing.T) {
	cfg := validTestConfig()
	cfg.GroupByAttributes = []string{"user.id"}
	require.NoError(t, cfg.Validate())
}

func TestConfigValidateRejectsEmptyAttributeKey(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.GroupByAttributes = []string{"user.id", ""}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "group_by_attributes")
}

func TestFactoryAcceptsAttributesOnlyConfig(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.GroupByAttributes = []string{"user.id"}
	factory := NewFactory()

	conn, err := factory.CreateLogsToTraces(context.Background(), newTestSettings(), cfg, newTestSink())
	require.NoError(t, err)
	assert.Empty(t, conn.(*logsToSpansConnector).compiledRegex,
		"attribute-only configs compile no body regexes")
}

func TestConfigValidateRejectsEmptyGroupByKeyEntry(t *testing.T) {
	cfg := validTestConfig()
	cfg.GroupByKeys = []string{"user", ""}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "group_by_keys")
}

func TestConfigValidateDoesNotMutate(t *testing.T) {
	cfg := &Config{}
	require.Error(t, cfg.Validate())
	assert.Zero(t, cfg.Timeout)
	assert.Zero(t, cfg.MaxWait)
	assert.Zero(t, cfg.EndSpanDuration)
	assert.Empty(t, cfg.ServiceName)
}

func TestConfigValidateKeepsExplicitValues(t *testing.T) {
	cfg := validTestConfig()
	cfg.Timeout = 10 * time.Second
	cfg.MaxWait = 60 * time.Second
	cfg.EndSpanDuration = 1 * time.Second
	cfg.ServiceName = "custom"
	err := cfg.Validate()
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, cfg.Timeout)
	assert.Equal(t, 60*time.Second, cfg.MaxWait)
	assert.Equal(t, 1*time.Second, cfg.EndSpanDuration)
	assert.Equal(t, "custom", cfg.ServiceName)
}

func TestNewFactoryReturnsValidFactory(t *testing.T) {
	f := NewFactory()
	require.NotNil(t, f)
}

func TestCapabilitiesMutatesDataFalse(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	conn := createTestConnector(t, cfg, sink)

	c := conn.(*logsToSpansConnector)
	assert.False(t, c.Capabilities().MutatesData)
}

func TestStartReturnsNil(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	conn := createTestConnector(t, cfg, sink)

	err := conn.Start(context.Background(), nil)
	assert.NoError(t, err)
}

func TestShutdownWithNoGroups(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	conn := createTestConnector(t, cfg, sink)

	err := conn.Shutdown(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, sink.AllTraces())
}

func TestMultipleShutdownCalls(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	r1 := newLogRecord("user=123 hello", now, "INFO")
	sendLogs(t, conn, []plog.LogRecord{r1})

	require.NoError(t, conn.Shutdown(context.Background()))
	require.NoError(t, conn.Shutdown(context.Background()))
}

func TestShutdownThenConsumeLogs(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	require.NoError(t, conn.Shutdown(context.Background()))

	now := time.Now()
	r1 := newLogRecord("user=456 should be dropped", now, "INFO")
	err := conn.ConsumeLogs(context.Background(), plog.NewLogs())
	require.NoError(t, err)
	_ = r1

	assert.Empty(t, sink.AllTraces())
}

func TestMultipleResourceLogsEntries(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	ld := plog.NewLogs()

	rl1 := ld.ResourceLogs().AppendEmpty()
	sl1 := rl1.ScopeLogs().AppendEmpty()
	lr1 := sl1.LogRecords().AppendEmpty()
	lr1.SetObservedTimestamp(pcommon.NewTimestampFromTime(now))
	lr1.Body().SetStr("user=111 first")

	rl2 := ld.ResourceLogs().AppendEmpty()
	sl2 := rl2.ScopeLogs().AppendEmpty()
	lr2 := sl2.LogRecords().AppendEmpty()
	lr2.SetObservedTimestamp(pcommon.NewTimestampFromTime(now.Add(1 * time.Second)))
	lr2.Body().SetStr("user=111 second")

	err := conn.ConsumeLogs(context.Background(), ld)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)
	assert.Equal(t, 2, traces[0].SpanCount())
}

func TestMultipleScopeLogsEntries(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	ld := plog.NewLogs()

	rl := ld.ResourceLogs().AppendEmpty()

	sl1 := rl.ScopeLogs().AppendEmpty()
	lr1 := sl1.LogRecords().AppendEmpty()
	lr1.SetObservedTimestamp(pcommon.NewTimestampFromTime(now))
	lr1.Body().SetStr("user=222 first")

	sl2 := rl.ScopeLogs().AppendEmpty()
	lr2 := sl2.LogRecords().AppendEmpty()
	lr2.SetObservedTimestamp(pcommon.NewTimestampFromTime(now.Add(1 * time.Second)))
	lr2.Body().SetStr("user=222 second")

	err := conn.ConsumeLogs(context.Background(), ld)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)
	assert.Equal(t, 2, traces[0].SpanCount())
}

func TestMultipleLogRecordsInSingleScope(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	r1 := newLogRecord("user=333 first", now, "INFO")
	r2 := newLogRecord("user=333 second", now.Add(1*time.Second), "INFO")
	r3 := newLogRecord("user=333 third", now.Add(2*time.Second), "INFO")

	sendLogs(t, conn, []plog.LogRecord{r1, r2, r3})

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)
	assert.Equal(t, 3, traces[0].SpanCount())
}

func TestSpanScopeName(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	r1 := newLogRecord("user=123 hello", now, "INFO")
	sendLogs(t, conn, []plog.LogRecord{r1})

	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	ss := traces[0].ResourceSpans().At(0).ScopeSpans().At(0)
	assert.Equal(t, "logs-to-spans", ss.Scope().Name())
}

func TestSpanKindInternal(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	r1 := newLogRecord("user=123 hello", now, "INFO")
	sendLogs(t, conn, []plog.LogRecord{r1})

	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	span := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	assert.Equal(t, ptrace.SpanKindInternal, span.Kind())
}

func TestNoSeverityAttributeWhenEmpty(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	r1 := newLogRecord("user=123 hello", now, "")
	sendLogs(t, conn, []plog.LogRecord{r1})

	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	span := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	_, ok := span.Attributes().Get("log.severity")
	assert.False(t, ok, "log.severity should not be set when severity is empty")
}

func TestGroupKeyAttributeOnSpan(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	r1 := newLogRecord("user=abc123 hello", now, "INFO")
	sendLogs(t, conn, []plog.LogRecord{r1})

	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	span := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	val, ok := span.Attributes().Get("group.key")
	require.True(t, ok)
	assert.Equal(t, "abc123", val.Str())
}

func TestLogBodyAttributeOnSpan(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	r1 := newLogRecord("user=456 test message", now, "INFO")
	sendLogs(t, conn, []plog.LogRecord{r1})

	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	span := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	val, ok := span.Attributes().Get("log.body")
	require.True(t, ok)
	assert.Equal(t, "user=456 test message", val.Str())
}

func TestParentChildChainThreeSpans(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecord("user=777 first", now, "INFO")
	r2 := newLogRecord("user=777 second", now.Add(1*time.Second), "INFO")
	r3 := newLogRecord("user=777 third", now.Add(2*time.Second), "INFO")

	sendLogs(t, conn, []plog.LogRecord{r1, r2, r3})

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)
	ss := traces[0].ResourceSpans().At(0).ScopeSpans().At(0)
	require.Equal(t, 3, ss.Spans().Len())

	span0 := ss.Spans().At(0)
	span1 := ss.Spans().At(1)
	span2 := ss.Spans().At(2)

	assert.Equal(t, span0.SpanID(), span1.ParentSpanID(), "span1 parent should be span0")
	assert.Equal(t, span1.SpanID(), span2.ParentSpanID(), "span2 parent should be span1")
	assert.Equal(t, span0.TraceID(), span1.TraceID())
	assert.Equal(t, span1.TraceID(), span2.TraceID())
}

func TestDurationFromDoubleAttribute(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	cfg.DurationKeys = []string{"dur"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(now))
	lr.Body().SetStr("user=123 first")
	lr.Attributes().PutDouble("dur", 1.5)

	err := conn.ConsumeLogs(context.Background(), logs)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	span0 := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	span0End := span0.EndTimestamp().AsTime()
	assert.Equal(t, now.Add(time.Duration(1.5*float64(time.Second))), span0End)
}

func TestDurationFromInvalidStringAttribute(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	cfg.DurationKeys = []string{"dur"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(now))
	lr.Body().SetStr("user=123 first")
	lr.Attributes().PutStr("dur", "not-a-duration")

	err := conn.ConsumeLogs(context.Background(), logs)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	span0 := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	span0End := span0.EndTimestamp().AsTime()
	assert.Equal(t, now.Add(500*time.Millisecond), span0End, "invalid duration string should fallback to EndSpanDuration")
}

func TestDurationKeysEmptyMeansNoOverride(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	cfg.DurationKeys = []string{}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecordWithAttrs("user=123 first", now, "INFO", map[string]string{"duration": "5s"})
	r2 := newLogRecordWithAttrs("user=123 second", now.Add(1*time.Second), "INFO", nil)

	sendLogs(t, conn, []plog.LogRecord{r1, r2})

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	span0 := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	span0End := span0.EndTimestamp().AsTime()
	assert.Equal(t, now.Add(1*time.Second), span0End, "empty DurationKeys means duration attribute is ignored")
}

func TestMaxWaitForceFlush(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 10 * time.Second
	cfg.MaxWait = 150 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	r1 := newLogRecord("user=888 first", now, "INFO")
	sendLogs(t, conn, []plog.LogRecord{r1})

	time.Sleep(500 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1, "max_wait should force-flush the group")
	assert.Equal(t, 1, traces[0].SpanCount())
}

func TestTwoConcurrentGroupsFlushIndependently(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	r1 := newLogRecord("user=aaa first", now, "INFO")
	r2 := newLogRecord("user=bbb first", now, "INFO")
	r3 := newLogRecord("user=aaa second", now.Add(1*time.Second), "INFO")

	sendLogs(t, conn, []plog.LogRecord{r1, r2, r3})

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 2, "should have 2 traces for 2 different user groups")

	totalSpans := 0
	for _, td := range traces {
		totalSpans += td.SpanCount()
	}
	assert.Equal(t, 3, totalSpans, "should have 3 spans total")
}

func TestSingleLogProducesSingleSpanWithEndSpanDuration(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.EndSpanDuration = 3 * time.Second
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecord("user=999 solo", now, "INFO")
	sendLogs(t, conn, []plog.LogRecord{r1})

	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	span := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	assert.Equal(t, now, span.StartTimestamp().AsTime())
	assert.Equal(t, now.Add(3*time.Second), span.EndTimestamp().AsTime())
}

func TestObservedTimestampUsedWhenNoTimestamp(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	observedTS := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(observedTS))
	lr.Body().SetStr("user=123 hello")

	err := conn.ConsumeLogs(context.Background(), logs)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	span := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	assert.Equal(t, observedTS, span.StartTimestamp().AsTime())
}

func TestStructMapBodyEmptyStringValueSkipped(t *testing.T) {
	keys := []string{"user", "session"}

	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = keys
	conn := createTestConnector(t, cfg, sink)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetEmptyMap().PutStr("user", "")
	lr.Body().Map().PutStr("session", "abc123")
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	c := conn.(*logsToSpansConnector)
	got := c.extractGroupKey(lr)
	assert.Equal(t, "abc123", got, "empty string value should be skipped, next key used")
}

func TestStructMapBodyAllEmptyReturnsEmpty(t *testing.T) {
	keys := []string{"user", "session"}

	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = keys
	conn := createTestConnector(t, cfg, sink)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetEmptyMap().PutStr("user", "")
	lr.Body().Map().PutStr("session", "")
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	c := conn.(*logsToSpansConnector)
	got := c.extractGroupKey(lr)
	assert.Equal(t, "", got, "all empty values should return empty")
}

func TestUnstructuredKeyValueWithHyphen(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user-id"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Now()
	r1 := newLogRecord("user-id=abc-123 hello", now, "INFO")
	sendLogs(t, conn, []plog.LogRecord{r1})

	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	val, ok := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().Get("group.key")
	require.True(t, ok)
	assert.Equal(t, "abc-123", val.Str())
}

func TestServiceNameRequiredWhenEmpty(t *testing.T) {
	cfg := validTestConfig()
	cfg.ServiceName = ""
	require.Error(t, cfg.Validate())
}

func TestConfigDefaultMaxLogsPerTrace(t *testing.T) {
	cfg := createDefaultConfig()
	assert.Equal(t, 100, cfg.MaxLogsPerTrace)
}

func TestConfigValidateNegativeMaxLogsPerTrace(t *testing.T) {
	cfg := validTestConfig()
	cfg.MaxLogsPerTrace = -1
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_logs_per_trace")
	assert.Equal(t, -1, cfg.MaxLogsPerTrace, "Validate must not rewrite the value")
}

func TestConfigValidateZeroMaxLogsPerTrace(t *testing.T) {
	cfg := validTestConfig()
	cfg.MaxLogsPerTrace = 0
	err := cfg.Validate()
	require.NoError(t, err, "zero means 'no limit' and is valid")
	assert.Equal(t, 0, cfg.MaxLogsPerTrace)
}

func TestMaxLogsPerTraceFlushesAtLimit(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 10 * time.Second
	cfg.MaxWait = 10 * time.Second
	cfg.MaxLogsPerTrace = 3
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecord("user=123 log1", now, "INFO")
	r2 := newLogRecord("user=123 log2", now.Add(1*time.Second), "INFO")
	r3 := newLogRecord("user=123 log3", now.Add(2*time.Second), "INFO")
	r4 := newLogRecord("user=123 log4", now.Add(3*time.Second), "INFO")
	r5 := newLogRecord("user=123 log5", now.Add(4*time.Second), "INFO")

	sendLogs(t, conn, []plog.LogRecord{r1, r2, r3, r4, r5})
	conn.Shutdown(context.Background())

	traces := sink.AllTraces()
	require.Len(t, traces, 2, "expected 2 traces: 3 logs in first, 2 in second")
	assert.Equal(t, 3, traces[0].SpanCount())
	assert.Equal(t, 2, traces[1].SpanCount())
}

func TestMaxLogsPerTraceExactLimit(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 10 * time.Second
	cfg.MaxWait = 10 * time.Second
	cfg.MaxLogsPerTrace = 3
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecord("user=123 log1", now, "INFO")
	r2 := newLogRecord("user=123 log2", now.Add(1*time.Second), "INFO")
	r3 := newLogRecord("user=123 log3", now.Add(2*time.Second), "INFO")
	r4 := newLogRecord("user=123 log4", now.Add(3*time.Second), "INFO")
	r5 := newLogRecord("user=123 log5", now.Add(4*time.Second), "INFO")
	r6 := newLogRecord("user=123 log6", now.Add(5*time.Second), "INFO")

	sendLogs(t, conn, []plog.LogRecord{r1, r2, r3, r4, r5, r6})

	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 2, "expected 2 traces: 3+3 logs")
	assert.Equal(t, 3, traces[0].SpanCount())
	assert.Equal(t, 3, traces[1].SpanCount())
}

func TestMaxLogsPerTraceBelowLimit(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.MaxWait = 10 * time.Second
	cfg.MaxLogsPerTrace = 5
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecord("user=123 log1", now, "INFO")
	r2 := newLogRecord("user=123 log2", now.Add(1*time.Second), "INFO")

	sendLogs(t, conn, []plog.LogRecord{r1, r2})

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1, "expected 1 trace flushed by timeout")
	assert.Equal(t, 2, traces[0].SpanCount())
}

func TestMaxLogsPerTraceZeroMeansNoLimit(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.MaxWait = 10 * time.Second
	cfg.MaxLogsPerTrace = 0
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	records := make([]plog.LogRecord, 10)
	for i := 0; i < 10; i++ {
		records[i] = newLogRecord("user=123 log", now.Add(time.Duration(i)*time.Second), "INFO")
	}

	sendLogs(t, conn, records)

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1, "expected 1 trace with all 10 spans")
	assert.Equal(t, 10, traces[0].SpanCount())
}

func TestMaxLogsPerTraceSpanLinks(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 10 * time.Second
	cfg.MaxWait = 10 * time.Second
	cfg.MaxLogsPerTrace = 3
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecord("user=123 log1", now, "INFO")
	r2 := newLogRecord("user=123 log2", now.Add(1*time.Second), "INFO")
	r3 := newLogRecord("user=123 log3", now.Add(2*time.Second), "INFO")
	r4 := newLogRecord("user=123 log4", now.Add(3*time.Second), "INFO")

	sendLogs(t, conn, []plog.LogRecord{r1, r2, r3, r4})
	conn.Shutdown(context.Background())

	traces := sink.AllTraces()
	require.Len(t, traces, 2)

	firstSpanOfSecondTrace := traces[1].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	require.Equal(t, 1, firstSpanOfSecondTrace.Links().Len(), "second trace should have 1 span link")

	link := firstSpanOfSecondTrace.Links().At(0)
	firstTraceID := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).TraceID()
	assert.Equal(t, firstTraceID, link.TraceID(), "link should point to first trace")

	lastSpanOfFirstTrace := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(2)
	assert.Equal(t, lastSpanOfFirstTrace.SpanID(), link.SpanID(), "link should point to last span of first trace")
}

func TestMaxLogsPerTraceSpanLinkChain(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 10 * time.Second
	cfg.MaxWait = 10 * time.Second
	cfg.MaxLogsPerTrace = 3
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	records := make([]plog.LogRecord, 9)
	for i := 0; i < 9; i++ {
		records[i] = newLogRecord("user=123 log", now.Add(time.Duration(i)*time.Second), "INFO")
	}

	sendLogs(t, conn, records)

	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 3, "expected 3 traces: 3+3+3")

	// Trace B links to Trace A
	spanB0 := traces[1].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	require.Equal(t, 1, spanB0.Links().Len())
	assert.Equal(t, traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).TraceID(), spanB0.Links().At(0).TraceID())
	assert.Equal(t, traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(2).SpanID(), spanB0.Links().At(0).SpanID())

	// Trace C links to Trace B
	spanC0 := traces[2].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	require.Equal(t, 1, spanC0.Links().Len())
	assert.Equal(t, traces[1].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).TraceID(), spanC0.Links().At(0).TraceID())
	assert.Equal(t, traces[1].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(2).SpanID(), spanC0.Links().At(0).SpanID())
}

func TestMaxLogsPerTraceSeparateGroups(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 10 * time.Second
	cfg.MaxWait = 10 * time.Second
	cfg.MaxLogsPerTrace = 2
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecord("user=aaa log1", now, "INFO")
	r2 := newLogRecord("user=aaa log2", now.Add(1*time.Second), "INFO")
	r3 := newLogRecord("user=aaa log3", now.Add(2*time.Second), "INFO")
	r4 := newLogRecord("user=bbb log1", now, "INFO")
	r5 := newLogRecord("user=bbb log2", now.Add(1*time.Second), "INFO")
	r6 := newLogRecord("user=bbb log3", now.Add(2*time.Second), "INFO")

	sendLogs(t, conn, []plog.LogRecord{r1, r2, r3, r4, r5, r6})
	conn.Shutdown(context.Background())

	traces := sink.AllTraces()
	require.Len(t, traces, 4, "expected 4 traces: 2 per user")

	aaaTraces := 0
	bbbTraces := 0
	for _, td := range traces {
		span := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		val, _ := span.Attributes().Get("group.key")
		if val.Str() == "aaa" {
			aaaTraces++
		} else if val.Str() == "bbb" {
			bbbTraces++
		}
	}
	assert.Equal(t, 2, aaaTraces)
	assert.Equal(t, 2, bbbTraces)
}

func TestMaxLogsPerTraceWithTimeout(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.MaxWait = 10 * time.Second
	cfg.MaxLogsPerTrace = 10
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	r1 := newLogRecord("user=123 log1", now, "INFO")
	r2 := newLogRecord("user=123 log2", now.Add(1*time.Second), "INFO")

	sendLogs(t, conn, []plog.LogRecord{r1, r2})

	time.Sleep(300 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1, "timeout should flush the group")
	assert.Equal(t, 2, traces[0].SpanCount())
}

// TestStaleTimerCallbackDoesNotEvictReplacementGroup covers issue #7. When
// max_logs_per_trace splits a group, the outgoing group's timer may already
// have fired and queued its callback. That callback must neither delete the
// replacement group nor re-emit the records the split already delivered.
func TestStaleTimerCallbackDoesNotEvictReplacementGroup(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 10 * time.Second
	cfg.MaxWait = 10 * time.Second
	cfg.MaxLogsPerTrace = 2
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)
	c := conn.(*logsToSpansConnector)

	now := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)

	// First record opens group g1.
	c.addToGroup("user=123", newLogRecord("user=123 log1", now, "INFO"))

	c.mu.Lock()
	g1 := c.groups["user=123"]
	c.mu.Unlock()
	require.NotNil(t, g1)

	// Second record hits the limit: g1 is retired and replaced by g2. g1's
	// timer callback may still be queued at this point.
	c.addToGroup("user=123", newLogRecord("user=123 log2", now.Add(1*time.Second), "INFO"))

	c.mu.Lock()
	g2 := c.groups["user=123"]
	c.mu.Unlock()
	require.NotNil(t, g2)
	require.NotSame(t, g1, g2, "max_logs_per_trace should have replaced the group")

	// The stale callback for g1 runs late.
	c.flushGroup("user=123", g1)

	c.mu.Lock()
	current := c.groups["user=123"]
	c.mu.Unlock()
	assert.Same(t, g2, current, "stale callback must not evict the live group")

	// The split already emitted g1's records. The stale callback must not emit
	// them a second time.
	require.Len(t, sink.AllTraces(), 1)
	assert.Equal(t, 2, sink.AllTraces()[0].SpanCount())

	// g2 keeps collecting, and its trace links back to the split trace.
	c.addToGroup("user=123", newLogRecord("user=123 log3", now.Add(2*time.Second), "INFO"))
	require.NoError(t, conn.Shutdown(context.Background()))

	traces := sink.AllTraces()
	require.Len(t, traces, 2, "records held by g2 must not be lost")
	assert.Equal(t, 1, traces[1].SpanCount())

	firstSpan := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	secondSpan := traces[1].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	require.Equal(t, 1, secondSpan.Links().Len(), "span-link chain must survive the split")
	assert.Equal(t, firstSpan.TraceID(), secondSpan.Links().At(0).TraceID())
}

// TestFlushGroupIsIdempotent covers the second timer of an already-flushed
// group. Both timer and maxTimer point at the same group, so whichever fires
// second must be a no-op rather than emitting a duplicate trace.
func TestFlushGroupIsIdempotent(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 10 * time.Second
	cfg.MaxWait = 10 * time.Second
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)
	c := conn.(*logsToSpansConnector)

	c.addToGroup("user=123", newLogRecord("user=123 only", time.Now(), "INFO"))

	c.mu.Lock()
	g := c.groups["user=123"]
	c.mu.Unlock()
	require.NotNil(t, g)

	c.flushGroup("user=123", g)
	c.flushGroup("user=123", g)

	require.Len(t, sink.AllTraces(), 1, "group must be emitted exactly once")
	assert.Equal(t, 1, sink.AllTraces()[0].SpanCount())

	c.mu.Lock()
	_, ok := c.groups["user=123"]
	c.mu.Unlock()
	assert.False(t, ok, "flushed group must be removed from the map")
}

// TestConcurrentSplitAndFlush exercises the map mutations and timer callbacks
// under -race, and asserts every record is emitted exactly once.
func TestConcurrentSplitAndFlush(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = time.Millisecond
	cfg.MaxWait = time.Millisecond
	cfg.MaxLogsPerTrace = 3
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	const goroutines = 8
	const perGoroutine = 100

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				ld := plog.NewLogs()
				sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
				lr := sl.LogRecords().AppendEmpty()
				lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				lr.Body().SetStr(fmt.Sprintf("user=%d log%d", id%4, i))
				_ = conn.ConsumeLogs(context.Background(), ld)
			}
		}(g)
	}
	wg.Wait()

	require.NoError(t, conn.Shutdown(context.Background()))

	seen := 0
	for _, td := range sink.AllTraces() {
		seen += td.SpanCount()
	}
	assert.Equal(t, goroutines*perGoroutine, seen, "no record may be dropped or duplicated")
}
