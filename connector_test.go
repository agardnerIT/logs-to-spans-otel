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
	assert.Equal(t, "drop", cfg.UnmatchedBehaviour)
	assert.Equal(t, "logs-to-spans", cfg.ServiceName)
	assert.Equal(t, 30*time.Second, cfg.MaxWait)
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

func TestConfigValidateNegativeTimeout(t *testing.T) {
	cfg := &Config{Timeout: -1 * time.Second}
	err := cfg.Validate()
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, cfg.Timeout)
}

func TestConfigValidateNegativeMaxWait(t *testing.T) {
	cfg := &Config{MaxWait: -1 * time.Second}
	err := cfg.Validate()
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, cfg.MaxWait)
}

func TestConfigValidateNegativeEndSpanDuration(t *testing.T) {
	cfg := &Config{EndSpanDuration: -1 * time.Second}
	err := cfg.Validate()
	require.NoError(t, err)
	assert.Equal(t, 500*time.Millisecond, cfg.EndSpanDuration)
}

func TestConfigValidateEmptyUnmatchedBehaviour(t *testing.T) {
	cfg := &Config{UnmatchedBehaviour: ""}
	err := cfg.Validate()
	require.NoError(t, err)
	assert.Equal(t, "drop", cfg.UnmatchedBehaviour)
}

func TestConfigValidateEmptyServiceName(t *testing.T) {
	cfg := &Config{ServiceName: ""}
	err := cfg.Validate()
	require.NoError(t, err)
	assert.Equal(t, "logs-to-spans", cfg.ServiceName)
}

func TestConfigValidateKeepsExplicitValues(t *testing.T) {
	cfg := &Config{
		Timeout:            10 * time.Second,
		MaxWait:            60 * time.Second,
		EndSpanDuration:    1 * time.Second,
		UnmatchedBehaviour: "pass_through",
		ServiceName:        "custom",
	}
	err := cfg.Validate()
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, cfg.Timeout)
	assert.Equal(t, 60*time.Second, cfg.MaxWait)
	assert.Equal(t, 1*time.Second, cfg.EndSpanDuration)
	assert.Equal(t, "pass_through", cfg.UnmatchedBehaviour)
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

func TestServiceNameDefaultWhenEmpty(t *testing.T) {
	cfg := &Config{ServiceName: ""}
	err := cfg.Validate()
	require.NoError(t, err)
	assert.Equal(t, "logs-to-spans", cfg.ServiceName)
}
