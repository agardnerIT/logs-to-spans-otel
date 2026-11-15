// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs_to_spans

import (
	"encoding/hex"
	"math"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// TestRecordWithoutTimestampsGetsCurrentTime covers issue #25: a record with
// neither a timestamp nor an observed timestamp must not carry the Unix epoch
// (or a wrapped year-1754 value) into the span.
func TestRecordWithoutTimestampsGetsCurrentTime(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.Timeout = 20 * time.Millisecond
	conn := createTestConnector(t, cfg, sink)

	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr("user=123 no timestamps")
	// Deliberately leave both Timestamp and ObservedTimestamp unset.
	require.NoError(t, conn.ConsumeLogs(t.Context(), ld))

	time.Sleep(100 * time.Millisecond)

	traces := flatTraces(sink)
	require.Len(t, traces, 1)
	span := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)

	start := span.StartTimestamp().AsTime()
	assert.WithinDuration(t, time.Now(), start, time.Minute,
		"a record with no timestamps must be stamped with the time the connector saw it")
	assert.Greater(t, start.Year(), 2000, "a missing timestamp must not produce a 1754 or 1970 span")
	assert.Less(t, span.EndTimestamp().AsTime(), time.Now().Add(time.Minute))
}

// TestRecordWithWrappedTimestampGetsCurrentTime reproduces the exact overflow in
// issue #25: pcommon.NewTimestampFromTime(time.Time{}) stores the negative Unix
// nanosecond value of year 1 as a uint64, which reads back as year 1754. A
// record carrying that value must not emit a 1754 span.
func TestRecordWithWrappedTimestampGetsCurrentTime(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.Timeout = 20 * time.Millisecond
	conn := createTestConnector(t, cfg, sink)

	rec := newLogRecord("user=123 wrapped", time.Time{}, "INFO")
	require.Greater(t, uint64(rec.ObservedTimestamp()), uint64(maxSaneTimestamp),
		"the reproduction depends on a timestamp above MaxInt64")
	sendLogs(t, conn, []plog.LogRecord{rec})

	time.Sleep(100 * time.Millisecond)

	traces := flatTraces(sink)
	require.Len(t, traces, 1)
	start := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).StartTimestamp().AsTime()
	assert.WithinDuration(t, time.Now(), start, time.Minute)
}

// TestRecordsWithoutTimestampCounter makes the timestamp substitution visible
// instead of silent.
func TestRecordsWithoutTimestampCounter(t *testing.T) {
	sink := newTestSink()
	conn, reader := createMetricsTestConnector(t, metricsTestConfig(), sink)

	sendLogs(t, conn, []plog.LogRecord{
		newLogRecord("user=123 no timestamp", time.Time{}, "INFO"),
	})

	time.Sleep(150 * time.Millisecond)
	counters := collectInt64Counters(t, reader)
	assert.Equal(t, int64(1), counters[metricRecordsWithoutTime])
}

// TestParseDurationRejectsOverflow covers issue #44. Every case here either
// overflows time.Duration (wrapping to a small positive value) or is not a
// usable duration at all.
func TestParseDurationRejectsOverflow(t *testing.T) {
	tests := []struct {
		name string
		val  pcommon.Value
		want time.Duration
		ok   bool
	}{
		{name: "int seconds", val: pcommon.NewValueInt(30), want: 30 * time.Second, ok: true},
		{name: "int max int64", val: pcommon.NewValueInt(math.MaxInt64)},
		{name: "int just above max seconds", val: pcommon.NewValueInt(maxDurationSeconds + 1)},
		{name: "int at max seconds", val: pcommon.NewValueInt(maxDurationSeconds), want: time.Duration(maxDurationSeconds) * time.Second, ok: true},
		{name: "int zero", val: pcommon.NewValueInt(0)},
		{name: "int negative", val: pcommon.NewValueInt(-5)},
		{name: "double seconds", val: pcommon.NewValueDouble(1.5), want: 1500 * time.Millisecond, ok: true},
		{name: "double NaN", val: pcommon.NewValueDouble(math.NaN())},
		{name: "double +Inf", val: pcommon.NewValueDouble(math.Inf(1))},
		{name: "double -Inf", val: pcommon.NewValueDouble(math.Inf(-1))},
		{name: "double overflow", val: pcommon.NewValueDouble(1e18)},
		{name: "double negative", val: pcommon.NewValueDouble(-1)},
		{name: "string duration", val: pcommon.NewValueStr("250ms"), want: 250 * time.Millisecond, ok: true},
		{name: "string invalid", val: pcommon.NewValueStr("soon")},
		{name: "string negative", val: pcommon.NewValueStr("-1s")},
		{name: "bool", val: pcommon.NewValueBool(true)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseDuration(tt.val)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestDurationAboveMaxSpanDurationIgnored covers the second half of #44: even a
// representable duration is rejected when it exceeds max_span_duration, and the
// rejection is counted.
func TestDurationAboveMaxSpanDurationIgnored(t *testing.T) {
	sink := newTestSink()
	cfg := metricsTestConfig()
	cfg.Timeout = 20 * time.Millisecond
	cfg.DurationKeys = []string{"duration"}
	cfg.MaxSpanDuration = time.Hour
	conn, reader := createMetricsTestConnector(t, cfg, sink)

	sendLogs(t, conn, []plog.LogRecord{
		newLogRecordWithAttrs("user=123 huge duration", time.Now(), "INFO", map[string]string{"duration": "25h"}),
	})

	time.Sleep(150 * time.Millisecond)

	traces := flatTraces(sink)
	require.Len(t, traces, 1)
	span := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	assert.Equal(t, cfg.EndSpanDuration, span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()),
		"an out-of-range duration must fall back to end_span_duration, not to a wrapped value")

	counters := collectInt64Counters(t, reader)
	assert.Equal(t, int64(1), counters[metricInvalidDurations])
}

// TestMaxRecordBytesTruncatesBody covers issue #22: a single oversized body is
// cut at max_record_bytes, rune-safe, and the loss is counted.
func TestMaxRecordBytesTruncatesBody(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.Timeout = 20 * time.Millisecond
	cfg.MaxRecordBytes = 16
	cfg.IncludeBodyAttribute = true
	conn := createTestConnector(t, cfg, sink)

	body := "user=123 " + strings.Repeat("é", 64)
	sendLogs(t, conn, []plog.LogRecord{newLogRecord(body, time.Now(), "INFO")})

	time.Sleep(100 * time.Millisecond)

	traces := flatTraces(sink)
	require.Len(t, traces, 1)
	val, ok := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().Get("log.body")
	require.True(t, ok)
	assert.LessOrEqual(t, len(val.Str()), 16)
	assert.True(t, strings.HasPrefix(body, val.Str()))
	assert.Contains(t, val.Str(), "user=123", "truncation keeps the beginning of the body")
}

// TestMaxGroupBytesSplitsGroup covers the group-level bound from #22: a group
// whose retained bytes exceed max_group_bytes is flushed and replaced, exactly
// like max_logs_per_trace. Records are never dropped.
func TestMaxGroupBytesSplitsGroup(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.Timeout = 10 * time.Second
	cfg.MaxWait = 10 * time.Second
	cfg.MaxLogsPerTrace = 0
	cfg.MaxRecordBytes = 1024
	cfg.MaxGroupBytes = 300
	conn := createTestConnector(t, cfg, sink)

	body := "user=123 " + strings.Repeat("x", 100)
	var records []plog.LogRecord
	for range 6 {
		records = append(records, newLogRecord(body, time.Now(), "INFO"))
	}
	sendLogs(t, conn, records)
	require.NoError(t, conn.Shutdown(t.Context()))

	traces := flatTraces(sink)
	require.Greater(t, len(traces), 1, "6 x ~230 byte records cannot fit in one 300-byte group")

	total := 0
	for _, td := range traces {
		total += td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().Len()
	}
	assert.Equal(t, 6, total, "the byte bound moves the trace boundary, it never drops records")
}

// TestValueToStringLimit pins the bounded serialization from issue #32: a
// Map body keeps only what the template can use, and the result is rune-safe.
func TestValueToStringLimit(t *testing.T) {
	v := pcommon.NewValueMap()
	m := v.Map()
	m.PutStr("a", "first")
	m.PutStr("b", strings.Repeat("é", 1000))
	m.PutInt("c", 3)

	full := valueToString(v)
	limited := valueToStringLimit(v, 12)
	assert.Less(t, len(limited), len(full))
	assert.LessOrEqual(t, len([]rune(limited)), 12)
	assert.True(t, strings.HasPrefix(full, limited),
		"the bounded value must be a prefix of the full serialization")

	assert.Equal(t, "abcdefgh", valueToStringLimit(pcommon.NewValueStr("abcdefghij"), 8))
	assert.Equal(t, "abc", valueToStringLimit(pcommon.NewValueStr("abc"), 8))
}

// TestExtractOptionsFor pins what extraction must produce for each template
// shape, which is what makes skipping the body serialization safe.
func TestExtractOptionsFor(t *testing.T) {
	tests := []struct {
		name            string
		template        string
		includeBodyAttr bool
		wantBodyLimit   int
	}{
		{name: "default truncated", template: defaultSpanNameTemplate, wantBodyLimit: 120},
		{name: "full body", template: "{body}", wantBodyLimit: fullBody},
		{name: "severity can render empty so body is the fallback", template: "{severity}", wantBodyLimit: fullBody},
		{name: "literal never empty", template: "static name", wantBodyLimit: 0},
		{name: "severity plus truncated", template: "{severity}: {truncated:40:body}", wantBodyLimit: 40},
		{name: "body attribute forces full body", template: "static", includeBodyAttr: true, wantBodyLimit: fullBody},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := createDefaultConfig()
			cfg.IncludeBodyAttribute = tt.includeBodyAttr
			tmpl, err := compileSpanNameTemplate(tt.template)
			require.NoError(t, err)
			assert.Equal(t, tt.wantBodyLimit, extractOptionsFor(cfg, tmpl).bodyLimit)
		})
	}
}

// TestIDGeneratorBulkIDs covers #31/#39: IDs come from one bulk draw, are never
// zero, and never repeat within a batch.
func TestIDGeneratorBulkIDs(t *testing.T) {
	gen, err := newIDGenerator()
	require.NoError(t, err)

	ids := gen.newBulk(4, 8)
	seenTrace := map[pcommon.TraceID]bool{}
	seenSpan := map[pcommon.SpanID]bool{}
	for range 4 {
		id := ids.traceID()
		assert.False(t, id.IsEmpty(), "a zero trace ID is rejected by every exporter")
		assert.False(t, seenTrace[id], "duplicate trace ID")
		seenTrace[id] = true
	}
	for range 8 {
		id := ids.spanID()
		assert.False(t, id.IsEmpty())
		assert.False(t, seenSpan[id], "duplicate span ID")
		seenSpan[id] = true
	}
}

// TestIncludeBodyAttributeOptIn covers #27: log.body is opt-in, and the default
// span name is bounded.
func TestIncludeBodyAttributeOptIn(t *testing.T) {
	cfg := createDefaultConfig()
	require.False(t, cfg.IncludeBodyAttribute, "log.body duplicates the span name and is off by default")

	sink := newTestSink()
	cfg.GroupByKeys = []string{"user"}
	cfg.Timeout = 20 * time.Millisecond
	conn := createTestConnector(t, cfg, sink)

	longBody := "user=123 " + strings.Repeat("y", 500)
	sendLogs(t, conn, []plog.LogRecord{newLogRecord(longBody, time.Now(), "INFO")})
	time.Sleep(100 * time.Millisecond)

	traces := flatTraces(sink)
	require.Len(t, traces, 1)
	span := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)

	_, ok := span.Attributes().Get("log.body")
	assert.False(t, ok, "log.body is not attached unless include_body_attribute is true")

	assert.Len(t, []rune(span.Name()), 120, "the default span name is capped at 120 runes")
	assert.True(t, strings.HasPrefix(longBody, span.Name()))
}

// TestMaxSpanNameLengthCap covers the defense-in-depth cap from #27: even an
// explicit {body} template cannot produce an unbounded span name.
func TestMaxSpanNameLengthCap(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.SpanNameTemplate = "{body}"
	cfg.MaxSpanNameLength = 10
	cfg.Timeout = 20 * time.Millisecond
	conn := createTestConnector(t, cfg, sink)

	sendLogs(t, conn, []plog.LogRecord{newLogRecord("user=123 an extremely long body", time.Now(), "INFO")})
	time.Sleep(100 * time.Millisecond)

	traces := flatTraces(sink)
	require.Len(t, traces, 1)
	assert.Equal(t, "user=123 a", traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
}

// TestRequireSpanIDSuppressesPartialLink covers #42: a trace ID attribute
// without a span ID is attacker-supplied data, and require_span_id lets an
// operator refuse to emit a half-link.
func TestRequireSpanIDSuppressesPartialLink(t *testing.T) {
	traceID := strings.Repeat("ab", 16)
	attrs := map[string]string{"trace_id": traceID}

	emit := func(t *testing.T, requireSpanID bool) ptrace.Span {
		t.Helper()
		sink := newTestSink()
		cfg := createDefaultConfig()
		cfg.GroupByKeys = []string{"user"}
		cfg.Timeout = 20 * time.Millisecond
		cfg.RequireSpanID = requireSpanID
		conn := createTestConnector(t, cfg, sink)

		sendLogs(t, conn, []plog.LogRecord{
			newLogRecordWithAttrs("user=123 with trace id only", time.Now(), "INFO", attrs),
		})
		time.Sleep(100 * time.Millisecond)

		traces := flatTraces(sink)
		require.Len(t, traces, 1)
		return traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	}

	partial := emit(t, false)
	require.Equal(t, 1, partial.Links().Len(), "by default a trace-ID-only link is emitted")
	linkTraceID := partial.Links().At(0).TraceID()
	assert.Equal(t, traceID, hex.EncodeToString(linkTraceID[:]))
	assert.True(t, partial.Links().At(0).SpanID().IsEmpty())

	strict := emit(t, true)
	assert.Zero(t, strict.Links().Len(), "require_span_id refuses the partial link")
}

// TestCopyResourceAttributesDisabled covers the resilience half of #23: when
// copying is off the group holds no resource copy at all.
func TestNoResourceCopyWhenDisabled(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.CopyResourceAttributes = false
	cfg.Timeout = time.Hour
	conn := createTestConnector(t, cfg, sink)

	sendLogsWithResource(t, conn, map[string]string{"host.name": "pod-1"},
		[]plog.LogRecord{newLogRecord("user=123 with resource", time.Now(), "INFO")})

	raw := conn.(*logsToSpansConnector)
	raw.mu.Lock()
	var group *logGroup
	for _, g := range raw.groups {
		group = g
	}
	raw.mu.Unlock()
	require.NotNil(t, group)
	assert.Zero(t, group.resource.Len(), "no resource copy is taken when copying is disabled")

	require.NoError(t, conn.Shutdown(t.Context()))
	traces := flatTraces(sink)
	require.Len(t, traces, 1)
	_, ok := traces[0].ResourceSpans().At(0).Resource().Attributes().Get("host.name")
	assert.False(t, ok)
}

// TestBatchSetKeepsOrderAndGroups covers #33's grouping structure: it is a
// slice for the common few-key case and switches to a map for many keys.
func TestBatchSetKeepsOrderAndGroups(t *testing.T) {
	var set batchSet
	for i := range linearBatchLimit * 3 {
		key := "k" + strings.Repeat("x", i%5)
		set.add(key, &logRecord{body: key})
	}
	require.Len(t, set.batches, 5, "one batch per distinct key")
	for _, batch := range set.batches {
		require.NotEmpty(t, batch.records)
		for _, rec := range batch.records {
			assert.Equal(t, batch.key, rec.body)
		}
	}
}

// TestExtractLogRecordAllocationBudget is the regression guard from #38: a
// deliberate loosening (current cost is well under this) is fine, but a change
// that doubles per-record allocation fails the build.
func TestExtractLogRecordAllocationBudget(t *testing.T) {
	ld := newStructuredBenchmarkLogs(1)
	lr := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	cfg := createDefaultConfig()
	opts := extractOptionsFor(cfg, mustCompileTemplate(t, cfg.SpanNameTemplate))

	allocs := testing.AllocsPerRun(50, func() {
		_ = extractLogRecord(lr, cfg, opts)
	})
	assert.LessOrEqual(t, allocs, float64(45), "extractLogRecord allocates %.0f times, budget is 45", allocs)
}

// --- Bounds fixed by #52, #53, #54 and #55 -------------------------------

// TestBoundGroupKey pins the max_group_key_bytes cap: byte-counted, rune-safe,
// disabled by 0.
func TestBoundGroupKey(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.MaxGroupKeyBytes = 8
	c := &logsToSpansConnector{config: cfg}

	assert.Equal(t, "12345678", c.boundGroupKey("1234567890"))
	assert.Equal(t, "12345678", c.boundGroupKey("12345678"))
	assert.Empty(t, c.boundGroupKey(""))
	// Rune-safe: the cut never splits a multi-byte character.
	assert.Equal(t, "éééé", c.boundGroupKey(strings.Repeat("é", 10)))

	// 0 disables the bound.
	cfg0 := createDefaultConfig()
	cfg0.MaxGroupKeyBytes = 0
	c0 := &logsToSpansConnector{config: cfg0}
	assert.Equal(t, strings.Repeat("é", 10), c0.boundGroupKey(strings.Repeat("é", 10)))
}

// TestMaxGroupKeyBytesDefault pins the shipped default.
func TestMaxGroupKeyBytesDefault(t *testing.T) {
	assert.Equal(t, 1024, createDefaultConfig().MaxGroupKeyBytes)
}

func TestMaxGroupKeyBytesValidate(t *testing.T) {
	cfg := validTestConfig()
	cfg.MaxGroupKeyBytes = -1
	require.ErrorContains(t, cfg.Validate(), "max_group_key_bytes")

	cfg = validTestConfig()
	cfg.MaxGroupKeyBytes = maxGroupKeyBytesUpperBound + 1
	require.ErrorContains(t, cfg.Validate(), "max_group_key_bytes")
}

// TestBuildMapKeyCapsResourceComponents covers the scoping half of #52: the
// group_by_resource_attributes values also end up in the internal map key, so
// they are capped the same way. Resources that differ only past the cap share
// a map key.
func TestBuildMapKeyCapsResourceComponents(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.MaxGroupKeyBytes = 8
	cfg.GroupByResourceAttributes = []string{"tenant"}
	c := &logsToSpansConnector{config: cfg}

	res := pcommon.NewMap()
	res.PutStr("tenant", strings.Repeat("x", 100))
	key := c.buildMapKey("user", res)

	res2 := pcommon.NewMap()
	res2.PutStr("tenant", strings.Repeat("x", 100)+"tail")
	assert.Equal(t, key, c.buildMapKey("user", res2),
		"resources that differ only past the cap must share a map key")

	assert.Contains(t, key, "8:xxxxxxxx", "the component holds the capped value, length-prefixed")
}

// TestExtractGroupKeyBoundsHugeValues covers the extraction half of #52: a
// huge attribute value or body value is bounded during conversion, not only
// when it reaches addRecords.
func TestExtractGroupKeyBoundsHugeValues(t *testing.T) {
	sink := newTestSink()

	cfg := createDefaultConfig()
	cfg.GroupByAttributes = []string{"user.id"}
	cfg.MaxGroupKeyBytes = 8
	conn := createTestConnector(t, cfg, sink)
	c := conn.(*logsToSpansConnector)

	lr := newLogRecordWithAttrs("body", time.Now(), "INFO",
		map[string]string{"user.id": strings.Repeat("u", 100)})
	assert.Equal(t, strings.Repeat("u", 8), c.extractGroupKey(lr))

	// Structured body, string value.
	cfg2 := createDefaultConfig()
	cfg2.GroupByKeys = []string{"user_id"}
	cfg2.MaxGroupKeyBytes = 8
	conn2 := createTestConnector(t, cfg2, sink)
	c2 := conn2.(*logsToSpansConnector)

	body := plog.NewLogs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	body.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	body.Body().SetEmptyMap().PutStr("user_id", strings.Repeat("v", 100))
	assert.Equal(t, strings.Repeat("v", 8), c2.extractGroupKey(body))

	// Structured body, nested Map value: the valueToStringLimit rune budget
	// bounds the serialization (never the full JSON), and the result is at
	// most the cap in runes.
	nested := plog.NewLogs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	nested.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	outer := nested.Body().SetEmptyMap().PutEmptyMap("user_id")
	outer.PutStr("deep", strings.Repeat("w", 1000))
	got := c2.extractGroupKey(nested)
	assert.LessOrEqual(t, len([]rune(got)), 8)
}

// TestMaxGroupKeyBytesTruncatesEmittedKeyAndMergesCollidingKeys is the
// end-to-end half of #52: the group.key attribute on every emitted span is the
// capped key, and two records whose keys differ only past the cap land in one
// group (one trace), while a key under the cap stays separate.
func TestMaxGroupKeyBytesTruncatesEmittedKeyAndMergesCollidingKeys(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByAttributes = []string{"user.id"}
	cfg.Timeout = 20 * time.Millisecond
	cfg.MaxGroupKeyBytes = 8
	conn := createTestConnector(t, cfg, sink)

	huge := strings.Repeat("é", 10) // 20 bytes; the cap keeps 4 runes
	sendLogs(t, conn, []plog.LogRecord{
		newLogRecordWithAttrs("one", time.Now(), "INFO", map[string]string{"user.id": huge}),
		newLogRecordWithAttrs("two", time.Now(), "INFO", map[string]string{"user.id": huge + "tail"}),
		newLogRecordWithAttrs("three", time.Now(), "INFO", map[string]string{"user.id": "short"}),
	})

	time.Sleep(100 * time.Millisecond)

	traces := flatTraces(sink)
	require.Len(t, traces, 2, "keys that differ only past the cap collapse into one group")

	mergedIdx, singleIdx := -1, -1
	for i, td := range traces {
		if td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().Len() == 2 {
			mergedIdx = i
		} else {
			singleIdx = i
		}
	}
	require.NotEqual(t, -1, mergedIdx, "the two records with huge keys share one trace")
	require.NotEqual(t, -1, singleIdx)

	merged := traces[mergedIdx].ResourceSpans().At(0).ScopeSpans().At(0)
	for i := range merged.Spans().Len() {
		val, ok := merged.Spans().At(i).Attributes().Get("group.key")
		require.True(t, ok)
		assert.Equal(t, "éééé", val.Str())
	}
	val, ok := traces[singleIdx].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().Get("group.key")
	require.True(t, ok)
	assert.Equal(t, "short", val.Str())
}

// TestMaxRecordBytesTruncatesSeverityText covers #53: the severity text is log
// input too, so it is bounded by the same max_record_bytes bound as the body.
func TestMaxRecordBytesTruncatesSeverityText(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.Timeout = 20 * time.Millisecond
	cfg.MaxRecordBytes = 16
	conn := createTestConnector(t, cfg, sink)

	sendLogs(t, conn, []plog.LogRecord{
		newLogRecord("user=123 x", time.Now(), strings.Repeat("é", 64)),
	})

	time.Sleep(100 * time.Millisecond)

	traces := flatTraces(sink)
	require.Len(t, traces, 1)
	val, ok := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().Get("log.severity")
	require.True(t, ok)
	assert.Equal(t, "éééééééé", val.Str(), "truncated rune-safe to max_record_bytes bytes")
}

// TestDecodeIDBytesRejectsHugeStringsBeforeDecoding covers #54:
// hex.DecodeString allocates len(src)/2 bytes before validating anything, so
// the length must be checked first. A wrong-length string must be rejected
// without a proportional allocation, whatever its content.
func TestDecodeIDBytesRejectsHugeStringsBeforeDecoding(t *testing.T) {
	hugeValidHex := pcommon.NewValueStr(strings.Repeat("ab", 1<<20)) // valid hex, wrong length
	hugeNonHex := pcommon.NewValueStr(strings.Repeat("z", 2<<20))

	_, ok := decodeIDBytes(hugeValidHex, len(pcommon.TraceID{}))
	assert.False(t, ok)
	_, ok = decodeIDBytes(hugeNonHex, len(pcommon.TraceID{}))
	assert.False(t, ok)

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, _ = decodeIDBytes(hugeValidHex, len(pcommon.TraceID{}))
	runtime.ReadMemStats(&after)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(512<<10),
		"rejecting a wrong-length hex string must not decode (and allocate) it first")
}

// TestBoundQuoted pins the per-element bound in writeValueJSON (#55): the cut
// is rune-safe and no-op when the value already fits.
func TestBoundQuoted(t *testing.T) {
	assert.Equal(t, "abc", boundQuoted("abcdef", 3))
	assert.Equal(t, "abcdef", boundQuoted("abcdef", 100))
	assert.Empty(t, boundQuoted("abcdef", 0))
	// Rune-safe: the cut never splits a multi-byte character (the result may
	// be slightly under the room, never an invalid prefix).
	assert.Equal(t, "é", boundQuoted(strings.Repeat("é", 10), 3))
}

// TestExtractionBoundedForHugeNestedStringValue covers #55: one huge string
// element inside a Map body must not be quoted in full when the template only
// reads a bounded prefix. MemStats, not AllocsPerRun: the bug is allocation
// *size*, not count.
func TestExtractionBoundedForHugeNestedStringValue(t *testing.T) {
	logs := plog.NewLogs()
	lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	inner := lr.Body().SetEmptyMap().PutEmptyMap("outer")
	inner.PutStr("msg", strings.Repeat("x", 1<<20))

	cfg := createDefaultConfig() // {truncated:120:body}, max_record_bytes 16 KiB
	opts := extractOptionsFor(cfg, mustCompileTemplate(t, cfg.SpanNameTemplate))

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	rec := extractLogRecord(lr, cfg, opts)
	runtime.ReadMemStats(&after)

	assert.Len(t, []rune(rec.body), 120, "the bounded prefix is still produced")
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(256<<10),
		"extracting a bounded prefix of a 1 MiB nested string must not serialize it in full")
}
