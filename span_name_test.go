// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs_to_spans

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestCompileSpanNameTemplateValid(t *testing.T) {
	rec := &logRecord{body: "Hello world", severity: "ERROR"}

	tests := []struct {
		name     string
		template string
		expected string
	}{
		{name: "body", template: "{body}", expected: "Hello world"},
		{name: "severity and body", template: "{severity}: {body}", expected: "ERROR: Hello world"},
		{name: "body then severity", template: "{body} ({severity})", expected: "Hello world (ERROR)"},
		{name: "literal only", template: "log", expected: "log"},
		{name: "literal around placeholder", template: "[{severity}] {body}", expected: "[ERROR] Hello world"},
		{name: "truncated", template: "{truncated:5:body}", expected: "Hello"},
		{name: "truncated longer than body", template: "{truncated:100:body}", expected: "Hello world"},
		{name: "truncated with whitespace", template: "{ truncated : 5 : body }", expected: "Hello"},
		{name: "severity twice", template: "{severity}-{severity}", expected: "ERROR-ERROR"},
		{name: "no braces at all", template: "plain name", expected: "plain name"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpl, err := compileSpanNameTemplate(tc.template)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, tmpl.render(rec))
		})
	}
}

func TestCompileSpanNameTemplateErrors(t *testing.T) {
	tests := []struct {
		name     string
		template string
	}{
		{name: "empty", template: ""},
		{name: "whitespace only", template: "   "},
		{name: "unterminated", template: "{body"},
		{name: "unknown placeholder", template: "{bodyy}"},
		{name: "unknown word", template: "{message}"},
		{name: "truncated missing body", template: "{truncated:10}"},
		{name: "truncated wrong target", template: "{truncated:10:severity}"},
		{name: "truncated non numeric", template: "{truncated:ten:body}"},
		{name: "truncated zero", template: "{truncated:0:body}"},
		{name: "truncated negative", template: "{truncated:-5:body}"},
		{name: "truncated too many tokens", template: "{truncated:10:body:extra}"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := compileSpanNameTemplate(tc.template)
			require.Error(t, err, "template %q must be rejected", tc.template)
			assert.Contains(t, err.Error(), "span_name_template",
				"the error must name the option so the user can find it")
		})
	}
}

func TestTruncateRunes(t *testing.T) {
	// "héllo" is 6 bytes but 5 runes; truncating at 3 runes must not split é.
	assert.Equal(t, "hé", truncateRunes("héllo", 2))
	assert.Equal(t, "héllo", truncateRunes("héllo", 5))
	assert.Equal(t, "héllo", truncateRunes("héllo", 99))
	assert.Equal(t, "", truncateRunes("héllo", 0))
	assert.Equal(t, "日本", truncateRunes("日本語", 2))
}

func TestSpanNameTemplateDefaultIsBody(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
	sendLogs(t, conn, []plog.LogRecord{newLogRecord("user=123 Hello world", now, "ERROR")})
	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)
	assert.Equal(t, "user=123 Hello world", traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
}

func TestSpanNameTemplateSeverityPrefix(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	cfg.SpanNameTemplate = "{severity}: {body}"
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
	sendLogs(t, conn, []plog.LogRecord{newLogRecord("user=123 Hello world", now, "ERROR")})
	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)
	assert.Equal(t, "ERROR: user=123 Hello world",
		traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
}

func TestSpanNameTemplateTruncatedBody(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	cfg.SpanNameTemplate = "{truncated:8:body}"
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
	sendLogs(t, conn, []plog.LogRecord{newLogRecord("user=123 Hello world", now, "ERROR")})
	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)
	assert.Equal(t, "user=123 Hello world"[:8],
		traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
}

func TestSpanNameTemplateLiteral(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	cfg.SpanNameTemplate = "log"
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
	sendLogs(t, conn, []plog.LogRecord{newLogRecord("user=123 Hello world", now, "ERROR")})
	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)
	assert.Equal(t, "log", traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
}

func TestSpanNameTemplateEmptyRenderFallsBackToBody(t *testing.T) {
	sink := newTestSink()
	cfg := createDefaultConfig()
	cfg.Timeout = 100 * time.Millisecond
	cfg.GroupByKeys = []string{"user"}
	// The record below carries no severity, so this template renders empty.
	cfg.SpanNameTemplate = "{severity}"
	conn := createTestConnector(t, cfg, sink)

	now := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
	sendLogs(t, conn, []plog.LogRecord{newLogRecord("user=123 Hello world", now, "")})
	time.Sleep(200 * time.Millisecond)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)
	assert.Equal(t, "user=123 Hello world",
		traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name(),
		"an empty render must fall back to the body so the span name is never empty")
}

func TestConfigValidateRejectsInvalidSpanNameTemplate(t *testing.T) {
	cfg := validTestConfig()
	cfg.SpanNameTemplate = "{nope}"
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "span_name_template")
}

func TestConfigValidateRejectsEmptySpanNameTemplate(t *testing.T) {
	cfg := validTestConfig()
	cfg.SpanNameTemplate = ""
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "span_name_template")
}

func TestFactoryRejectsInvalidSpanNameTemplate(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.SpanNameTemplate = "{truncated:oops:body}"

	factory := NewFactory()
	_, err := factory.CreateLogsToTraces(context.Background(), newTestSettings(), cfg, newTestSink())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "span_name_template")
}

func TestFactoryDefaultsAbsentSpanNameTemplate(t *testing.T) {
	// A Config built without createDefaultConfig has no template. The factory
	// must default it rather than reject the config.
	cfg := createDefaultConfig()
	cfg.GroupByKeys = []string{"user"}
	cfg.SpanNameTemplate = ""
	// Validate would reject this, but the factory is reachable without Validate.

	conn := createTestConnector(t, cfg, newTestSink())
	require.NotNil(t, conn.(*logsToSpansConnector).spanName)
}
