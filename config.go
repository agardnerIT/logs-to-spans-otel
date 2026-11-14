// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs_to_spans

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"time"
)

// defaultServiceName is written to the output resource when the connector is
// not configured with an explicit service_name and the source logs do not
// carry one either. It guarantees every emitted trace has a service.name.
const defaultServiceName = "logs-to-spans"

// Upper bounds enforced by Validate. They exist so a typo (an extra zero in
// max_groups, say) fails at startup instead of at OOM. They are deliberately
// far above any sane deployment.
const (
	maxGroupsUpperBound         = 1_000_000
	maxLogsPerTraceUpperBound   = 1_000_000
	maxBufferedRecordsBound     = 10_000_000
	maxRecordBytesUpperBound    = 64 << 20
	maxGroupBytesUpperBound     = 1 << 30
	maxSpanNameLengthUpperBound = 4096
)

// Config defines the configuration for the logs_to_spans connector.
type Config struct {
	// Timeout is the inactivity period after which a group is flushed. Each new
	// log record in a group resets it.
	Timeout time.Duration `mapstructure:"timeout"`

	// MaxWait is the maximum time a group may stay open, measured from its first
	// log record, regardless of later activity.
	MaxWait time.Duration `mapstructure:"max_wait"`

	// MaxLogsPerTrace caps the number of log records in one trace. When a group
	// reaches the cap it is flushed as a trace and a follow-on group starts with
	// a span link back to it. 0 disables the cap.
	MaxLogsPerTrace int `mapstructure:"max_logs_per_trace"`

	// MaxGroups bounds the number of buffered groups. When a new group would
	// exceed it, the least recently updated group is flushed early. 0 disables
	// the bound.
	MaxGroups int `mapstructure:"max_groups"`

	// MaxRecordBytes bounds the retained size of one buffered log record. A
	// longer body is truncated, rune-safe, so a single huge log line cannot be
	// retained in full. Truncation is counted by
	// otelcol_connector_logs_to_spans_records_truncated. 0 disables the bound.
	MaxRecordBytes int `mapstructure:"max_record_bytes"`

	// MaxGroupBytes bounds the retained body bytes of one group. When a record
	// would push a group past the bound, the group is flushed as a trace and a
	// follow-on group starts with a span link back to it, exactly as
	// max_logs_per_trace does. Records are never dropped for this bound. 0
	// disables it.
	MaxGroupBytes int `mapstructure:"max_group_bytes"`

	// GroupByKeys is the ordered list of keys to look for in the log body, as
	// structured map keys or as key=value pairs. At least one of GroupByKeys or
	// GroupByAttributes must be set.
	GroupByKeys []string `mapstructure:"group_by_keys"`

	// GroupByAttributes is the ordered list of log attribute names to use as the
	// group key. Attribute matches take precedence over GroupByKeys.
	GroupByAttributes []string `mapstructure:"group_by_attributes"`

	// GroupByResourceAttributes is the ordered list of resource attribute names
	// that scope the group. Records whose resource differs in any of these
	// attributes form separate groups and therefore separate traces. It is an
	// additional scoping dimension: the extracted group key is still required.
	// With an empty list, resource attributes do not affect grouping.
	GroupByResourceAttributes []string `mapstructure:"group_by_resource_attributes"`

	// DurationKeys is the ordered list of attribute names that hold an explicit
	// span duration, as a number of seconds or a Go duration string. Values that
	// cannot be represented, or that exceed MaxSpanDuration, are ignored and
	// counted by otelcol_connector_logs_to_spans_invalid_durations.
	DurationKeys []string `mapstructure:"duration_keys"`

	// TraceIDKeys is the ordered list of attribute names that hold an
	// originating trace ID, as a 32-character hex string or 16 raw bytes. When a
	// log record carries one, the span generated from it links back to that
	// trace. Record-level trace context (set by a receiver or trace_parser
	// operator) takes precedence over these attributes. An empty list disables
	// the attribute lookup.
	//
	// Trust boundary: an attribute is written by whoever produced the log line,
	// so these IDs are attacker-influenced whenever the log source is not fully
	// trusted. The connector validates the format of an ID, never its
	// provenance; a producer can therefore make its spans link to an arbitrary
	// trace. Set TraceIDKeys/SpanIDKeys to an empty list (and rely on
	// record-level trace context, which only a receiver or a collector-side
	// operator can set) when the log source is not trusted, or use
	// GroupByResourceAttributes to keep tenants apart.
	TraceIDKeys []string `mapstructure:"trace_id_keys"`

	// SpanIDKeys is the ordered list of attribute names that hold the
	// originating span ID, as a 16-character hex string or 8 raw bytes. It is
	// looked up independently of TraceIDKeys, so a record-level trace ID can be
	// paired with a span ID from an attribute. An empty list disables the
	// attribute lookup. See TraceIDKeys for the trust boundary.
	SpanIDKeys []string `mapstructure:"span_id_keys"`

	// RequireSpanID makes a span link conditional on both a trace ID and a span
	// ID being present. By default a record that carries only a trace ID still
	// links, with a zero span ID. Requiring the pair means a forged or partial
	// link cannot be emitted, at the cost of dropping links for producers that
	// only know the trace ID.
	RequireSpanID bool `mapstructure:"require_span_id"`

	// EndSpanDuration is the duration given to the last span in a group when
	// neither an explicit duration nor a following log provides an end time.
	EndSpanDuration time.Duration `mapstructure:"end_span_duration"`

	// MaxSpanDuration rejects an explicit duration larger than this, which is
	// otherwise either nonsense or an overflow of time.Duration. 0 disables the
	// bound.
	MaxSpanDuration time.Duration `mapstructure:"max_span_duration"`

	// CopyResourceAttributes copies the source logs' resource attributes onto the
	// resource of every emitted trace. When a group contains records from more
	// than one resource, the first record's resource is used. Set to false to
	// emit only service.name.
	CopyResourceAttributes bool `mapstructure:"copy_resource_attributes"`

	// ServiceName overrides the service.name resource attribute of every emitted
	// trace. When empty, the source logs' service.name is preserved and
	// "logs-to-spans" is used when the source has none.
	ServiceName string `mapstructure:"service_name"`

	// SpanNameTemplate controls the name of every generated span. It is literal
	// text with three placeholders: {body} (the full log body), {severity} (the
	// log record's severity text) and {truncated:N:body} (the first N characters
	// of the body, the default). An unknown placeholder is a startup error. A
	// template that renders to an empty string falls back to the log body, so a
	// span name is never empty. The rendered name is capped at
	// MaxSpanNameLength runes.
	SpanNameTemplate string `mapstructure:"span_name_template"`

	// IncludeBodyAttribute attaches the log body as the log.body span attribute.
	// It is off by default: the body is already available through
	// SpanNameTemplate, and duplicating it on every span doubles the payload.
	IncludeBodyAttribute bool `mapstructure:"include_body_attribute"`

	// MaxSpanNameLength caps the rendered span name, counted in runes. Span
	// names are low-cardinality grouping keys in most backends, so an unbounded
	// name (a raw log line, say) defeats aggregation and inflates every index.
	// 0 disables the cap.
	MaxSpanNameLength int `mapstructure:"max_span_name_length"`
}

// validateAttributeKeys rejects empty entries in an attribute-name list. An
// empty key can never match a log or resource attribute and only hides a typo.
// field is the mapstructure name, used in the error message.
func validateAttributeKeys(field string, keys []string) error {
	if slices.Contains(keys, "") {
		return fmt.Errorf("%s must not contain an empty key", field)
	}
	return nil
}

// groupKeyValuePattern builds the regular expression used to pull "key=value"
// pairs out of unstructured log bodies. The user-supplied key is quoted so it
// is matched literally: without the quoting, "user.id" matches "userXid" and
// "user(" panics at compile time.
func groupKeyValuePattern(key string) string {
	return regexp.QuoteMeta(key) + `=(\S+)`
}

// buildGroupKeyRegexes compiles one extraction pattern per key, in order.
func buildGroupKeyRegexes(keys []string) ([]*regexp.Regexp, error) {
	regexes := make([]*regexp.Regexp, 0, len(keys))
	for _, key := range keys {
		if key == "" {
			return nil, errors.New("group_by_keys must not contain an empty key")
		}
		re, err := regexp.Compile(groupKeyValuePattern(key))
		if err != nil {
			return nil, fmt.Errorf("invalid group_by_keys entry %q: %w", key, err)
		}
		regexes = append(regexes, re)
	}
	return regexes, nil
}

// Validate reports invalid configuration instead of silently rewriting it.
// Defaults live in createDefaultConfig and are applied before Validate runs.
func (cfg *Config) Validate() error {
	var errs []error

	if len(cfg.GroupByKeys) == 0 && len(cfg.GroupByAttributes) == 0 {
		errs = append(errs, errors.New(
			"at least one of group_by_keys or group_by_attributes must be set: without one every log record is dropped",
		))
	} else if _, err := buildGroupKeyRegexes(cfg.GroupByKeys); err != nil {
		errs = append(errs, err)
	}

	if err := validateAttributeKeys("group_by_attributes", cfg.GroupByAttributes); err != nil {
		errs = append(errs, err)
	}

	if err := validateAttributeKeys("group_by_resource_attributes", cfg.GroupByResourceAttributes); err != nil {
		errs = append(errs, err)
	}

	if err := validateAttributeKeys("trace_id_keys", cfg.TraceIDKeys); err != nil {
		errs = append(errs, err)
	}

	if err := validateAttributeKeys("span_id_keys", cfg.SpanIDKeys); err != nil {
		errs = append(errs, err)
	}

	if err := validateAttributeKeys("duration_keys", cfg.DurationKeys); err != nil {
		errs = append(errs, err)
	}

	if cfg.Timeout <= 0 {
		errs = append(errs, fmt.Errorf("timeout must be greater than zero, got %s", cfg.Timeout))
	}
	if cfg.MaxWait <= 0 {
		errs = append(errs, fmt.Errorf("max_wait must be greater than zero, got %s", cfg.MaxWait))
	}
	if cfg.MaxLogsPerTrace < 0 {
		errs = append(errs, fmt.Errorf("max_logs_per_trace must not be negative, got %d", cfg.MaxLogsPerTrace))
	}
	if cfg.MaxLogsPerTrace > maxLogsPerTraceUpperBound {
		errs = append(errs, fmt.Errorf("max_logs_per_trace must not exceed %d, got %d", maxLogsPerTraceUpperBound, cfg.MaxLogsPerTrace))
	}
	if cfg.MaxGroups < 0 {
		errs = append(errs, fmt.Errorf("max_groups must not be negative, got %d", cfg.MaxGroups))
	}
	if cfg.MaxGroups > maxGroupsUpperBound {
		errs = append(errs, fmt.Errorf("max_groups must not exceed %d, got %d", maxGroupsUpperBound, cfg.MaxGroups))
	}
	if cfg.MaxGroups > 0 && cfg.MaxLogsPerTrace > 0 && cfg.MaxGroups > maxBufferedRecordsBound/cfg.MaxLogsPerTrace {
		errs = append(errs, fmt.Errorf(
			"max_groups × max_logs_per_trace must not exceed %d buffered records, got %d × %d",
			maxBufferedRecordsBound, cfg.MaxGroups, cfg.MaxLogsPerTrace,
		))
	}
	if cfg.MaxRecordBytes < 0 {
		errs = append(errs, fmt.Errorf("max_record_bytes must not be negative, got %d", cfg.MaxRecordBytes))
	}
	if cfg.MaxRecordBytes > maxRecordBytesUpperBound {
		errs = append(errs, fmt.Errorf("max_record_bytes must not exceed %d, got %d", maxRecordBytesUpperBound, cfg.MaxRecordBytes))
	}
	if cfg.MaxGroupBytes < 0 {
		errs = append(errs, fmt.Errorf("max_group_bytes must not be negative, got %d", cfg.MaxGroupBytes))
	}
	if cfg.MaxGroupBytes > maxGroupBytesUpperBound {
		errs = append(errs, fmt.Errorf("max_group_bytes must not exceed %d, got %d", maxGroupBytesUpperBound, cfg.MaxGroupBytes))
	}
	if cfg.MaxGroupBytes > 0 && cfg.MaxRecordBytes > cfg.MaxGroupBytes {
		errs = append(errs, fmt.Errorf(
			"max_group_bytes (%d) must be at least max_record_bytes (%d)", cfg.MaxGroupBytes, cfg.MaxRecordBytes,
		))
	}
	if cfg.EndSpanDuration <= 0 {
		errs = append(errs, fmt.Errorf("end_span_duration must be greater than zero, got %s", cfg.EndSpanDuration))
	}
	if cfg.MaxSpanDuration < 0 {
		errs = append(errs, fmt.Errorf("max_span_duration must not be negative, got %s", cfg.MaxSpanDuration))
	}
	if cfg.MaxSpanNameLength < 0 {
		errs = append(errs, fmt.Errorf("max_span_name_length must not be negative, got %d", cfg.MaxSpanNameLength))
	}
	if cfg.MaxSpanNameLength > maxSpanNameLengthUpperBound {
		errs = append(errs, fmt.Errorf("max_span_name_length must not exceed %d, got %d", maxSpanNameLengthUpperBound, cfg.MaxSpanNameLength))
	}

	if _, err := compileSpanNameTemplate(cfg.SpanNameTemplate); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func createDefaultConfig() *Config {
	return &Config{
		Timeout:                   5 * time.Second,
		MaxWait:                   30 * time.Second,
		MaxLogsPerTrace:           100,
		MaxGroups:                 1000,
		MaxRecordBytes:            16 << 10,
		MaxGroupBytes:             4 << 20,
		GroupByKeys:               []string{},
		GroupByAttributes:         []string{},
		GroupByResourceAttributes: []string{},
		DurationKeys:              []string{},
		TraceIDKeys:               []string{"trace_id", "trace.id"},
		SpanIDKeys:                []string{"span_id", "span.id"},
		RequireSpanID:             false,
		EndSpanDuration:           500 * time.Millisecond,
		MaxSpanDuration:           24 * time.Hour,
		CopyResourceAttributes:    true,
		// Empty means "preserve the source service.name, fall back to
		// defaultServiceName". A non-empty value is an explicit override.
		ServiceName: "",
		// Bounded by default: a span name is a low-cardinality grouping key in
		// every backend, and an unbounded name is the whole log line.
		SpanNameTemplate:     defaultSpanNameTemplate,
		IncludeBodyAttribute: false,
		MaxSpanNameLength:    200,
	}
}

// maxDurationSeconds is the largest number of whole seconds that fits in a
// time.Duration (which counts nanoseconds in an int64), about 292 years. A
// duration source above it overflows and wraps, which is why parseDuration
// rejects it instead of guessing.
const maxDurationSeconds = math.MaxInt64 / int64(time.Second)
