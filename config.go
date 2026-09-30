// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs_to_spans

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

// defaultServiceName is written to the output resource when the connector is
// not configured with an explicit service_name and the source logs do not
// carry one either. It guarantees every emitted trace has a service.name.
const defaultServiceName = "logs-to-spans"

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
	// span duration.
	DurationKeys []string `mapstructure:"duration_keys"`

	// TraceIDKeys is the ordered list of attribute names that hold an
	// originating trace ID, as a 32-character hex string or 16 raw bytes. When a
	// log record carries one, the span generated from it links back to that
	// trace. Record-level trace context (set by a receiver or trace_parser
	// operator) takes precedence over these attributes. An empty list disables
	// the attribute lookup.
	TraceIDKeys []string `mapstructure:"trace_id_keys"`

	// SpanIDKeys is the ordered list of attribute names that hold the
	// originating span ID, as a 16-character hex string or 8 raw bytes. It is
	// looked up independently of TraceIDKeys, so a record-level trace ID can be
	// paired with a span ID from an attribute. An empty list disables the
	// attribute lookup.
	SpanIDKeys []string `mapstructure:"span_id_keys"`

	// EndSpanDuration is the duration given to the last span in a group when
	// neither an explicit duration nor a following log provides an end time.
	EndSpanDuration time.Duration `mapstructure:"end_span_duration"`

	// CopyResourceAttributes copies the source logs' resource attributes onto the
	// resource of every emitted trace. When a group contains records from more
	// than one resource, the first record's resource is used. Set to false to
	// emit only service.name.
	CopyResourceAttributes bool `mapstructure:"copy_resource_attributes"`

	// ServiceName overrides the service.name resource attribute of every emitted
	// trace. When empty, the source logs' service.name is preserved and
	// "logs-to-spans" is used when the source has none.
	ServiceName string `mapstructure:"service_name"`
}

// validateAttributeKeys rejects empty entries in an attribute-name list. An
// empty key can never match a log or resource attribute and only hides a typo.
// field is the mapstructure name, used in the error message.
func validateAttributeKeys(field string, keys []string) error {
	for _, key := range keys {
		if key == "" {
			return fmt.Errorf("%s must not contain an empty key", field)
		}
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
			"at least one of group_by_keys or group_by_attributes must be set: without one every log record is dropped"))
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

	if cfg.Timeout <= 0 {
		errs = append(errs, fmt.Errorf("timeout must be greater than zero, got %s", cfg.Timeout))
	}
	if cfg.MaxWait <= 0 {
		errs = append(errs, fmt.Errorf("max_wait must be greater than zero, got %s", cfg.MaxWait))
	}
	if cfg.MaxLogsPerTrace < 0 {
		errs = append(errs, fmt.Errorf("max_logs_per_trace must not be negative, got %d", cfg.MaxLogsPerTrace))
	}
	if cfg.MaxGroups < 0 {
		errs = append(errs, fmt.Errorf("max_groups must not be negative, got %d", cfg.MaxGroups))
	}
	if cfg.EndSpanDuration <= 0 {
		errs = append(errs, fmt.Errorf("end_span_duration must be greater than zero, got %s", cfg.EndSpanDuration))
	}
	return errors.Join(errs...)
}

func createDefaultConfig() *Config {
	return &Config{
		Timeout:                   5 * time.Second,
		MaxWait:                   30 * time.Second,
		MaxLogsPerTrace:           100,
		MaxGroups:                 1000,
		GroupByKeys:               []string{},
		GroupByAttributes:         []string{},
		GroupByResourceAttributes: []string{},
		DurationKeys:              []string{},
		TraceIDKeys:               []string{"trace_id", "trace.id"},
		SpanIDKeys:                []string{"span_id", "span.id"},
		EndSpanDuration:           500 * time.Millisecond,
		CopyResourceAttributes:    true,
		// Empty means "preserve the source service.name, fall back to
		// defaultServiceName". A non-empty value is an explicit override.
		ServiceName: "",
	}
}
