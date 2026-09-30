// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs_to_spans

import (
	"errors"
	"fmt"
	"regexp"
	"time"
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

	// GroupByKeys is the ordered list of keys to look for in the log body, as
	// structured map keys or as key=value pairs. At least one of GroupByKeys or
	// GroupByAttributes must be set.
	GroupByKeys []string `mapstructure:"group_by_keys"`

	// GroupByAttributes is the ordered list of log attribute names to use as the
	// group key. Attribute matches take precedence over GroupByKeys.
	GroupByAttributes []string `mapstructure:"group_by_attributes"`

	// DurationKeys is the ordered list of attribute names that hold an explicit
	// span duration.
	DurationKeys []string `mapstructure:"duration_keys"`

	// EndSpanDuration is the duration given to the last span in a group when
	// neither an explicit duration nor a following log provides an end time.
	EndSpanDuration time.Duration `mapstructure:"end_span_duration"`

	// ServiceName is written to the service.name resource attribute of every
	// emitted trace.
	ServiceName string `mapstructure:"service_name"`
}

// validateAttributeKeys rejects empty entries in group_by_attributes. An empty
// key can never match a log attribute and only hides a typo.
func validateAttributeKeys(keys []string) error {
	for _, key := range keys {
		if key == "" {
			return errors.New("group_by_attributes must not contain an empty key")
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

	if err := validateAttributeKeys(cfg.GroupByAttributes); err != nil {
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
	if cfg.ServiceName == "" {
		errs = append(errs, errors.New("service_name must not be empty"))
	}

	return errors.Join(errs...)
}

func createDefaultConfig() *Config {
	return &Config{
		Timeout:           5 * time.Second,
		MaxWait:           30 * time.Second,
		MaxLogsPerTrace:   100,
		MaxGroups:         1000,
		GroupByKeys:       []string{},
		GroupByAttributes: []string{},
		DurationKeys:      []string{},
		EndSpanDuration:   500 * time.Millisecond,
		ServiceName:       "logs-to-spans",
	}
}
