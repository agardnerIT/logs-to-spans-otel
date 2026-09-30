package logs_to_spans

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

type Config struct {
	Timeout           time.Duration `mapstructure:"timeout"`
	MaxWait           time.Duration `mapstructure:"max_wait"`
	MaxLogsPerTrace   int           `mapstructure:"max_logs_per_trace"`
	MaxGroups         int           `mapstructure:"max_groups"`
	GroupByKeys       []string      `mapstructure:"group_by_keys"`
	GroupByAttributes []string      `mapstructure:"group_by_attributes"`
	DurationKeys      []string      `mapstructure:"duration_keys"`
	EndSpanDuration   time.Duration `mapstructure:"end_span_duration"`
	ServiceName       string        `mapstructure:"service_name"`
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
