package logs_to_spans

import (
	"time"
)

type Config struct {
	Timeout            time.Duration `mapstructure:"timeout"`
	MaxWait            time.Duration `mapstructure:"max_wait"`
	MaxLogsPerTrace    int           `mapstructure:"max_logs_per_trace"`
	GroupByKeys        []string      `mapstructure:"group_by_keys"`
	DurationKeys       []string      `mapstructure:"duration_keys"`
	EndSpanDuration    time.Duration `mapstructure:"end_span_duration"`
	UnmatchedBehaviour string        `mapstructure:"unmatched_behaviour"`
	ServiceName        string        `mapstructure:"service_name"`
}

func (cfg *Config) Validate() error {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = 30 * time.Second
	}
	if cfg.MaxLogsPerTrace < 0 {
		cfg.MaxLogsPerTrace = 100
	}
	if cfg.EndSpanDuration <= 0 {
		cfg.EndSpanDuration = 500 * time.Millisecond
	}
	if cfg.UnmatchedBehaviour == "" {
		cfg.UnmatchedBehaviour = "drop"
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "logs-to-spans"
	}
	return nil
}

func createDefaultConfig() *Config {
	return &Config{
		Timeout:            5 * time.Second,
		MaxWait:            30 * time.Second,
		MaxLogsPerTrace:    100,
		GroupByKeys:        []string{},
		DurationKeys:       []string{},
		EndSpanDuration:    500 * time.Millisecond,
		UnmatchedBehaviour: "drop",
		ServiceName:        "logs-to-spans",
	}
}
