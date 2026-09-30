package logs_to_spans

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

const TypeStr = "logs_to_spans"

func NewFactory() connector.Factory {
	return connector.NewFactory(
		component.MustNewType(TypeStr),
		func() component.Config { return createDefaultConfig() },
		connector.WithLogsToTraces(createLogsToTraces, component.StabilityLevelDevelopment),
	)
}

func createLogsToTraces(
	_ context.Context,
	set connector.Settings,
	cfg component.Config,
	tracesConsumer consumer.Traces,
) (connector.Logs, error) {
	c := cfg.(*Config)

	// Config.Validate compiles the same patterns and returns any error, but the
	// factory can be reached without Validate (direct construction in tests and
	// embedders), so compile here too rather than panicking.
	compiledRegex, err := buildGroupKeyRegexes(c.GroupByKeys)
	if err != nil {
		return nil, err
	}

	telemetry, err := newTelemetry(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	return &logsToSpansConnector{
		config:         c,
		logger:         set.Logger,
		tracesConsumer: tracesConsumer,
		groups:         make(map[string]*logGroup),
		compiledRegex:  compiledRegex,
		telemetry:      telemetry,
	}, nil
}
