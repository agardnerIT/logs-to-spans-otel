package logs_to_spans

import (
	"context"
	"regexp"

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
	
	// Pre-compile regex patterns for group_by_keys
	compiledRegex := make([]*regexp.Regexp, 0, len(c.GroupByKeys))
	for _, key := range c.GroupByKeys {
		re := regexp.MustCompile(key + `=(\S+)`)
		compiledRegex = append(compiledRegex, re)
	}
	
	return &logsToSpansConnector{
		config:         c,
		logger:         set.Logger,
		tracesConsumer: tracesConsumer,
		groups:         make(map[string]*logGroup),
		compiledRegex:  compiledRegex,
	}, nil
}
