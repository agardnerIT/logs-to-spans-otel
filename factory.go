// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate make mdatagen

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

	// As with the regexes above, Validate compiles the template and reports a
	// useful error, but the factory can be reached without Validate. An absent
	// template means the default body-as-name behaviour.
	tmpl := c.SpanNameTemplate
	if tmpl == "" {
		tmpl = defaultSpanNameTemplate
	}
	spanName, err := compileSpanNameTemplate(tmpl)
	if err != nil {
		return nil, err
	}

	conn := &logsToSpansConnector{
		config:         c,
		logger:         set.Logger,
		tracesConsumer: tracesConsumer,
		groups:         make(map[string]*logGroup),
		compiledRegex:  compiledRegex,
		spanName:       spanName,
	}

	telemetry, err := newTelemetry(set.TelemetrySettings, conn.activeGroupCount)
	if err != nil {
		return nil, err
	}
	conn.telemetry = telemetry

	return conn, nil
}
