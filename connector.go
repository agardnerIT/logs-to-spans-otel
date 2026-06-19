package logs_to_spans

import (
	"context"
	"crypto/rand"
	"regexp"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type logsToSpansConnector struct {
	config         *Config
	logger         *zap.Logger
	groups         map[string]*logGroup
	tracesConsumer consumer.Traces
	mu             sync.Mutex
	stopped        bool
	compiledRegex  []*regexp.Regexp
}

type logGroup struct {
	key       string
	records   []*logRecord
	timer     *time.Timer
	maxTimer  *time.Timer
}

type logRecord struct {
	timestamp time.Time
	duration  time.Duration
	body      string
	severity  string
}

func (c *logsToSpansConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *logsToSpansConnector) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (c *logsToSpansConnector) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				key := c.extractGroupKey(lr)
				if key == "" {
					continue
				}
				c.addToGroup(key, lr)
			}
		}
	}
	return nil
}

func (c *logsToSpansConnector) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()

	c.mu.Lock()
	for _, g := range c.groups {
		if g.timer != nil {
			g.timer.Stop()
		}
		if g.maxTimer != nil {
			g.maxTimer.Stop()
		}
	}
	groups := make([]*logGroup, 0, len(c.groups))
	for _, g := range c.groups {
		groups = append(groups, g)
	}
	c.groups = make(map[string]*logGroup)
	c.mu.Unlock()

	for _, g := range groups {
		c.processGroup(ctx, g)
	}
	return nil
}

func (c *logsToSpansConnector) addToGroup(key string, lr plog.LogRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return
	}

	group, exists := c.groups[key]
	if !exists {
		group = &logGroup{key: key}
		c.groups[key] = group
		group.maxTimer = time.AfterFunc(c.config.MaxWait, func() {
			c.flushGroup(key, group)
		})
	}

	group.records = append(group.records, extractLogRecord(lr, c.config))

	if group.timer != nil {
		group.timer.Stop()
	}
	group.timer = time.AfterFunc(c.config.Timeout, func() {
		c.flushGroup(key, group)
	})
}

func (c *logsToSpansConnector) flushGroup(key string, group *logGroup) {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	delete(c.groups, key)
	if group.maxTimer != nil {
		group.maxTimer.Stop()
	}
	if group.timer != nil {
		group.timer.Stop()
	}
	c.mu.Unlock()
	c.processGroup(context.Background(), group)
}

func extractLogRecord(lr plog.LogRecord, cfg *Config) *logRecord {
	ts := lr.ObservedTimestamp().AsTime()
	if lr.Timestamp() != 0 {
		ts = lr.Timestamp().AsTime()
	}

	var dur time.Duration
	for _, key := range cfg.DurationKeys {
		if val, ok := lr.Attributes().Get(key); ok {
			if d := parseDuration(val); d > 0 {
				dur = d
				break
			}
		}
	}

	return &logRecord{
		timestamp: ts,
		duration:  dur,
		body:      valueToString(lr.Body()),
		severity:  lr.SeverityText(),
	}
}

func parseDuration(v pcommon.Value) time.Duration {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		d, err := time.ParseDuration(v.Str())
		if err != nil {
			return 0
		}
		return d
	case pcommon.ValueTypeInt:
		return time.Duration(v.Int()) * time.Second
	case pcommon.ValueTypeDouble:
		return time.Duration(v.Double() * float64(time.Second))
	default:
		return 0
	}
}

func valueToString(v pcommon.Value) string {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeMap:
		return v.AsString()
	default:
		return v.AsString()
	}
}

func (c *logsToSpansConnector) extractGroupKey(lr plog.LogRecord) string {
	if len(c.config.GroupByKeys) == 0 {
		return ""
	}

	body := lr.Body()

	if body.Type() == pcommon.ValueTypeMap {
		m := body.Map()
		for _, key := range c.config.GroupByKeys {
			if val, ok := m.Get(key); ok {
				s := val.AsString()
				if s != "" {
					return s
				}
			}
		}
		return ""
	}

	bodyStr := valueToString(body)
	for i := range c.config.GroupByKeys {
		re := c.compiledRegex[i]
		matches := re.FindStringSubmatch(bodyStr)
		if len(matches) >= 2 && matches[1] != "" {
			return matches[1]
		}
	}

	return ""
}

func (c *logsToSpansConnector) processGroup(ctx context.Context, group *logGroup) {
	if len(group.records) == 0 {
		return
	}

	sort.Slice(group.records, func(i, j int) bool {
		return group.records[i].timestamp.Before(group.records[j].timestamp)
	})

	traceID := generateTraceID()

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", c.config.ServiceName)
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("logs-to-spans")

	for i, rec := range group.records {
		span := ss.Spans().AppendEmpty()
		span.SetTraceID(traceID)
		span.SetSpanID(generateSpanID())
		span.SetName(rec.body)
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(rec.timestamp))
		span.SetKind(ptrace.SpanKindInternal)

		var endTime time.Time
		if rec.duration > 0 {
			endTime = rec.timestamp.Add(rec.duration)
		} else if i < len(group.records)-1 {
			endTime = group.records[i+1].timestamp
		} else {
			endTime = rec.timestamp.Add(c.config.EndSpanDuration)
		}
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(endTime))

		attrs := span.Attributes()
		attrs.PutStr("log.body", rec.body)
		if rec.severity != "" {
			attrs.PutStr("log.severity", rec.severity)
		}
		attrs.PutStr("group.key", group.key)

		if i > 0 {
			parentSpanID := ss.Spans().At(i - 1).SpanID()
			span.SetParentSpanID(parentSpanID)
		}
	}

	c.logger.Info("converted log group to trace",
		zap.String("group_key", group.key),
		zap.Int("log_count", len(group.records)),
	)

	if err := c.tracesConsumer.ConsumeTraces(ctx, td); err != nil {
		c.logger.Error("failed to consume traces", zap.Error(err))
	}
}

func generateTraceID() pcommon.TraceID {
	var tid [16]byte
	_, _ = rand.Read(tid[:])
	return pcommon.TraceID(tid)
}

func generateSpanID() pcommon.SpanID {
	var sid [8]byte
	_, _ = rand.Read(sid[:])
	return pcommon.SpanID(sid)
}
