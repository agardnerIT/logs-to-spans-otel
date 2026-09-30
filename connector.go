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
	telemetry      *telemetry
}

type logGroup struct {
	key         string
	records     []*logRecord
	timer       *time.Timer
	maxTimer    *time.Timer
	prevTraceID pcommon.TraceID
	prevSpanID  pcommon.SpanID
	traceID     pcommon.TraceID
	lastSpanID  pcommon.SpanID
	// lastUpdated is the time the most recent record was added. It drives the
	// least-recently-updated eviction when max_groups is reached.
	lastUpdated time.Time
	// flushed guards against a group being emitted twice. time.Timer.Stop()
	// returns false when the callback has already fired and is queued, so a
	// stale callback can still run after the group has left the map. It also
	// stops that callback evicting the replacement group under the same key.
	flushed bool
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

// activeGroupCount reports how many groups are buffered right now. It is read
// by the active_groups observable gauge, so it takes the same lock every other
// map access takes.
func (c *logsToSpansConnector) activeGroupCount() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return int64(len(c.groups))
}

func (c *logsToSpansConnector) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (c *logsToSpansConnector) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				c.telemetry.logsIngested.Add(ctx, 1)
				key := c.extractGroupKey(lr)
				if key == "" {
					c.telemetry.unmatchedDropped.Add(ctx, 1)
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

	if c.stopped {
		c.mu.Unlock()
		return
	}

	group, exists := c.groups[key]
	var evicted *logGroup
	if !exists {
		// Bound the number of live groups. A high-cardinality key (a request or
		// connection ID, a raw trace ID) otherwise creates one map entry and two
		// live timers per distinct value until the timeout fires.
		if c.config.MaxGroups > 0 && len(c.groups) >= c.config.MaxGroups {
			evicted = c.evictLeastRecentlyUpdatedLocked()
		}
		group = &logGroup{key: key}
		c.groups[key] = group
		group.maxTimer = time.AfterFunc(c.config.MaxWait, func() {
			c.flushGroup(key, group)
		})
	}

	group.lastUpdated = time.Now()
	group.records = append(group.records, extractLogRecord(lr, c.config))

	if group.timer != nil {
		group.timer.Stop()
	}
	group.timer = time.AfterFunc(c.config.Timeout, func() {
		c.flushGroup(key, group)
	})

	if c.config.MaxLogsPerTrace > 0 && len(group.records) >= c.config.MaxLogsPerTrace {
		traceID := generateTraceID()
		lastSpanID := generateSpanID()
		flushedRecords := group.records
		flushedPrevTraceID := group.prevTraceID
		flushedPrevSpanID := group.prevSpanID

		// Retire the outgoing group before it leaves the map. Its timers may
		// already have fired and queued a callback, and Stop() returning false
		// is not enough on its own: mark the group flushed so a queued callback
		// becomes a no-op instead of re-emitting these records or deleting the
		// replacement group installed below.
		group.flushed = true
		if group.maxTimer != nil {
			group.maxTimer.Stop()
		}
		if group.timer != nil {
			group.timer.Stop()
		}
		delete(c.groups, key)
		group.records = nil

		newGroup := &logGroup{key: key}
		newGroup.lastUpdated = time.Now()
		newGroup.prevTraceID = traceID
		newGroup.prevSpanID = lastSpanID
		c.groups[key] = newGroup
		newGroup.maxTimer = time.AfterFunc(c.config.MaxWait, func() {
			c.flushGroup(key, newGroup)
		})
		newGroup.timer = time.AfterFunc(c.config.Timeout, func() {
			c.flushGroup(key, newGroup)
		})
		c.mu.Unlock()
		c.processGroup(context.Background(), &logGroup{
			key:         key,
			records:     flushedRecords,
			prevTraceID: flushedPrevTraceID,
			prevSpanID:  flushedPrevSpanID,
			traceID:     traceID,
			lastSpanID:  lastSpanID,
		})
		c.emitEvictedGroup(evicted)
		return
	}

	c.mu.Unlock()
	c.emitEvictedGroup(evicted)
}

// evictLeastRecentlyUpdatedLocked removes and returns the group with the
// oldest lastUpdated time so a new group can take its place. The caller must
// hold c.mu. Ties are broken by key, which keeps eviction deterministic when
// two groups were updated in the same clock tick.
func (c *logsToSpansConnector) evictLeastRecentlyUpdatedLocked() *logGroup {
	var victimKey string
	var victim *logGroup
	for key, group := range c.groups {
		if victim == nil || group.lastUpdated.Before(victim.lastUpdated) ||
			(group.lastUpdated.Equal(victim.lastUpdated) && key < victimKey) {
			victimKey, victim = key, group
		}
	}
	if victim == nil {
		return nil
	}

	// Retire the victim before it leaves the map, exactly as the
	// max_logs_per_trace split does: an already-queued timer callback must
	// find the group flushed instead of re-emitting it or deleting a
	// replacement.
	victim.flushed = true
	if victim.timer != nil {
		victim.timer.Stop()
	}
	if victim.maxTimer != nil {
		victim.maxTimer.Stop()
	}
	delete(c.groups, victimKey)
	return victim
}

// emitEvictedGroup converts a group evicted by max_groups into a trace. The
// group has already left the map with its timers stopped, so this only emits.
// Evicted records are not lost - the group is flushed early - but the early
// trace boundary is visible to operators through the groups_evicted counter.
func (c *logsToSpansConnector) emitEvictedGroup(group *logGroup) {
	if group == nil {
		return
	}
	c.telemetry.groupsEvicted.Add(context.Background(), 1)
	c.logger.Debug("evicted least recently updated group: max_groups reached",
		zap.String("group_key", group.key),
		zap.Int("max_groups", c.config.MaxGroups),
		zap.Int("log_count", len(group.records)),
	)
	c.processGroup(context.Background(), group)
}

func (c *logsToSpansConnector) flushGroup(key string, group *logGroup) {
	c.mu.Lock()
	if c.stopped || group.flushed {
		c.mu.Unlock()
		return
	}
	group.flushed = true

	// Stop this group's own timers before touching the map. Both timer and
	// maxTimer point at this group, so whichever fires second must find it
	// already flushed and do nothing.
	if group.maxTimer != nil {
		group.maxTimer.Stop()
	}
	if group.timer != nil {
		group.timer.Stop()
	}

	// Only the group that still owns the key may evict it. A callback from a
	// group replaced by max_logs_per_trace must not delete the replacement.
	if current, ok := c.groups[key]; ok && current == group {
		delete(c.groups, key)
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

// extractGroupKey returns the value used to group a log record, or "" when
// nothing matches. Precedence is attribute keys first, then the body:
//
//  1. group_by_attributes, in order, matched against the log record's
//     attributes. Attribute values are converted to string, so numeric and
//     boolean fields work without a transform processor.
//  2. group_by_keys against a structured (Map) body, in order.
//  3. group_by_keys against an unstructured (string) body, as key=value.
//
// Attributes win because they are explicit structured fields; the body regexes
// are a heuristic that can match text the log never intended as a key.
func (c *logsToSpansConnector) extractGroupKey(lr plog.LogRecord) string {
	attrs := lr.Attributes()
	for _, key := range c.config.GroupByAttributes {
		if val, ok := attrs.Get(key); ok {
			if s := valueToString(val); s != "" {
				return s
			}
		}
	}

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

	traceID := group.traceID
	if traceID == (pcommon.TraceID{}) {
		traceID = generateTraceID()
	}

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", c.config.ServiceName)
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("logs-to-spans")

	for i, rec := range group.records {
		span := ss.Spans().AppendEmpty()
		span.SetTraceID(traceID)

		if i == len(group.records)-1 && group.lastSpanID != (pcommon.SpanID{}) {
			span.SetSpanID(group.lastSpanID)
		} else {
			span.SetSpanID(generateSpanID())
		}

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

		if i == 0 && group.prevTraceID != (pcommon.TraceID{}) {
			link := span.Links().AppendEmpty()
			link.SetTraceID(group.prevTraceID)
			link.SetSpanID(group.prevSpanID)
		}
	}

	c.logger.Info("converted log group to trace",
		zap.String("group_key", group.key),
		zap.Int("log_count", len(group.records)),
	)

	c.telemetry.tracesCreated.Add(ctx, 1)

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
