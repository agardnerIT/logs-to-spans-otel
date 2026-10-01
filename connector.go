// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs_to_spans

import (
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/agardnerIT/logs-to-spans-otel/internal/metadata"
)

type logsToSpansConnector struct {
	config         *Config
	logger         *zap.Logger
	tracesConsumer consumer.Traces
	compiledRegex  []*regexp.Regexp
	spanName       *spanNameTemplate
	telemetry      *metadata.TelemetryBuilder

	// mu guards every field below plus the groups map. The per-record path only
	// takes it for the O(1) map/list splice; deadline checks, resource copying
	// and eviction selection no longer run under it. See reapExpired.
	mu      sync.Mutex
	groups  map[string]*logGroup
	lru     *list.List // container/list of *logGroup, front = most recently updated
	stopped bool

	// reaperStop/reaperDone are the lifecycle handles of the single background
	// reaper goroutine started by startReaperLocked. Both are nil before the
	// reaper starts and again after Shutdown stops it. They are guarded by mu,
	// which makes starting during a concurrent Shutdown impossible: addToGroup
	// only starts it after checking stopped under the same lock.
	reaperStop chan struct{}
	reaperDone chan struct{}
}

type logGroup struct {
	key string
	// mapKey is the fully scoped key used in the groups map (the extracted key
	// plus any group_by_resource_attributes components). It is kept so the LRU
	// list can evict a group without a map scan.
	mapKey string
	// resource is a deep copy of the source resource attributes of the first
	// record in the group, taken while the upstream pdata is still valid. It is
	// copied onto the emitted trace's resource when copy_resource_attributes is
	// enabled.
	resource    pcommon.Map
	records     []*logRecord
	prevTraceID pcommon.TraceID
	prevSpanID  pcommon.SpanID
	traceID     pcommon.TraceID
	lastSpanID  pcommon.SpanID
	// elem is this group's node in the connector LRU list.
	elem *list.Element
	// firstAdded is when the group (or its post-split replacement) first
	// received a record. It caps the group's absolute lifetime at max_wait.
	firstAdded time.Time
	// lastUpdated is the time the most recent record was added. It drives the
	// inactivity timeout and, by recency order, max_groups eviction.
	lastUpdated time.Time
	// flushed guards against a group being emitted twice. A group is stamped
	// flushed while still under the connector mutex, before it leaves the map.
	// It stops a reaper that collected the group and a manual flushGroup racing
	// on the same pointer from emitting it twice.
	flushed bool
}

type logRecord struct {
	timestamp time.Time
	duration  time.Duration
	body      string
	severity  string
	// traceID and spanID are the trace context the log record arrived with,
	// zero when it carried none. They become a span link on the generated span
	// so the emitted trace stays connected to the real trace it came from.
	traceID pcommon.TraceID
	spanID  pcommon.SpanID
}

func (*logsToSpansConnector) Capabilities() consumer.Capabilities {
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return nil
	}
	c.startReaperLocked()
	return nil
}

func (c *logsToSpansConnector) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resource := rl.Resource().Attributes()
		// The resource is deep-copied at most once per ResourceLogs, outside the
		// connector mutex, and only if this batch actually has groupable records.
		// A group created below takes ownership of the copy; a group that already
		// exists ignores it. Either way no pdata copy happens under the mutex.
		var resourceCopy pcommon.Map
		resourceCopied := false

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			// Group the batch's records by extracted key so the connector mutex
			// is taken once per group per batch instead of once per record. A
			// single-key stream therefore does one small lock acquisition per
			// ConsumeLogs call rather than one per log. order is capped so a
			// huge single-key scope does not preallocate a huge key slice.
			batches := make(map[string][]*logRecord)
			order := make([]string, 0, min(sl.LogRecords().Len(), 16))
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				c.telemetry.ConnectorLogsToSpansLogsIngested.Add(ctx, 1)
				key := c.extractGroupKey(lr)
				if key == "" {
					c.telemetry.ConnectorLogsToSpansUnmatchedDropped.Add(ctx, 1)
					continue
				}

				// Extract the record before taking the lock. The body/severity
				// conversion is the dominant per-record cost (for a Map body
				// valueToString serializes it to JSON while it runs), and running it
				// inside c.mu serialized every core against all groups and the
				// reaper. The result is an immutable copy, so the batch append only
				// does map/list work under the lock. Extraction must stay eager: plog
				// values are views into upstream-owned pdata and must not be retained
				// past ConsumeLogs (the connector declares MutatesData: false).
				rec := extractLogRecord(lr, c.config)
				if _, seen := batches[key]; !seen {
					order = append(order, key)
				}
				batches[key] = append(batches[key], rec)
			}

			if len(order) == 0 {
				continue
			}
			if !resourceCopied {
				resourceCopy = copyResourceAttributes(resource)
				resourceCopied = true
			}
			for _, key := range order {
				c.addRecords(key, resource, resourceCopy, batches[key])
			}
		}
	}
	return nil
}

func (c *logsToSpansConnector) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	c.stopped = true
	stop := c.reaperStop
	done := c.reaperDone
	c.reaperStop = nil
	c.reaperDone = nil

	groups := make([]*logGroup, 0, len(c.groups))
	for _, g := range c.groups {
		g.flushed = true
		groups = append(groups, g)
	}
	c.groups = make(map[string]*logGroup)
	c.lru.Init()
	c.mu.Unlock()

	// Stop the reaper before flushing what is left, so no group can be emitted
	// twice and no goroutine survives Shutdown. The wait is outside the mutex:
	// a reaper mid-scan may still hold it.
	if stop != nil {
		close(stop)
		<-done
	}

	for _, g := range groups {
		c.processGroup(ctx, g)
	}

	// Unregister the active_groups observable gauge callback so the meter does
	// not retain the connector after shutdown.
	c.telemetry.Shutdown()
	return nil
}

// addToGroup buffers a single record. It is the entry point the tests drive;
// the hot path is ConsumeLogs, which batches a group's records into one call to
// addRecords. See addRecords for what actually runs under c.mu.
func (c *logsToSpansConnector) addToGroup(key string, resource pcommon.Map, rec *logRecord) {
	c.addRecords(key, resource, copyResourceAttributes(resource), []*logRecord{rec})
}

// addRecords buffers every record in recs under one acquisition of c.mu. recs
// all share the extracted key and resource, and are appended in order. The
// resource copy is supplied by the caller (ConsumeLogs copies it once per
// ResourceLogs) so what runs under the lock is deliberately small:
//
//   - the scoped map key is built before the lock, so the resource attribute
//     walk (and any JSON serialization of a Map-valued attribute) is parallel;
//   - a new group takes the caller's resource copy, so no pdata copy runs under
//     the mutex;
//   - max_groups eviction is O(1) off the LRU list, not an O(groups) scan;
//   - there are no per-group timers at all: the single reaper goroutine owns
//     every deadline, so only the map/list splice, the per-group append and a
//     rare max_logs_per_trace split remain.
func (c *logsToSpansConnector) addRecords(key string, resource, resourceCopy pcommon.Map, recs []*logRecord) {
	if len(recs) == 0 {
		return
	}
	mapKey := c.buildMapKey(key, resource)
	now := time.Now()

	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.startReaperLocked()

	group, exists := c.groups[mapKey]
	var evicted *logGroup
	if !exists {
		// Bound the number of live groups. The LRU list makes this O(1): a
		// high-cardinality key (a request or connection ID, a raw trace ID)
		// otherwise grows the map without bound until the timeout fires.
		if c.config.MaxGroups > 0 && len(c.groups) >= c.config.MaxGroups {
			evicted = c.evictLeastRecentlyUsedLocked()
		}
		group = &logGroup{key: key, mapKey: mapKey, resource: resourceCopy, firstAdded: now, lastUpdated: now}
		group.elem = c.lru.PushFront(group)
		c.groups[mapKey] = group
	}

	var flushed []*logGroup
	for _, rec := range recs {
		if full := c.addRecordLocked(mapKey, group, rec, now); full != nil {
			flushed = append(flushed, full)
			// A max_logs_per_trace split installed a linked replacement under
			// the same key; the rest of the batch goes into it.
			group = c.groups[mapKey]
		}
	}
	c.mu.Unlock()

	for _, full := range flushed {
		c.processGroup(context.Background(), full)
	}
	c.emitEvictedGroup(evicted)
}

// addRecordLocked appends rec to group, stamps its recency and keeps the LRU
// order. When the group reaches max_logs_per_trace it is retired and replaced
// in place, and the retired group is returned for the caller to emit after it
// releases c.mu. The caller must hold c.mu.
func (c *logsToSpansConnector) addRecordLocked(mapKey string, group *logGroup, rec *logRecord, now time.Time) *logGroup {
	group.lastUpdated = now
	group.records = append(group.records, rec)
	if group.elem != nil {
		c.lru.MoveToFront(group.elem)
	}

	if c.config.MaxLogsPerTrace <= 0 || len(group.records) < c.config.MaxLogsPerTrace {
		return nil
	}

	traceID := generateTraceID()
	lastSpanID := generateSpanID()

	// Stamp the outgoing group flushed before it leaves the map. Nothing else
	// can emit it now: the reaper checks flushed under the same lock, and
	// flushGroup checks it too.
	group.flushed = true
	delete(c.groups, mapKey)
	if group.elem != nil {
		c.lru.Remove(group.elem)
		group.elem = nil
	}
	// The retired group becomes the emitted trace, so it owns the ids the next
	// group links back to.
	group.traceID = traceID
	group.lastSpanID = lastSpanID

	// The replacement inherits the group's resource: a split must not change
	// the resource of the records that follow. Its max_wait restarts here, as
	// the previous per-group timer did.
	replacement := &logGroup{
		key:         group.key,
		mapKey:      mapKey,
		resource:    group.resource,
		prevTraceID: traceID,
		prevSpanID:  lastSpanID,
		firstAdded:  now,
		lastUpdated: now,
	}
	replacement.elem = c.lru.PushFront(replacement)
	c.groups[mapKey] = replacement

	return group
}

// startReaperLocked starts the single reaper goroutine if it is not already
// running. The caller must hold c.mu and must have checked !c.stopped, so a
// concurrent Shutdown cannot set stopped and miss the goroutine. It is called
// from Start for the collector lifecycle and from the first addToGroup so
// embedders and tests that skip Start still get timeout flushing.
func (c *logsToSpansConnector) startReaperLocked() {
	if c.reaperStop != nil {
		return
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	c.reaperStop = stop
	c.reaperDone = done
	go c.reapLoop(stop, done)
}

// reapLoop is the connector's only timing goroutine. One tick scans for expired
// groups instead of two time.AfterFunc allocations per log record, which is
// what kept the per-record path inside a mutex. Shutdown closes stop and waits
// for done, so it never outlives the connector (the package runs under goleak).
func (c *logsToSpansConnector) reapLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(c.reapInterval())
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			c.reapExpired()
		}
	}
}

// reapInterval picks the reaper's tick. It is a quarter of the shorter of
// timeout and max_wait, clamped to [10ms, 1s]: fine enough that a short timeout
// is honored promptly, coarse enough that the scan is negligible against the
// default five-second timeout. A group can therefore be flushed up to one tick
// after its deadline; max_wait still caps a group's absolute lifetime.
func (c *logsToSpansConnector) reapInterval() time.Duration {
	interval := c.config.Timeout
	interval = min(interval, c.config.MaxWait)
	interval /= 4
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	if interval > time.Second {
		interval = time.Second
	}
	return interval
}

// reapExpired flushes every group past its inactivity timeout or its absolute
// max_wait. The O(groups) scan runs on the reaper goroutine, not on the
// per-record path; each collected group is emitted after the lock is released.
func (c *logsToSpansConnector) reapExpired() {
	now := time.Now()

	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	var expired []*logGroup
	for key, group := range c.groups {
		if now.Sub(group.lastUpdated) < c.config.Timeout && now.Sub(group.firstAdded) < c.config.MaxWait {
			continue
		}
		group.flushed = true
		delete(c.groups, key)
		if group.elem != nil {
			c.lru.Remove(group.elem)
			group.elem = nil
		}
		expired = append(expired, group)
	}
	c.mu.Unlock()

	for _, group := range expired {
		c.processGroup(context.Background(), group)
	}
}

// evictLeastRecentlyUsedLocked removes and returns the least recently updated
// group so a new group can take its place. The caller must hold c.mu. It reads
// the tail of the LRU list, so it is O(1); the list is kept in recency order by
// addRecordLocked and the replacement installed by a max_logs_per_trace split.
func (c *logsToSpansConnector) evictLeastRecentlyUsedLocked() *logGroup {
	elem := c.lru.Back()
	if elem == nil {
		return nil
	}
	victim := elem.Value.(*logGroup)

	// Retire the victim before it leaves the map, exactly as the
	// max_logs_per_trace split does: the reaper and flushGroup both check
	// flushed under this lock, so it cannot be emitted twice.
	victim.flushed = true
	c.lru.Remove(elem)
	victim.elem = nil
	delete(c.groups, victim.mapKey)
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
	c.telemetry.ConnectorLogsToSpansGroupsEvicted.Add(context.Background(), 1)
	c.logger.Debug("evicted least recently updated group: max_groups reached",
		zap.String("group_key", group.key),
		zap.Int("max_groups", c.config.MaxGroups),
		zap.Int("log_count", len(group.records)),
	)
	c.processGroup(context.Background(), group)
}

// flushGroup retires a group by key and emits it unless it has already been
// flushed. The reaper no longer needs it (it collects expired groups itself),
// but it remains the single, lock-guarded retire path the tests drive to prove
// an already-flushed group is never emitted twice.
func (c *logsToSpansConnector) flushGroup(key string, group *logGroup) {
	c.mu.Lock()
	if c.stopped || group.flushed {
		c.mu.Unlock()
		return
	}
	group.flushed = true

	// Only the group that still owns the key may leave the map. A group
	// replaced by max_logs_per_trace must not delete the replacement.
	if current, ok := c.groups[key]; ok && current == group {
		delete(c.groups, key)
		if group.elem != nil {
			c.lru.Remove(group.elem)
			group.elem = nil
		}
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

	traceID, spanID := extractTraceContext(lr, cfg)

	return &logRecord{
		timestamp: ts,
		duration:  dur,
		body:      valueToString(lr.Body()),
		severity:  lr.SeverityText(),
		traceID:   traceID,
		spanID:    spanID,
	}
}

// extractTraceContext returns the originating trace and span IDs carried by a
// log record, or zero values when it carries none. Precedence for each ID is
// the record-level trace context first (a receiver, OTLP-native application or
// trace_parser operator sets it directly), then the configured trace_id_keys /
// span_id_keys attributes in order. The two lookups are independent: a
// record-level trace ID can be paired with a span ID from an attribute.
func extractTraceContext(lr plog.LogRecord, cfg *Config) (pcommon.TraceID, pcommon.SpanID) {
	traceID := lr.TraceID()
	if traceID.IsEmpty() {
		for _, key := range cfg.TraceIDKeys {
			if v, ok := lr.Attributes().Get(key); ok {
				if id, ok := parseTraceIDValue(v); ok {
					traceID = id
					break
				}
			}
		}
	}

	spanID := lr.SpanID()
	if spanID.IsEmpty() {
		for _, key := range cfg.SpanIDKeys {
			if v, ok := lr.Attributes().Get(key); ok {
				if id, ok := parseSpanIDValue(v); ok {
					spanID = id
					break
				}
			}
		}
	}

	return traceID, spanID
}

// decodeIDBytes returns the raw bytes of a trace or span ID held in an
// attribute value, accepting a hex string or raw bytes of exactly size bytes.
// It rejects anything else, including a wrong length and malformed hex.
func decodeIDBytes(v pcommon.Value, size int) ([]byte, bool) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		b, err := hex.DecodeString(strings.TrimSpace(v.Str()))
		if err != nil || len(b) != size {
			return nil, false
		}
		return b, true
	case pcommon.ValueTypeBytes:
		b := v.Bytes().AsRaw()
		if len(b) != size {
			return nil, false
		}
		return b, true
	default:
		return nil, false
	}
}

// parseTraceIDValue decodes a 16-byte trace ID from an attribute value. An
// invalid or all-zero ID is rejected so the record is treated as having no
// trace context instead of linking to nowhere.
func parseTraceIDValue(v pcommon.Value) (pcommon.TraceID, bool) {
	b, ok := decodeIDBytes(v, len(pcommon.TraceID{}))
	if !ok {
		return pcommon.TraceID{}, false
	}
	var id pcommon.TraceID
	copy(id[:], b)
	if id.IsEmpty() {
		return pcommon.TraceID{}, false
	}
	return id, true
}

// parseSpanIDValue decodes an 8-byte span ID from an attribute value. It has the
// same acceptance rules as parseTraceIDValue.
func parseSpanIDValue(v pcommon.Value) (pcommon.SpanID, bool) {
	b, ok := decodeIDBytes(v, len(pcommon.SpanID{}))
	if !ok {
		return pcommon.SpanID{}, false
	}
	var id pcommon.SpanID
	copy(id[:], b)
	if id.IsEmpty() {
		return pcommon.SpanID{}, false
	}
	return id, true
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

// copyResourceAttributes deep-copies a source resource attribute set. The
// result is safe to keep after ConsumeLogs returns; the upstream pdata it came
// from is not (the connector declares Capabilities{MutatesData: false}).
func copyResourceAttributes(resource pcommon.Map) pcommon.Map {
	out := pcommon.NewMap()
	resource.CopyTo(out)
	return out
}

// buildMapKey returns the key used in the internal groups map. Without
// group_by_resource_attributes it is exactly the extracted key. With it, the
// configured resource attribute values are appended so the same extracted
// value from different sources forms separate groups. Every component is
// length-prefixed, so distinct scopes can never collide, even when a value
// contains the separator character. A resource that is missing one of the
// configured attributes contributes an empty component and therefore collapses
// with other resources that are also missing it.
func (c *logsToSpansConnector) buildMapKey(key string, resource pcommon.Map) string {
	if len(c.config.GroupByResourceAttributes) == 0 {
		return key
	}

	var b strings.Builder
	appendComponent := func(s string) {
		b.WriteString(strconv.Itoa(len(s)))
		b.WriteByte(':')
		b.WriteString(s)
	}

	appendComponent(key)
	for _, name := range c.config.GroupByResourceAttributes {
		value := ""
		if v, ok := resource.Get(name); ok {
			value = valueToString(v)
		}
		appendComponent(value)
	}
	return b.String()
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
	attrs := rs.Resource().Attributes()
	if c.config.CopyResourceAttributes {
		group.resource.CopyTo(attrs)
	}
	// service.name precedence: an explicit service_name always wins; otherwise
	// the source service.name is preserved, and defaultServiceName is used only
	// when the source has none. This keeps the connector from inventing an
	// identity over a real one, while still guaranteeing every trace has a
	// service.name.
	switch {
	case c.config.ServiceName != "":
		attrs.PutStr("service.name", c.config.ServiceName)
	default:
		if v, ok := attrs.Get("service.name"); !ok || v.Str() == "" {
			attrs.PutStr("service.name", defaultServiceName)
		}
	}
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

		span.SetName(c.renderSpanName(rec))
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(rec.timestamp))
		span.SetKind(ptrace.SpanKindInternal)

		var endTime time.Time
		switch {
		case rec.duration > 0:
			endTime = rec.timestamp.Add(rec.duration)
		case i < len(group.records)-1:
			endTime = group.records[i+1].timestamp
		default:
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

		// Link back to the trace the log record came from, when it carried trace
		// context. This is independent of the max_logs_per_trace chain link
		// below: the span is part of the generated trace and also points at its
		// origin. A record-level or attribute trace ID is enough for the link;
		// the span ID is zero when the record carried none.
		if !rec.traceID.IsEmpty() {
			link := span.Links().AppendEmpty()
			link.SetTraceID(rec.traceID)
			link.SetSpanID(rec.spanID)
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

	c.telemetry.ConnectorLogsToSpansTracesCreated.Add(ctx, 1)

	if err := c.tracesConsumer.ConsumeTraces(ctx, td); err != nil {
		c.logger.Error("failed to consume traces", zap.Error(err))
	}
}

// renderSpanName expands the configured span_name_template for one record. A
// template that renders to an empty string (for example {severity} on a record
// with no severity text) falls back to the full log body, because
// OpenTelemetry requires a span name and the body is always the most
// informative thing the connector has.
func (c *logsToSpansConnector) renderSpanName(rec *logRecord) string {
	name := c.spanName.render(rec)
	if name == "" {
		return rec.body
	}
	return name
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
