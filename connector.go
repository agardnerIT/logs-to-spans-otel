// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs_to_spans

import (
	"container/heap"
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/agardnerIT/logs-to-spans-otel/internal/metadata"
)

// Bounds and timeouts for the export path. None of these are user-visible
// configuration: they exist so a stalled downstream cannot grow the connector's
// memory or shutdown time without bound.
const (
	// maxGroupsPerExport caps how many groups go into a single downstream
	// ConsumeTraces call. Each group becomes one ResourceSpans, so the limit
	// bounds the pdata built for one call while still collapsing hundreds of
	// groups into a handful of exports at shutdown.
	maxGroupsPerExport = 100
	// exportQueueSize is how many pending export batches the reaper may hand to
	// the export workers before it exports inline itself.
	exportQueueSize = 64
	// exportWorkerCount is the size of the export pool. The reaper never exports
	// on its own goroutine, so a slow exporter cannot stop deadlines from being
	// enforced.
	exportWorkerCount = 2
	// exportTimeoutDefault bounds one downstream ConsumeTraces call (or one chunk
	// of a shutdown flush), so a wedged exporter cannot pin a worker forever.
	exportTimeoutDefault = 10 * time.Second
	// shutdownWaitTimeout bounds how long Shutdown waits for the reaper and the
	// export workers. Exceeding it is reported, not ignored.
	shutdownWaitTimeout = 10 * time.Second
	// recordOverheadBytes is the per-record bookkeeping cost (struct, slices,
	// timestamps) counted against max_group_bytes in addition to the body, so
	// the byte bound is not defeated by many tiny records.
	recordOverheadBytes = 128
	// fullBody is the extractOptions bodyLimit that keeps the whole body.
	fullBody = -1
)

// Drop reasons reported through the records_dropped counter. They are part of
// the metric contract (metadata.yaml enumerates them), so they are constants.
const (
	dropReasonStopped         = "stopped"
	dropReasonDownstreamError = "downstream_error"
	dropReasonShutdown        = "shutdown"
)

// defaultSpanNameFallback is used when the configured template renders empty
// and the record has no body either. OpenTelemetry requires a non-empty span
// name.
const defaultSpanNameFallback = "log record"

// errConnectorStopped is returned by ConsumeLogs after Shutdown. The records
// were not accepted, so the caller must not treat the batch as consumed.
var errConnectorStopped = errors.New("logs_to_spans connector is shut down")

type logsToSpansConnector struct {
	config         *Config
	logger         *zap.Logger
	tracesConsumer consumer.Traces
	compiledRegex  []*regexp.Regexp
	spanName       *spanNameTemplate
	extractOpts    extractOptions
	telemetry      *metadata.TelemetryBuilder
	// ids draws trace and span IDs from a generator seeded once from
	// crypto/rand. See idGenerator.
	ids *idGenerator
	// exportTimeout bounds one downstream ConsumeTraces call. It is a field so
	// tests can shorten it; the factory sets exportTimeoutDefault.
	exportTimeout time.Duration
	// shutdownWait bounds how long Shutdown waits for the reaper and the export
	// workers. Like exportTimeout it is a field so tests can shorten it.
	shutdownWait time.Duration

	// mu guards every field below plus the groups map. The per-record path only
	// takes it for the O(1) map/list/heap splice; deadline checks, resource
	// copying and eviction selection no longer run under it.
	mu     sync.Mutex
	groups map[string]*logGroup
	lru    *list.List // container/list of *logGroup, front = most recently updated
	// deadlines is a min-heap of every live group keyed by its absolute flush
	// deadline. A reaper tick pops only the groups that are actually due, so the
	// expiry cost is O(expired × log(groups)) instead of O(groups) per tick.
	deadlines deadlineHeap
	stopped   bool

	// reaperStop/reaperDone are the lifecycle handles of the single background
	// reaper goroutine. Both are nil after Shutdown stops it. Guarded by mu,
	// which makes starting during a concurrent Shutdown impossible.
	reaperStop chan struct{}
	reaperDone chan struct{}

	// exportCh is the queue the reaper hands expired groups to. It is drained by
	// exportWorkerCount goroutines, so a slow exporter stalls neither deadline
	// enforcement nor Shutdown. Enqueues happen under mu (never blocking) and the
	// channel is closed by Shutdown after the reaper has stopped, so a send on a
	// closed channel is impossible.
	exportCh     chan exportBatch
	workers      sync.WaitGroup
	workersStart bool
}

type logGroup struct {
	key string
	// mapKey is the fully scoped key used in the groups map (the extracted key
	// plus any group_by_resource_attributes components). It is kept so the LRU
	// list and the deadline heap can retire a group without a map scan.
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
	// bytes is the retained body bytes of records plus a per-record overhead. It
	// bounds the group against max_group_bytes.
	bytes int
	// elem is this group's node in the connector LRU list.
	elem *list.Element
	// heapIdx is this group's index in the deadline heap, or -1 when the group is
	// not in it.
	heapIdx int
	// firstAdded is when the group (or its post-split replacement) first
	// received a record. It caps the group's absolute lifetime at max_wait.
	firstAdded time.Time
	// lastUpdated is the time the most recent record was added. It drives the
	// inactivity timeout and, by recency order, max_groups eviction.
	lastUpdated time.Time
	// deadline is the absolute time at which the group must be flushed:
	// min(lastUpdated+timeout, firstAdded+max_wait). The reaper only looks at
	// groups whose deadline has passed.
	deadline time.Time
	// flushed guards against a group being emitted twice. A group is stamped
	// flushed while still under the connector mutex, before it leaves the map.
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
	// stampedNow records that the record carried neither a timestamp nor an
	// observed timestamp, so the connector substituted the current time.
	stampedNow bool
	// truncated records that the body was cut at max_record_bytes.
	truncated bool
	// invalidDuration records that a duration_keys attribute was present but
	// could not be used (overflow, unparsable, or above max_span_duration).
	invalidDuration bool
}

// extractOptions is the precomputed set of extraction decisions that depend on
// the compiled span name template and the config rather than on a record.
type extractOptions struct {
	// bodyLimit is the number of body runes to keep: fullBody for the whole
	// body, 0 when the connector will never read the body, or a positive rune
	// count for a bounded body.
	bodyLimit int
}

// exportBatch is one unit of work handed to the export workers: a set of
// retired groups plus the drop reason to report if the downstream export
// fails.
type exportBatch struct {
	groups []*logGroup
	reason string
}

// deadlineHeap is a min-heap of groups keyed by logGroup.deadline. Ties are
// broken by mapKey so expiry order is deterministic.
type deadlineHeap []*logGroup

func (h deadlineHeap) Len() int { return len(h) }

func (h deadlineHeap) Less(i, j int) bool {
	if h[i].deadline.Equal(h[j].deadline) {
		return h[i].mapKey < h[j].mapKey
	}
	return h[i].deadline.Before(h[j].deadline)
}

func (h deadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx = i
	h[j].heapIdx = j
}

func (h *deadlineHeap) Push(x any) {
	group := x.(*logGroup)
	group.heapIdx = len(*h)
	*h = append(*h, group)
}

func (h *deadlineHeap) Pop() any {
	old := *h
	n := len(old)
	group := old[n-1]
	old[n-1] = nil
	group.heapIdx = -1
	*h = old[:n-1]
	return group
}

// idGenerator draws trace and span IDs from a ChaCha8 generator seeded once
// from crypto/rand, in bulk.
//
// Two properties matter. First, a crypto/rand failure is fatal at construction
// (the factory returns the error and the component does not start) instead of
// silently producing all-zero IDs, which pcommon rejects and exporters drop.
// Second, one bulk read produces every ID for a batch of groups, so emitting a
// trace costs one generator call instead of one call per span. ChaCha8 is not
// goroutine-safe, so the mutex serializes the (very rare) bulk reads.
type idGenerator struct {
	mu  sync.Mutex
	rng *mathrand.ChaCha8
}

func newIDGenerator() (*idGenerator, error) {
	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil, fmt.Errorf("seeding trace ID generator from crypto/rand: %w", err)
	}
	return &idGenerator{rng: mathrand.NewChaCha8(seed)}, nil
}

// newBulk draws traceIDs 16-byte trace IDs and spanIDs 8-byte span IDs in one
// call. The returned cursor hands them out without further allocation.
func (g *idGenerator) newBulk(traceIDs, spanIDs int) *bulkIDs {
	buf := make([]byte, traceIDs*len(pcommon.TraceID{})+spanIDs*len(pcommon.SpanID{}))
	g.mu.Lock()
	// ChaCha8.Read never returns an error and always fills p.
	_, _ = g.rng.Read(buf)
	g.mu.Unlock()
	return &bulkIDs{buf: buf}
}

// bulkIDs is a pre-drawn set of IDs handed out in order.
type bulkIDs struct {
	buf []byte
	off int
}

func (b *bulkIDs) next(n int) []byte {
	if len(b.buf)-b.off < n {
		// Unreachable: callers size the bulk read with the same counts they
		// consume. A panic here is a programming error, not runtime input.
		panic("logs_to_spans: bulk ID buffer underrun")
	}
	raw := b.buf[b.off : b.off+n]
	b.off += n
	return raw
}

func (b *bulkIDs) traceID() pcommon.TraceID {
	var id pcommon.TraceID
	copy(id[:], b.next(len(id)))
	return id
}

func (b *bulkIDs) spanID() pcommon.SpanID {
	var id pcommon.SpanID
	copy(id[:], b.next(len(id)))
	return id
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
	c.startBackgroundLocked()
	return nil
}

// ConsumeLogs groups a batch's log records by extracted key and buffers them.
// It returns an error when records were accepted but could not be turned into
// spans (a downstream export failure, or a connector that is shutting down), so
// upstream receivers can retry instead of treating a silent loss as success.
func (c *logsToSpansConnector) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	var errs []error

	for i := range ld.ResourceLogs().Len() {
		rl := ld.ResourceLogs().At(i)
		resource := rl.Resource().Attributes()

		for j := range rl.ScopeLogs().Len() {
			sl := rl.ScopeLogs().At(j)

			batches, ingested, unmatched := c.collectBatches(sl)
			// Instrumentation is per batch, not per record: an Int64Counter.Add is
			// a lock/atomic + measurement path, and doing it 100 times for a
			// 100-record batch is pure overhead.
			if ingested > 0 {
				c.telemetry.ConnectorLogsToSpansLogsIngested.Add(ctx, int64(ingested))
			}
			if unmatched > 0 {
				c.telemetry.ConnectorLogsToSpansUnmatchedDropped.Add(ctx, int64(unmatched))
			}
			if len(batches) == 0 {
				continue
			}

			var pending []*logGroup
			for _, batch := range batches {
				flushed, evicted, err := c.addRecords(batch.key, resource, batch.records)
				pending = append(pending, flushed...)
				if evicted != nil {
					pending = append(pending, evicted)
				}
				if err != nil {
					errs = append(errs, err)
				}
			}

			// One ConsumeTraces call for every group this scope flushed, rather
			// than one per group.
			if len(pending) > 0 {
				if err := c.exportGroups(ctx, dropReasonDownstreamError, pending); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}

	return errors.Join(errs...)
}

// recordBatch is the set of records in one scope that share an extracted key,
// in arrival order.
type recordBatch struct {
	key     string
	records []*logRecord
}

// linearBatchLimit is how many distinct keys batchSet keeps in a plain slice
// before switching to a map. Realistically a scope has 1-5 keys, and for those
// a linear scan beats allocating a map per scope.
const linearBatchLimit = 8

// batchSet accumulates records per extracted key, preserving first-seen order.
type batchSet struct {
	batches []recordBatch
	index   map[string]int
}

func (s *batchSet) add(key string, rec *logRecord) {
	if s.index == nil {
		if len(s.batches) < linearBatchLimit {
			for i := range s.batches {
				if s.batches[i].key == key {
					s.batches[i].records = append(s.batches[i].records, rec)
					return
				}
			}
			s.batches = append(s.batches, recordBatch{key: key, records: []*logRecord{rec}})
			return
		}
		s.index = make(map[string]int, len(s.batches)+1)
		for i := range s.batches {
			s.index[s.batches[i].key] = i
		}
	}
	if i, ok := s.index[key]; ok {
		s.batches[i].records = append(s.batches[i].records, rec)
		return
	}
	s.index[key] = len(s.batches)
	s.batches = append(s.batches, recordBatch{key: key, records: []*logRecord{rec}})
}

// collectBatches extracts every record in a scope outside the connector mutex
// and returns the resulting batches plus the ingested/unmatched counts the
// caller reports as metrics.
func (c *logsToSpansConnector) collectBatches(sl plog.ScopeLogs) (batches []recordBatch, ingested, unmatched int) {
	set := batchSet{}
	var counters logRecordCounters

	for k := range sl.LogRecords().Len() {
		lr := sl.LogRecords().At(k)
		ingested++
		key := c.extractGroupKey(lr)
		if key == "" {
			unmatched++
			continue
		}
		// Extraction is the dominant per-record cost (for a Map body it
		// JSON-serializes the value), so it runs before the lock. The result is
		// an immutable copy: plog values are views into upstream-owned pdata and
		// must not be retained past ConsumeLogs (MutatesData: false).
		rec := extractLogRecord(lr, c.config, c.extractOpts)
		counters.add(rec)
		set.add(key, rec)
	}

	c.reportRecordCounters(context.Background(), counters)
	return set.batches, ingested, unmatched
}

// logRecordCounters counts the per-record substitutions extraction made, so one
// metric update covers a whole batch.
type logRecordCounters struct {
	stampedNow      int
	truncated       int
	invalidDuration int
}

func (n *logRecordCounters) add(rec *logRecord) {
	if rec.stampedNow {
		n.stampedNow++
	}
	if rec.truncated {
		n.truncated++
	}
	if rec.invalidDuration {
		n.invalidDuration++
	}
}

func (c *logsToSpansConnector) reportRecordCounters(ctx context.Context, n logRecordCounters) {
	if n.stampedNow > 0 {
		c.telemetry.ConnectorLogsToSpansRecordsWithoutTimestamp.Add(ctx, int64(n.stampedNow))
	}
	if n.truncated > 0 {
		c.telemetry.ConnectorLogsToSpansRecordsTruncated.Add(ctx, int64(n.truncated))
	}
	if n.invalidDuration > 0 {
		c.telemetry.ConnectorLogsToSpansInvalidDurations.Add(ctx, int64(n.invalidDuration))
	}
}

// Shutdown flushes every buffered group, stops the reaper and drains the export
// workers. Every wait is bounded: a wedged downstream cannot hang the collector
// forever, and anything that is dropped is counted and logged instead of being
// acknowledged silently.
func (c *logsToSpansConnector) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
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
	c.deadlines = nil
	workersStarted := c.workersStart
	c.workersStart = false
	c.mu.Unlock()

	var errs []error

	// Stop the reaper before flushing what is left, so no group can be emitted
	// twice and no goroutine survives Shutdown. The wait is outside the mutex: a
	// reaper mid-scan may still hold it.
	waitTimeout := c.shutdownWait
	if waitTimeout <= 0 {
		waitTimeout = shutdownWaitTimeout
	}

	if stop != nil {
		close(stop)
		if !waitFor(done, waitTimeout) {
			errs = append(errs, errors.New("logs_to_spans: background reaper did not stop within shutdown timeout"))
		}
	}

	// No enqueue can happen any more (stopped is set and enqueueing requires the
	// mutex), so closing the queue is safe. Closing drains it: the workers exit
	// once every queued batch has been exported.
	if workersStarted {
		close(c.exportCh)
		workersDone := make(chan struct{})
		go func() {
			c.workers.Wait()
			close(workersDone)
		}()
		if !waitFor(workersDone, waitTimeout) {
			errs = append(errs, errors.New("logs_to_spans: export workers did not stop within shutdown timeout"))
		}
	}

	// The shutdown context may already be cancelled (the collector shutdown
	// budget is spent elsewhere). Detach from cancellation but keep the values,
	// and bound the flush ourselves, so buffered records are still delivered.
	flushTimeout := c.exportTimeout
	if flushTimeout <= 0 {
		flushTimeout = exportTimeoutDefault
	}
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), flushTimeout)
	// The flush runs on its own goroutine so a consumer that ignores its context
	// (a wedged exporter) cannot hang Shutdown past the wait budget. If it does
	// overrun, the loss is reported rather than hidden; the count is not touched
	// because the export may still complete once the downstream unblocks.
	flushDone := make(chan error, 1)
	go func() { flushDone <- c.exportGroups(flushCtx, dropReasonShutdown, groups) }()
	select {
	case err := <-flushDone:
		errs = append(errs, err)
	case <-time.After(waitTimeout):
		errs = append(errs, errors.New("logs_to_spans: shutdown flush did not complete within shutdown timeout"))
	}
	cancel()

	// Unregister the active_groups observable gauge callback so the meter does
	// not retain the connector after shutdown.
	c.telemetry.Shutdown()
	return errors.Join(errs...)
}

// waitFor waits for done to close and reports whether it did so in time.
func waitFor(done <-chan struct{}, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// addToGroup buffers a single record. It is the entry point the tests drive;
// the hot path is ConsumeLogs, which batches a scope's records into one call to
// addRecords. Its errors are surfaced through the drop counters like any other
// caller's.
func (c *logsToSpansConnector) addToGroup(key string, resource pcommon.Map, rec *logRecord) {
	flushed, evicted, err := c.addRecords(key, resource, []*logRecord{rec})
	pending := append(flushed, evicted) //nolint:gocritic // evicted may be nil; append handles it
	if len(pending) > 0 {
		if exportErr := c.exportGroups(context.Background(), dropReasonDownstreamError, pending); exportErr != nil {
			err = errors.Join(err, exportErr)
		}
	}
	if err != nil {
		c.logger.Warn("failed to buffer log record", zap.Error(err))
	}
}

// addRecords buffers every record in recs under one acquisition of c.mu. recs
// all share the extracted key and resource, and are appended in order.
//
// The caller supplies the source resource: it is deep-copied here, under the
// mutex, only when a new group is created and only when the copy will be used.
// In steady state - one long-lived group per key - no copy happens at all.
//
// It returns the groups that a max_logs_per_trace/max_group_bytes split or a
// max_groups eviction retired, so the caller can export them after releasing
// the mutex, and an error when the records were not buffered (connector already
// shut down).
func (c *logsToSpansConnector) addRecords(key string, resource pcommon.Map, recs []*logRecord) (flushed []*logGroup, evicted *logGroup, err error) {
	if len(recs) == 0 {
		return nil, nil, nil
	}
	// The key comes from log input, so cap it here — the single point every
	// caller's key passes through — before it can become a map key, an LRU or
	// heap entry, or the group.key attribute echoed on every emitted span.
	key = c.boundGroupKey(key)
	mapKey := c.buildMapKey(key, resource)
	now := time.Now()

	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		c.dropRecords(context.Background(), dropReasonStopped, len(recs))
		return nil, nil, errConnectorStopped
	}
	c.startBackgroundLocked()

	group, exists := c.groups[mapKey]
	if !exists {
		// Bound the number of live groups. The LRU list makes this O(1): a
		// high-cardinality key (a request or connection ID, a raw trace ID)
		// otherwise grows the map without bound until the timeout fires.
		if c.config.MaxGroups > 0 && len(c.groups) >= c.config.MaxGroups {
			evicted = c.evictLeastRecentlyUsedLocked()
		}
		group = &logGroup{key: key, mapKey: mapKey, firstAdded: now, lastUpdated: now, heapIdx: -1}
		// The copy is only taken when it will be used and only for a brand-new
		// group: in steady state (one long-lived group per key) no resource copy
		// happens at all. The zero map keeps CopyTo safe when copying is off.
		group.resource = pcommon.NewMap()
		if c.config.CopyResourceAttributes {
			group.resource = copyResourceAttributes(resource)
		}
		group.elem = c.lru.PushFront(group)
		c.groups[mapKey] = group
		c.stampDeadlineLocked(group, now)
		heap.Push(&c.deadlines, group)
	}

	for _, rec := range recs {
		if full := c.addRecordLocked(mapKey, group, rec, now); full != nil {
			flushed = append(flushed, full)
			// A split installed a linked replacement under the same key; the rest
			// of the batch goes into it.
			group = c.groups[mapKey]
		}
	}
	c.mu.Unlock()

	if evicted != nil {
		c.telemetry.ConnectorLogsToSpansGroupsEvicted.Add(context.Background(), 1)
		c.logger.Debug("evicted least recently updated group: max_groups reached",
			zap.String("group_key", evicted.key),
			zap.Int("max_groups", c.config.MaxGroups),
			zap.Int("log_count", len(evicted.records)),
		)
	}

	return flushed, evicted, nil
}

// addRecordLocked appends rec to group, stamps its recency and keeps the LRU
// order and deadline heap in sync. When the group reaches max_logs_per_trace or
// max_group_bytes it is retired and replaced in place, and the retired group is
// returned for the caller to emit after it releases c.mu. The caller must hold
// c.mu.
func (c *logsToSpansConnector) addRecordLocked(mapKey string, group *logGroup, rec *logRecord, now time.Time) *logGroup {
	group.records = append(group.records, rec)
	group.bytes += len(rec.body) + len(rec.severity) + recordOverheadBytes
	group.lastUpdated = now
	if group.elem != nil {
		c.lru.MoveToFront(group.elem)
	}

	if !c.groupFullLocked(group) {
		c.stampDeadlineLocked(group, now)
		heap.Fix(&c.deadlines, group.heapIdx)
		return nil
	}

	// One bulk read draws the trace ID the retired group will carry and the span
	// ID its last span will use, so the replacement can link back to it.
	ids := c.ids.newBulk(1, 1)
	traceID := ids.traceID()
	lastSpanID := ids.spanID()

	// The retired group becomes the emitted trace, so it owns the ids the next
	// group links back to.
	group.traceID = traceID
	group.lastSpanID = lastSpanID
	// Stamp the outgoing group flushed and take it out of every index before the
	// replacement takes its place: nothing else can emit it now.
	c.retireGroupLocked(group)

	// The replacement inherits the group's resource: a split must not change the
	// resource of the records that follow. Its max_wait restarts here, as the
	// previous per-group timer did.
	replacement := &logGroup{
		key:         group.key,
		mapKey:      mapKey,
		resource:    group.resource,
		prevTraceID: traceID,
		prevSpanID:  lastSpanID,
		firstAdded:  now,
		lastUpdated: now,
		heapIdx:     -1,
	}
	replacement.elem = c.lru.PushFront(replacement)
	c.groups[mapKey] = replacement
	c.stampDeadlineLocked(replacement, now)
	heap.Push(&c.deadlines, replacement)

	return group
}

// groupFullLocked reports whether the group has reached a size bound. A single
// record that alone exceeds max_group_bytes keeps its own group: the bound moves
// the trace boundary, it never drops data.
func (c *logsToSpansConnector) groupFullLocked(group *logGroup) bool {
	if c.config.MaxLogsPerTrace > 0 && len(group.records) >= c.config.MaxLogsPerTrace {
		return true
	}
	return c.config.MaxGroupBytes > 0 && group.bytes > c.config.MaxGroupBytes && len(group.records) > 1
}

// stampDeadlineLocked recomputes the group's absolute flush deadline:
// min(lastUpdated+timeout, firstAdded+max_wait).
func (c *logsToSpansConnector) stampDeadlineLocked(group *logGroup, _ time.Time) {
	group.deadline = group.lastUpdated.Add(c.config.Timeout)
	if maxWaitDeadline := group.firstAdded.Add(c.config.MaxWait); maxWaitDeadline.Before(group.deadline) {
		group.deadline = maxWaitDeadline
	}
}

// retireGroupLocked removes a group from the map, the LRU list and the deadline
// heap, and stamps it flushed so nothing emits it twice. The caller must hold
// c.mu.
func (c *logsToSpansConnector) retireGroupLocked(group *logGroup) {
	group.flushed = true
	delete(c.groups, group.mapKey)
	if group.elem != nil {
		c.lru.Remove(group.elem)
		group.elem = nil
	}
	if group.heapIdx >= 0 {
		heap.Remove(&c.deadlines, group.heapIdx)
	}
}

// startBackgroundLocked starts the reaper and the export workers if they are
// not already running. The caller must hold c.mu and must have checked
// !c.stopped, so a concurrent Shutdown cannot set stopped and miss them.
func (c *logsToSpansConnector) startBackgroundLocked() {
	if !c.workersStart {
		c.workersStart = true
		for range exportWorkerCount {
			c.workers.Add(1)
			go c.exportWorker()
		}
	}
	if c.reaperStop != nil {
		return
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	c.reaperStop = stop
	c.reaperDone = done
	go c.reapLoop(stop, done)
}

// exportWorker drains the export queue until Shutdown closes it. It never takes
// c.mu, so a slow downstream stalls only this goroutine.
func (c *logsToSpansConnector) exportWorker() {
	defer c.workers.Done()
	for batch := range c.exportCh {
		// The reaper cannot report an error to a caller, so the failure is
		// counted and logged (exportChunk does both).
		if err := c.exportGroups(context.Background(), batch.reason, batch.groups); err != nil {
			c.logger.Warn("failed to export reaped log groups", zap.Int("groups", len(batch.groups)), zap.Error(err))
		}
	}
}

// reapLoop is the connector's only timing goroutine. One tick pops the groups
// whose deadline has passed instead of two time.AfterFunc allocations per log
// record, and hands them to the export workers instead of exporting inline.
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
// is honored promptly, coarse enough that the tick is negligible against the
// default five-second timeout. A group can therefore be flushed up to one tick
// after its deadline; max_wait still caps a group's absolute lifetime.
func (c *logsToSpansConnector) reapInterval() time.Duration {
	interval := min(c.config.Timeout, c.config.MaxWait)
	interval /= 4
	interval = max(interval, 10*time.Millisecond)
	interval = min(interval, time.Second)
	return interval
}

// reapExpired flushes every group whose deadline has passed. Popping the
// deadline heap is O(expired × log(groups)), so a tick that finds nothing costs
// one comparison rather than a scan of every group.
func (c *logsToSpansConnector) reapExpired() {
	now := time.Now()

	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	var expired []*logGroup
	for len(c.deadlines) > 0 {
		next := c.deadlines[0]
		if next.deadline.After(now) {
			break
		}
		heap.Pop(&c.deadlines)
		if next.flushed {
			// Defensive: every removal path also removes the heap entry.
			continue
		}
		c.retireGroupLocked(next)
		expired = append(expired, next)
	}
	leftover := c.enqueueLocked(expired)
	c.mu.Unlock()

	// Only when the export queue is already full (a sustained downstream stall)
	// does the reaper export inline. Deadlines are still enforced for every other
	// tick, and this is far better than an unbounded queue.
	if len(leftover) > 0 {
		if err := c.exportGroups(context.Background(), dropReasonDownstreamError, leftover); err != nil {
			c.logger.Warn("failed to export reaped log groups", zap.Int("groups", len(leftover)), zap.Error(err))
		}
	}
}

// enqueueLocked hands groups to the export workers without blocking. The caller
// must hold c.mu. Whatever does not fit is returned for the caller to export
// inline after unlocking.
func (c *logsToSpansConnector) enqueueLocked(groups []*logGroup) []*logGroup {
	if len(groups) == 0 {
		return nil
	}
	select {
	case c.exportCh <- exportBatch{groups: groups, reason: dropReasonDownstreamError}:
		return nil
	default:
		return groups
	}
}

// evictLeastRecentlyUsedLocked retires and returns the least recently updated
// group so a new group can take its place. The caller must hold c.mu.
func (c *logsToSpansConnector) evictLeastRecentlyUsedLocked() *logGroup {
	elem := c.lru.Back()
	if elem == nil {
		return nil
	}
	victim := elem.Value.(*logGroup)
	c.retireGroupLocked(victim)
	return victim
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
	// replaced by a split must not delete the replacement.
	if current, ok := c.groups[key]; ok && current == group {
		delete(c.groups, key)
		if group.elem != nil {
			c.lru.Remove(group.elem)
			group.elem = nil
		}
		if group.heapIdx >= 0 {
			heap.Remove(&c.deadlines, group.heapIdx)
		}
	}
	c.mu.Unlock()
	if err := c.exportGroups(context.Background(), dropReasonDownstreamError, []*logGroup{group}); err != nil {
		c.logger.Warn("failed to export flushed group", zap.Error(err))
	}
}

// exportGroups emits groups as traces in chunks of maxGroupsPerExport. Every
// call is bounded by exportTimeout and detached from the caller's cancellation
// where the records are already out of the buffer, so a cancelled shutdown
// context cannot silently discard them.
func (c *logsToSpansConnector) exportGroups(ctx context.Context, reason string, groups []*logGroup) error {
	groups = slices.DeleteFunc(groups, func(g *logGroup) bool { return g == nil || len(g.records) == 0 })
	if len(groups) == 0 {
		return nil
	}

	timeout := c.exportTimeout
	if timeout <= 0 {
		timeout = exportTimeoutDefault
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	var errs []error
	for start := 0; start < len(groups); start += maxGroupsPerExport {
		chunk := groups[start:min(start+maxGroupsPerExport, len(groups))]
		if err := c.exportChunk(ctx, reason, chunk); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// exportChunk builds one ptrace.Traces holding every group in the chunk (one
// ResourceSpans each) and hands it to the downstream consumer once.
func (c *logsToSpansConnector) exportChunk(ctx context.Context, reason string, groups []*logGroup) error {
	td := ptrace.NewTraces()
	ids := c.ids.newBulk(countTraceIDs(groups), countSpanIDs(groups))
	for _, group := range groups {
		c.appendGroupSpans(td, group, ids)
	}

	c.telemetry.ConnectorLogsToSpansTracesCreated.Add(ctx, int64(len(groups)))

	if err := c.tracesConsumer.ConsumeTraces(ctx, td); err != nil {
		dropped := 0
		for _, group := range groups {
			dropped += len(group.records)
		}
		c.dropRecords(ctx, reason, dropped)
		c.logger.Warn("failed to export generated traces: records dropped",
			zap.String("reason", reason),
			zap.Int("groups", len(groups)),
			zap.Int("records", dropped),
			zap.Error(err),
		)
		return fmt.Errorf("exporting %d generated trace(s): %w", len(groups), err)
	}
	return nil
}

// countTraceIDs and countSpanIDs size the bulk ID read for a chunk: a group
// whose traceID was already assigned by a split reuses it, and a group whose
// last span ID was assigned by a split reuses that too.
func countTraceIDs(groups []*logGroup) int {
	n := 0
	for _, group := range groups {
		if group.traceID.IsEmpty() {
			n++
		}
	}
	return n
}

func countSpanIDs(groups []*logGroup) int {
	n := 0
	for _, group := range groups {
		n += len(group.records)
		if !group.lastSpanID.IsEmpty() {
			n--
		}
	}
	return n
}

// appendGroupSpans converts one group into a ResourceSpans appended to td.
func (c *logsToSpansConnector) appendGroupSpans(td ptrace.Traces, group *logGroup, ids *bulkIDs) {
	// Stable: records that share a timestamp keep the order they arrived in.
	// Equal timestamps are routine (a receiver stamping a batch with the same
	// clock tick, or a coarse ObservedTimestamp), and an unstable sort would
	// reorder them arbitrarily between runs, scrambling a trace's span chain.
	if !slices.IsSortedFunc(group.records, func(a, b *logRecord) int {
		return a.timestamp.Compare(b.timestamp)
	}) {
		slices.SortStableFunc(group.records, func(a, b *logRecord) int {
			return a.timestamp.Compare(b.timestamp)
		})
	}

	traceID := group.traceID
	if traceID.IsEmpty() {
		traceID = ids.traceID()
	}

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

	last := len(group.records) - 1
	for i, rec := range group.records {
		span := ss.Spans().AppendEmpty()
		span.SetTraceID(traceID)

		if i == last && !group.lastSpanID.IsEmpty() {
			span.SetSpanID(group.lastSpanID)
		} else {
			span.SetSpanID(ids.spanID())
		}

		span.SetName(c.renderSpanName(rec))
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(rec.timestamp))
		span.SetKind(ptrace.SpanKindInternal)

		var endTime time.Time
		switch {
		case rec.duration > 0:
			endTime = rec.timestamp.Add(rec.duration)
		case i < last:
			endTime = group.records[i+1].timestamp
		default:
			endTime = rec.timestamp.Add(c.config.EndSpanDuration)
		}
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(endTime))

		spanAttrs := span.Attributes()
		// Opt-in: the body is already available through the span name, and
		// duplicating it on every span doubles the payload for no new information.
		if c.config.IncludeBodyAttribute {
			spanAttrs.PutStr("log.body", rec.body)
		}
		if rec.severity != "" {
			spanAttrs.PutStr("log.severity", rec.severity)
		}
		spanAttrs.PutStr("group.key", group.key)

		if i > 0 {
			parentSpanID := ss.Spans().At(i - 1).SpanID()
			span.SetParentSpanID(parentSpanID)
		}

		// Link back to the trace the log record came from, when it carried trace
		// context. This is independent of the max_logs_per_trace chain link
		// below: the span is part of the generated trace and also points at its
		// origin. require_span_id makes a partial link (trace ID only) a no-op,
		// so a forged or truncated attribute cannot produce a half-link.
		if !rec.traceID.IsEmpty() && (!c.config.RequireSpanID || !rec.spanID.IsEmpty()) {
			link := span.Links().AppendEmpty()
			link.SetTraceID(rec.traceID)
			link.SetSpanID(rec.spanID)
		}

		if i == 0 && !group.prevTraceID.IsEmpty() {
			link := span.Links().AppendEmpty()
			link.SetTraceID(group.prevTraceID)
			link.SetSpanID(group.prevSpanID)
		}
	}

	// Debug, not Info: this fires once per emitted trace, and at a few thousand
	// traces per second an Info line here floods the collector's shared logging
	// pipeline and leaks group keys (often a user or session ID) into it.
	c.logger.Debug("converted log group to trace",
		zap.String("group_key", group.key),
		zap.Int("log_count", len(group.records)),
	)
}

// dropRecords counts records that were accepted and then lost. The reason
// attribute makes the loss attributable, and the counter is the durable signal:
// for the reaper path there is no caller left to return an error to.
func (c *logsToSpansConnector) dropRecords(ctx context.Context, reason string, n int) {
	if n <= 0 {
		return
	}
	c.telemetry.ConnectorLogsToSpansRecordsDropped.Add(ctx, int64(n),
		metric.WithAttributes(attribute.String("reason", reason)),
	)
}

// extractLogRecord converts one log record into the immutable form the
// connector buffers. It runs outside the connector mutex.
func extractLogRecord(lr plog.LogRecord, cfg *Config, opts extractOptions) *logRecord {
	rec := &logRecord{
		severity: lr.SeverityText(),
	}

	ts := lr.Timestamp()
	if ts == 0 {
		ts = lr.ObservedTimestamp()
	}
	switch {
	case ts == 0:
		// Neither timestamp is set. pcommon converts a zero timestamp to the Unix
		// epoch, so carrying it through puts the span in 1970 - centuries outside
		// any backend's retention window, and ahead of every other span in the
		// trace. The record was observed by the connector now, so use that.
		rec.timestamp = time.Now()
		rec.stampedNow = true
	case ts > maxSaneTimestamp:
		// pcommon.Timestamp is nanoseconds since the epoch as a *uint64*, but
		// everything that writes one converts from an int64. A value above
		// MaxInt64 therefore came from a negative time.Time (time.Time{} becomes
		// year 1754 this way) and is not a real timestamp. Stamping it would put
		// the span centuries out of range and sort it ahead of its whole trace.
		rec.timestamp = time.Now()
		rec.stampedNow = true
	default:
		rec.timestamp = ts.AsTime()
	}

	for _, key := range cfg.DurationKeys {
		val, ok := lr.Attributes().Get(key)
		if !ok {
			continue
		}
		d, valid := parseDuration(val)
		if !valid || (cfg.MaxSpanDuration > 0 && d > cfg.MaxSpanDuration) {
			rec.invalidDuration = true
			continue
		}
		rec.duration = d
		break
	}

	// The severity text is log input too, so it is bounded by the same
	// max_record_bytes bound as the body. Deliberately not counted in
	// records_truncated: that metric's contract is log bodies (metadata.yaml),
	// and severity truncation is expected to be rare.
	if cfg.MaxRecordBytes > 0 && len(rec.severity) > cfg.MaxRecordBytes {
		rec.severity = truncateBytes(rec.severity, cfg.MaxRecordBytes)
	}

	rec.traceID, rec.spanID = extractTraceContext(lr, cfg)

	body := valueToStringLimit(lr.Body(), opts.bodyLimit)
	if cfg.MaxRecordBytes > 0 && len(body) > cfg.MaxRecordBytes {
		body = truncateBytes(body, cfg.MaxRecordBytes)
		rec.truncated = true
	}
	rec.body = body

	return rec
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
		// Length first: hex.DecodeString allocates len(src)/2 bytes before it
		// validates anything, so decoding first would let one huge attribute
		// value cost a huge transient allocation per record per key.
		s := strings.TrimSpace(v.Str())
		if len(s) != 2*size {
			return nil, false
		}
		b, err := hex.DecodeString(s)
		if err != nil {
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

// parseDuration converts a duration_keys attribute to a time.Duration and
// reports whether the value is usable. It rejects out-of-range input instead of
// letting the conversion wrap: time.Duration is an int64 of nanoseconds, so
// anything above ~292 years overflows, and a wrapped value can be positive but
// tiny, which would silently produce a nonsense span end time.
func parseDuration(v pcommon.Value) (time.Duration, bool) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		d, err := time.ParseDuration(v.Str())
		if err != nil || d <= 0 {
			return 0, false
		}
		return d, true
	case pcommon.ValueTypeInt:
		seconds := v.Int()
		if seconds <= 0 || seconds > maxDurationSeconds {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	case pcommon.ValueTypeDouble:
		seconds := v.Double()
		// NaN and ±Inf fail both comparisons, so the explicit check below is
		// what rejects them.
		if !(seconds > 0) || seconds > float64(maxDurationSeconds) {
			return 0, false
		}
		return time.Duration(seconds * float64(time.Second)), true
	default:
		return 0, false
	}
}

// valueToString converts a pcommon.Value to a string. A Map or Slice value is
// JSON-serialized, which is the expensive case; see valueToStringLimit.
func valueToString(v pcommon.Value) string {
	return valueToStringLimit(v, fullBody)
}

// valueToStringLimit converts a pcommon.Value to a string, keeping at most
// limit runes. limit <= 0 keeps the whole value.
//
// Bounding the conversion matters for Map and Slice bodies: the full JSON
// serialization is the single most expensive per-record operation, and a
// {truncated:N:body} template only ever reads the first N runes. For those
// shapes the value is written into a builder that stops once it has enough
// bytes for N runes, so a megabyte body never becomes a megabyte string.
func valueToStringLimit(v pcommon.Value, limit int) string {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		if limit > 0 {
			return truncateRunes(v.Str(), limit)
		}
		return v.Str()
	case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		if limit <= 0 {
			return v.AsString()
		}
		var b strings.Builder
		writeValueJSON(&b, v, byteBudgetForRunes(limit))
		return truncateRunes(b.String(), limit)
	default:
		// Ints, doubles, bools, bytes and empty are all far below any sane limit.
		s := v.AsString()
		if limit > 0 {
			return truncateRunes(s, limit)
		}
		return s
	}
}

// byteBudgetForRunes returns how many bytes are needed to guarantee n runes of
// UTF-8 text (worst case, 4 bytes per rune).
func byteBudgetForRunes(n int) int {
	return n * 4
}

// writeValueJSON writes a JSON-ish encoding of v into b, stopping once b holds
// budget bytes. The result is a prefix of the full encoding, so a truncated
// value is not necessarily valid JSON - it is only ever used as display text.
// String and bytes elements are themselves cut to the remaining budget
// (boundQuoted): the budget is checked between elements, so without that cut
// one huge string element would be quoted in full - and quoting can expand a
// byte up to six times - even though only the first few runes survive.
func writeValueJSON(b *strings.Builder, v pcommon.Value, budget int) {
	if b.Len() >= budget {
		return
	}
	switch v.Type() {
	case pcommon.ValueTypeStr:
		b.WriteString(strconv.Quote(boundQuoted(v.Str(), budget-b.Len())))
	case pcommon.ValueTypeInt:
		b.WriteString(strconv.FormatInt(v.Int(), 10))
	case pcommon.ValueTypeDouble:
		b.WriteString(strconv.FormatFloat(v.Double(), 'g', -1, 64))
	case pcommon.ValueTypeBool:
		b.WriteString(strconv.FormatBool(v.Bool()))
	case pcommon.ValueTypeBytes:
		b.WriteString(strconv.Quote(boundQuoted(string(v.Bytes().AsRaw()), budget-b.Len())))
	case pcommon.ValueTypeMap:
		b.WriteByte('{')
		first := true
		for key, val := range v.Map().All() {
			if b.Len() >= budget {
				break
			}
			if !first {
				b.WriteByte(',')
			}
			first = false
			b.WriteString(strconv.Quote(boundQuoted(key, budget-b.Len())))
			b.WriteByte(':')
			writeValueJSON(b, val, budget)
		}
		b.WriteByte('}')
	case pcommon.ValueTypeSlice:
		b.WriteByte('[')
		for i, val := range v.Slice().All() {
			if b.Len() >= budget {
				break
			}
			if i > 0 {
				b.WriteByte(',')
			}
			writeValueJSON(b, val, budget)
		}
		b.WriteByte(']')
	default:
		b.WriteString("null")
	}
}

// boundQuoted cuts s so that quoting it cannot blow past the remaining budget.
// Quoting can expand a byte up to six times (\\u00XX), so the cut is
// conservative; the caller re-bounds the final string in runes anyway and the
// result is display text, never parsed. room is at least 1 when called: the
// writeValueJSON entry check guarantees the builder is under budget.
func boundQuoted(s string, room int) string {
	if room <= 0 {
		return ""
	}
	if len(s) > room {
		s = truncateBytes(s, room)
	}
	return s
}

// copyResourceAttributes deep-copies a source resource attribute set. The
// result is safe to keep after ConsumeLogs returns; the upstream pdata it came
// from is not (the connector declares Capabilities{MutatesData: false}).
func copyResourceAttributes(resource pcommon.Map) pcommon.Map {
	out := pcommon.NewMap()
	resource.CopyTo(out)
	return out
}

// keyRuneLimit is the rune limit handed to valueToStringLimit when converting
// a candidate group-key value, so a nested Map or Slice value is never
// serialized in full only to be truncated afterwards. It reads
// max_group_key_bytes as a rune count: for a Map or Slice value the
// serialization stops after roughly cap*4 bytes, and boundGroupKey (or the
// chokepoint in addRecords) applies the exact byte cap. max_group_key_bytes = 0
// disables the bound, so it maps to fullBody.
func (c *logsToSpansConnector) keyRuneLimit() int {
	if c.config.MaxGroupKeyBytes > 0 {
		return c.config.MaxGroupKeyBytes
	}
	return fullBody
}

// boundGroupKey caps a group key at max_group_key_bytes, rune-safe. It is
// applied in addRecords, the single point every caller's key passes through,
// and to the resource-attribute components of the internal map key. Keys that
// differ only past the cap collapse into one group; that is the documented
// cost of bounding attacker-influenced input.
func (c *logsToSpansConnector) boundGroupKey(s string) string {
	if c.config.MaxGroupKeyBytes <= 0 || len(s) <= c.config.MaxGroupKeyBytes {
		return s
	}
	if cut := truncateBytes(s, c.config.MaxGroupKeyBytes); cut != "" {
		return cut
	}
	// Degenerate input (a run of UTF-8 continuation bytes has no valid
	// non-empty prefix): keep a raw byte prefix rather than dropping the key.
	return s[:c.config.MaxGroupKeyBytes]
}

// buildMapKey returns the key used in the internal groups map. Without
// group_by_resource_attributes it is exactly the extracted key. With it, the
// configured resource attribute values are appended so the same extracted
// value from different sources forms separate groups. Every component is
// length-prefixed, so distinct scopes can never collide, even when a value
// contains the separator character. A resource that is missing one of the
// configured attributes contributes an empty component and therefore collapses
// with other resources that are also missing it. Every component is capped at
// max_group_key_bytes like the extracted key, so a huge resource attribute
// cannot be retained per group either.
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
			// Capped like the extracted key: these values also end up in the
			// internal map key, so a huge resource attribute cannot be retained
			// per group either. Values that differ only past the cap collapse.
			value = c.boundGroupKey(valueToStringLimit(v, c.keyRuneLimit()))
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
			// valueToStringLimit bounds the conversion: a nested Map or Slice
			// value is serialized only up to roughly the key cap, never in full,
			// and addRecords applies the exact byte cap to whatever returns.
			if s := valueToStringLimit(val, c.keyRuneLimit()); s != "" {
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
				// Bounded like the attribute path: AsString on a nested value
				// JSON-serializes it in full, so the conversion is capped at the
				// key cap's rune budget and addRecords applies the byte cap.
				s := valueToStringLimit(val, c.keyRuneLimit())
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

// renderSpanName expands the configured span_name_template for one record,
// enforcing a non-empty name and the max_span_name_length cap. The cap is
// defense in depth: a template using only literals cannot grow, but any body
// placeholder can, and a span name is a low-cardinality grouping key in every
// backend.
func (c *logsToSpansConnector) renderSpanName(rec *logRecord) string {
	name := c.spanName.render(rec)
	if name == "" {
		name = rec.body
	}
	if name == "" {
		name = defaultSpanNameFallback
	}
	if c.config.MaxSpanNameLength > 0 && len(name) > c.config.MaxSpanNameLength {
		name = truncateRunes(name, c.config.MaxSpanNameLength)
	}
	return name
}

// extractOptionsFor decides what extraction has to produce for a compiled span
// name template and config, so a template that never reads the body does not
// pay for serializing it.
func extractOptionsFor(cfg *Config, tmpl *spanNameTemplate) extractOptions {
	req := tmpl.bodyRequirement()
	switch {
	case cfg.IncludeBodyAttribute, req.full:
		return extractOptions{bodyLimit: fullBody}
	case req.truncated > 0:
		// The template only needs a prefix; extraction stops there instead of
		// materializing the whole body (for a Map body, the whole JSON encoding).
		return extractOptions{bodyLimit: req.truncated}
	case req.mayRenderEmpty:
		// The template can render empty and falls back to the body, so the body
		// is needed even though the template does not mention it.
		return extractOptions{bodyLimit: fullBody}
	default:
		return extractOptions{bodyLimit: 0}
	}
}

// maxSaneTimestamp is the largest pcommon.Timestamp that a non-negative int64
// nanosecond value can produce (2262-04-11). Anything larger came from a
// negative time.Time and is not a real record timestamp.
const maxSaneTimestamp = pcommon.Timestamp(1<<63 - 1)
