package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxTumblingStateBytes = 1 << 20
	tumblingTimeLayout    = "2006-01-02T15:04:05Z"
	reportBatchItemFailed = "ReportBatchItemFailures"
	emptyJSONObject       = "{}"
)

var (
	errTumblingStateMissing = errors.New("tumbling window response has no state property")
	errBatchItemFailure     = errors.New("function reported a failure for the first record of the batch")
)

// tumblingWindow is one open aggregation window of a shard lane; state is the last state a function returned.
type tumblingWindow struct {
	start       time.Time
	end         time.Time
	state       json.RawMessage
	finalQueued bool
}

// streamBuffer holds records waiting for BatchSize or MaximumBatchingWindowInSeconds.
type streamBuffer struct {
	firstSeen time.Time
	events    []streamEvent
}

func (p *EventSourcePoller) nowTime() time.Time {
	if p.clock != nil {
		return p.clock()
	}

	return time.Now()
}

func laneKey(shardKey string, lane, lanes int) string {
	if lanes <= 1 {
		return shardKey
	}

	return shardKey + "#" + strconv.Itoa(lane)
}

func isLaneOf(key, shardKey string) bool {
	return key == shardKey || strings.HasPrefix(key, shardKey+"#")
}

func laneIndex(partitionKey string, lanes int) int {
	if lanes <= 1 {
		return 0
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(partitionKey))

	return int(h.Sum32()&math.MaxInt32) % lanes
}

// deliverStreamEvents buffers the events and dispatches whatever is due.
func (p *EventSourcePoller) deliverStreamEvents(
	ctx context.Context, m *EventSourceMapping, ref streamShardRef, events []streamEvent,
) {
	p.bufferStreamEvents(ref.key, events)
	p.flushStreamShard(ctx, m, ref, false)
}

// settleStreamShard is called when the shard has no new records: due windows close and due buffers flush.
func (p *EventSourcePoller) settleStreamShard(ctx context.Context, m *EventSourceMapping, ref streamShardRef) {
	p.flushStreamShard(ctx, m, ref, true)
}

func (p *EventSourcePoller) bufferStreamEvents(key string, events []streamEvent) {
	p.mu.Lock("bufferStreamEvents")
	defer p.mu.Unlock()

	buf := p.streamBuffers[key]
	if buf == nil {
		buf = &streamBuffer{firstSeen: p.nowTime()}
		p.streamBuffers[key] = buf
	}

	buf.events = append(buf.events, events...)
}

// takeDueEvents pops the buffered events that are ready to be invoked, in BatchSize chunks.
func (p *EventSourcePoller) takeDueEvents(m *EventSourceMapping, key string, now time.Time) [][]streamEvent {
	p.mu.Lock("takeDueEvents")
	defer p.mu.Unlock()

	buf := p.streamBuffers[key]
	if buf == nil || len(buf.events) == 0 {
		return nil
	}

	size := m.BatchSize
	if size <= 0 {
		size = len(buf.events)
	}

	window := time.Duration(m.MaximumBatchingWindowInSeconds) * time.Second
	take := len(buf.events)

	if window > 0 && now.Sub(buf.firstSeen) < window {
		take = len(buf.events) / size * size
		if take == 0 {
			return nil
		}
	}

	taken := buf.events[:take]
	if take == len(buf.events) {
		delete(p.streamBuffers, key)
	} else {
		buf.events = slices.Clone(buf.events[take:])
	}

	return slices.Collect(slices.Chunk(taken, size))
}

func (p *EventSourcePoller) flushStreamShard(
	ctx context.Context, m *EventSourceMapping, ref streamShardRef, caughtUp bool,
) {
	now := p.nowTime()
	lanes := max(1, m.ParallelizationFactor)

	for _, chunk := range p.takeDueEvents(m, ref.key, now) {
		perLane := make([][]streamEvent, lanes)
		for _, e := range chunk {
			i := laneIndex(e.key, lanes)
			perLane[i] = append(perLane[i], e)
		}

		for i, evs := range perLane {
			if len(evs) > 0 {
				p.enqueueLane(m, laneKey(ref.key, i, lanes), evs, now)
			}
		}
	}

	if caughtUp && m.TumblingWindowInSeconds > 0 {
		p.queueDueFinals(ref.key, now)
	}

	p.drainShard(ctx, m, ref)
}

func (p *EventSourcePoller) enqueueLane(m *EventSourceMapping, lane string, events []streamEvent, now time.Time) {
	if m.TumblingWindowInSeconds <= 0 {
		p.enqueueStreamBatch(lane, &streamBatch{events: events})

		return
	}

	size := time.Duration(m.TumblingWindowInSeconds) * time.Second

	p.mu.Lock("enqueueLane")
	defer p.mu.Unlock()

	for i := 0; i < len(events); {
		w := p.streamWindows[lane]

		var floor time.Time
		if w != nil && !w.finalQueued {
			floor = w.start
		}

		start := laterOf(windowStart(events[i], size, now), floor)
		j := i + 1

		for j < len(events) && laterOf(windowStart(events[j], size, now), floor).Equal(start) {
			j++
		}

		if w == nil || w.finalQueued || !w.start.Equal(start) {
			if w != nil && !w.finalQueued {
				p.queueFinalLocked(lane, w, false)
			}

			w = &tumblingWindow{start: start, end: start.Add(size)}
			p.streamWindows[lane] = w
		}

		p.streamPending[lane] = append(p.streamPending[lane], &streamBatch{events: events[i:j], win: w})
		i = j
	}
}

func laterOf(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}

	return a
}

func windowStart(e streamEvent, size time.Duration, now time.Time) time.Time {
	t := e.arrival
	if t.IsZero() {
		t = now
	}

	secs := int64(size / time.Second)

	return time.Unix(t.Unix()/secs*secs, 0).UTC()
}

func (p *EventSourcePoller) queueFinalLocked(lane string, w *tumblingWindow, early bool) {
	w.finalQueued = true
	p.streamPending[lane] = append(p.streamPending[lane], &streamBatch{win: w, final: true, early: early})
}

func (p *EventSourcePoller) queueDueFinals(shardKey string, now time.Time) {
	p.mu.Lock("queueDueFinals")
	defer p.mu.Unlock()

	for lane, w := range p.streamWindows {
		if isLaneOf(lane, shardKey) && !w.finalQueued && !now.Before(w.end) {
			p.queueFinalLocked(lane, w, false)
		}
	}
}

// shardBlocked reports whether any lane of the shard still has batches to retry.
func (p *EventSourcePoller) shardBlocked(shardKey string) bool {
	p.mu.RLock("shardBlocked")
	defer p.mu.RUnlock()

	for k, q := range p.streamPending {
		if len(q) > 0 && isLaneOf(k, shardKey) {
			return true
		}
	}

	return false
}

// drainShard drains every lane of the shard concurrently; batches within a lane stay ordered.
func (p *EventSourcePoller) drainShard(ctx context.Context, m *EventSourceMapping, ref streamShardRef) {
	p.mu.RLock("drainShard")

	var keys []string

	for k, q := range p.streamPending {
		if len(q) > 0 && isLaneOf(k, ref.key) {
			keys = append(keys, k)
		}
	}

	p.mu.RUnlock()

	if len(keys) == 1 {
		lane := ref
		lane.key = keys[0]
		p.drainStream(ctx, m, lane)

		return
	}

	var wg sync.WaitGroup

	for _, k := range keys {
		lane := ref
		lane.key = k

		wg.Go(func() { p.drainStream(ctx, m, lane) })
	}

	wg.Wait()
}

type windowRange struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

type windowedPayload struct {
	State                   json.RawMessage   `json:"state"`
	Window                  windowRange       `json:"window"`
	ShardID                 string            `json:"shardId"`
	EventSourceARN          string            `json:"eventSourceARN"`
	Records                 []json.RawMessage `json:"Records"`
	IsFinalInvokeForWindow  bool              `json:"isFinalInvokeForWindow"`
	IsWindowTerminatedEarly bool              `json:"isWindowTerminatedEarly"`
}

func (p *EventSourcePoller) streamBatchPayload(
	m *EventSourceMapping, ref streamShardRef, b *streamBatch,
) ([]byte, error) {
	if b.win == nil {
		return streamEventPayload(b.events)
	}

	p.mu.RLock("streamBatchPayload")
	state := b.win.state
	p.mu.RUnlock()

	if len(state) == 0 {
		state = json.RawMessage(emptyJSONObject)
	}

	raws := make([]json.RawMessage, len(b.events))
	for i, e := range b.events {
		raws[i] = e.raw
	}

	return json.Marshal(windowedPayload{
		Records: raws,
		Window: windowRange{
			Start: b.win.start.UTC().Format(tumblingTimeLayout),
			End:   b.win.end.UTC().Format(tumblingTimeLayout),
		},
		State:                   state,
		ShardID:                 ref.shardID,
		EventSourceARN:          m.EventSourceARN,
		IsFinalInvokeForWindow:  b.final,
		IsWindowTerminatedEarly: b.early,
	})
}

// windowResponseState extracts the required state property of a tumbling-window response.
func windowResponseState(body []byte) (json.RawMessage, bool) {
	var resp struct {
		State *json.RawMessage `json:"state"`
	}

	if err := json.Unmarshal(body, &resp); err != nil || resp.State == nil {
		return nil, false
	}

	if bytes.Equal(bytes.TrimSpace(*resp.State), []byte("null")) {
		return json.RawMessage(emptyJSONObject), true
	}

	return *resp.State, true
}

// batchFailureIndex returns -1 when the response reports no failures, the position of the lowest-sequence failed
// record otherwise, and 0 when the report is unusable (a complete failure).
func batchFailureIndex(body []byte, events []streamEvent) int {
	var resp struct {
		BatchItemFailures []struct {
			ItemIdentifier *string `json:"itemIdentifier"`
		} `json:"batchItemFailures"`
	}

	if len(bytes.TrimSpace(body)) == 0 || json.Unmarshal(body, &resp) != nil || len(resp.BatchItemFailures) == 0 {
		return -1
	}

	first := len(events)

	for _, f := range resp.BatchItemFailures {
		if f.ItemIdentifier == nil || *f.ItemIdentifier == "" {
			return 0
		}

		i := slices.IndexFunc(events, func(e streamEvent) bool { return e.seq == *f.ItemIdentifier })
		if i < 0 {
			return 0
		}

		first = min(first, i)
	}

	return first
}

// interpretStreamResponse applies the tumbling-window and partial-batch rules to a successful invocation.
// It returns the outcome (marked failed when the response is unusable) and the leading records confirmed processed.
func (p *EventSourcePoller) interpretStreamResponse(
	m *EventSourceMapping, b *streamBatch, out invokeOutcome,
) (invokeOutcome, int) {
	if out.failed() {
		return out, 0
	}

	idx := -1
	if len(b.events) > 0 && slices.Contains(m.FunctionResponseTypes, reportBatchItemFailed) {
		idx = batchFailureIndex(out.body, b.events)
	}

	if idx == 0 {
		out.err, out.functionError = errBatchItemFailure, functionErrorUnhandled

		return out, 0
	}

	if b.win != nil {
		state, ok := windowResponseState(out.body)
		if !ok {
			out.err, out.functionError = errTumblingStateMissing, functionErrorUnhandled

			return out, 0
		}

		p.mu.Lock("interpretStreamResponse")
		b.win.state = state
		p.mu.Unlock()
	}

	return out, max(idx, 0)
}

// settleWindowBatch finishes a window after its final invocation succeeds or is dropped, or ends it early
// when the aggregated state outgrows the per-shard limit.
func (p *EventSourcePoller) settleWindowBatch(lane string, b *streamBatch, succeeded bool) {
	if b.win == nil {
		return
	}

	p.mu.Lock("settleWindowBatch")
	defer p.mu.Unlock()

	switch {
	case b.final && b.early:
		b.win.finalQueued = false
		b.win.state = nil
	case b.final:
		if p.streamWindows[lane] == b.win {
			delete(p.streamWindows, lane)
		}

		b.win.state = nil
	case succeeded && len(b.win.state) > maxTumblingStateBytes && !b.win.finalQueued:
		b.win.finalQueued = true
		q := p.streamPending[lane]
		p.streamPending[lane] = slices.Concat(q[:1], []*streamBatch{{win: b.win, final: true, early: true}}, q[1:])
	}
}
