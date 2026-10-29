package lambda

import (
	"cmp"
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// streamEvent is one filtered Kinesis or DynamoDB stream record, already shaped for the Lambda event.
type streamEvent struct {
	arrival time.Time
	seq     string
	key     string
	raw     json.RawMessage
}

// streamBatch is a batch awaiting a successful invocation.
type streamBatch struct {
	win      *tumblingWindow
	events   []streamEvent
	last     invokeOutcome
	attempts int
	final    bool
	early    bool
}

// streamShardRef identifies the shard a batch belongs to; key is the poller's per-shard map key.
type streamShardRef struct {
	shardID string
	key     string
	ddb     bool
}

func streamEventPayload(events []streamEvent) ([]byte, error) {
	raws := make([]json.RawMessage, len(events))
	for i, e := range events {
		raws[i] = e.raw
	}

	return json.Marshal(struct {
		Records []json.RawMessage `json:"Records"`
	}{Records: raws})
}

func (p *EventSourcePoller) frontStreamBatch(key string) *streamBatch {
	p.mu.RLock("frontStreamBatch")
	defer p.mu.RUnlock()

	if q := p.streamPending[key]; len(q) > 0 {
		return q[0]
	}

	return nil
}

func (p *EventSourcePoller) enqueueStreamBatch(key string, b *streamBatch) {
	p.mu.Lock("enqueueStreamBatch")
	defer p.mu.Unlock()

	p.streamPending[key] = append(p.streamPending[key], b)
}

func (p *EventSourcePoller) replaceFrontStreamBatch(key string, with ...*streamBatch) {
	p.mu.Lock("replaceFrontStreamBatch")
	defer p.mu.Unlock()

	q := p.streamPending[key]
	if len(q) == 0 {
		return
	}

	rest := slices.Concat(with, q[1:])
	if len(rest) == 0 {
		delete(p.streamPending, key)

		return
	}

	p.streamPending[key] = rest
}

// drainStream works through the shard's pending batches in order, stopping at the first one that must be retried.
func (p *EventSourcePoller) drainStream(ctx context.Context, m *EventSourceMapping, ref streamShardRef) {
	pol := policyFor(m)

	for ctx.Err() == nil {
		b := p.frontStreamBatch(ref.key)
		if b == nil || !p.stepStreamBatch(ctx, m, ref, pol, b) {
			return
		}
	}
}

// stepStreamBatch makes one attempt at the front batch and reports whether the shard can move on.
func (p *EventSourcePoller) stepStreamBatch(
	ctx context.Context, m *EventSourceMapping, ref streamShardRef, pol retryPolicy, b *streamBatch,
) bool {
	if !b.final {
		fresh, expired := splitExpiredEvents(b.events, pol.maxAge, p.nowTime())
		if len(expired) > 0 {
			p.dropStreamEvents(ctx, m, ref, b, expired)
			b.events = fresh
		}

		if len(b.events) == 0 {
			p.replaceFrontStreamBatch(ref.key)

			return true
		}
	}

	payload, err := p.streamBatchPayload(m, ref, b)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "event source poller: failed to marshal stream event", "error", err)
		p.replaceFrontStreamBatch(ref.key)

		return true
	}

	out, done := p.interpretStreamResponse(m, b, p.invokeStream(ctx, m, ref, payload))
	b.attempts++
	b.last = out

	failed := len(b.events)
	if !out.failed() {
		failed = 0
		if done > 0 {
			failed = len(b.events) - done
		}
	}

	p.esmInvokeMetrics(m, len(b.events), failed)

	if !out.failed() && done == 0 {
		p.lambdaBackend.setESMLastProcessingResult(m.UUID, "OK")
		p.settleWindowBatch(ref.key, b, true)
		p.replaceFrontStreamBatch(ref.key)

		return true
	}

	p.lambdaBackend.setESMLastProcessingResult(m.UUID, "PROBLEM: "+out.message())
	logger.Load(ctx).WarnContext(ctx, "event source poller: stream invocation failed",
		"uuid", m.UUID, "attempt", b.attempts, "error", out.message())

	if done > 0 {
		b.events = b.events[done:]
	} else if pol.bisect && len(b.events) > 1 {
		mid := len(b.events) / bisectParts
		left := &streamBatch{events: b.events[:mid], attempts: b.attempts - 1, last: out, win: b.win}
		right := &streamBatch{events: b.events[mid:], attempts: b.attempts - 1, last: out, win: b.win}
		p.replaceFrontStreamBatch(ref.key, left, right)

		return true
	}

	if pol.limited && b.attempts > pol.maxRetries {
		p.dropStreamEvents(ctx, m, ref, b, b.events)
		p.settleWindowBatch(ref.key, &streamBatch{win: b.win, final: b.final, early: b.early}, false)
		p.replaceFrontStreamBatch(ref.key)

		return true
	}

	return false
}

func splitExpiredEvents(events []streamEvent, maxAge time.Duration, now time.Time) ([]streamEvent, []streamEvent) {
	if maxAge <= 0 {
		return events, nil
	}

	var fresh, expired []streamEvent

	for _, e := range events {
		if !e.arrival.IsZero() && now.Sub(e.arrival) > maxAge {
			expired = append(expired, e)
		} else {
			fresh = append(fresh, e)
		}
	}

	return fresh, expired
}

// invokeStream invokes the mapped function synchronously so function errors surface.
func (p *EventSourcePoller) invokeStream(
	ctx context.Context, m *EventSourceMapping, ref streamShardRef, payload []byte,
) invokeOutcome {
	sctx, span := startESMSpan(ctx, m.FunctionARN, m.EventSourceARN)
	out := p.invokeStreamUntraced(sctx, m, ref, payload)
	span.FinishErr(out.err)

	return out
}

func (p *EventSourcePoller) invokeStreamUntraced(
	ctx context.Context, m *EventSourceMapping, ref streamShardRef, payload []byte,
) invokeOutcome {
	fnName, qualifier := functionNameAndQualifierFromARN(m.FunctionARN)
	if fnName == "" {
		fnName = m.FunctionARN
	}

	out := invokeOutcome{requestID: uuid.New().String(), statusCode: statusOK, executedVersion: versionLatest}

	switch {
	case p.streamInvoker != nil:
		return fillOutcome(p.streamInvoker(ctx, fnName, payload), out)
	case ref.ddb && p.ddbInvoker != nil:
		if err := p.ddbInvoker(ctx, fnName, payload); err != nil {
			out.err = err
			out.functionError = functionErrorUnhandled
		}

		return out
	}

	if fn, err := p.lambdaBackend.resolveQualifier(fnName, qualifier); err == nil && fn.Version != "" {
		out.executedVersion = fn.Version
	}

	body, _, fnErr, status, err := p.lambdaBackend.InvokeFunctionWithQualifier(
		ctx, fnName, qualifier, "", "", InvocationTypeRequestResponse, payload,
	)
	out.statusCode = cmp.Or(status, out.statusCode)
	out.functionError = fnErr
	out.body = body

	if err != nil {
		out.err = err
		out.functionError = cmp.Or(fnErr, functionErrorUnhandled)
	}

	return out
}

func fillOutcome(got, def invokeOutcome) invokeOutcome {
	got.requestID = cmp.Or(got.requestID, def.requestID)
	got.executedVersion = cmp.Or(got.executedVersion, def.executedVersion)
	got.statusCode = cmp.Or(got.statusCode, def.statusCode)

	if got.err != nil && got.functionError == "" {
		got.functionError = functionErrorUnhandled
	}

	return got
}

// dropStreamEvents counts events discarded for expiry or exhausted retries and delivers them to OnFailure.
func (p *EventSourcePoller) dropStreamEvents(
	ctx context.Context, m *EventSourceMapping, ref streamShardRef, b *streamBatch, events []streamEvent,
) {
	if len(events) == 0 {
		return
	}

	p.esmCount(m, esmMetricDropped, len(events))

	dest := failureDestination(m)
	if dest == "" {
		return
	}

	rec := newFailureRecord(m, b.attempts, b.last)
	info := streamInfoFor(events, ref, m.EventSourceARN)

	if ref.ddb {
		rec.DDBBatchInfo = &info
	} else {
		rec.KinesisBatchInfo = &info
	}

	if isS3Destination(dest) {
		if whole, err := streamEventPayload(events); err == nil {
			rec.Payload = string(whole)
		}
	}

	body, err := marshalFailure(rec)
	if err == nil {
		err = p.lambdaBackend.deliverFailureRecord(ctx, m.UUID, ref.shardID, dest, body)
	}

	if err != nil {
		logDeliveryFailure(ctx, m.UUID, dest, err)

		return
	}

	p.esmCount(m, esmMetricOnFailureDelivered, len(events))
}

func streamInfoFor(events []streamEvent, ref streamShardRef, streamARN string) streamBatchInfo {
	first, last := events[0], events[len(events)-1]
	layout := failureTimestampMillisLayout

	if ref.ddb {
		layout = failureTimestampSecondsLayout
	}

	return streamBatchInfo{
		ShardID:                         ref.shardID,
		StartSequenceNumber:             first.seq,
		EndSequenceNumber:               last.seq,
		ApproximateArrivalOfFirstRecord: first.arrival.UTC().Format(layout),
		ApproximateArrivalOfLastRecord:  last.arrival.UTC().Format(layout),
		BatchSize:                       len(events),
		StreamArn:                       streamARN,
	}
}

// shardKeyMapping returns the mapping UUID a per-shard key belongs to.
func (p *EventSourcePoller) shardKeyMapping(key string) string {
	id, _, _ := strings.Cut(key, ":")

	return id
}
