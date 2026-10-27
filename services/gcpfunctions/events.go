package gcpfunctions

import (
	"context"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"cloud.google.com/go/functions/apiv2/functionspb"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpevents"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	// DefaultRetryMin is Eventarc's default minimum redelivery backoff.
	DefaultRetryMin = 10 * time.Second
	// DefaultRetryMax is Eventarc's default maximum redelivery backoff.
	DefaultRetryMax = 600 * time.Second
	// DefaultRetention is how long Eventarc keeps retrying an event by default.
	DefaultRetention = 24 * time.Hour
)

// StorageEvent is one Cloud Storage object change; Data is the object resource JSON.
type StorageEvent struct {
	Time   time.Time
	Type   string
	Bucket string
	Name   string
	Data   []byte
}

// DocumentEvent is one Firestore document change. Old is nil for a create, New is nil for a delete.
type DocumentEvent struct {
	Time       time.Time
	Old        *firestorepb.Document
	New        *firestorepb.Document
	Kind       string
	UpdateMask []string
}

// SetClock replaces the time source used for event redelivery and instance scale-down; nil restores the wall clock.
func (b *InMemoryBackend) SetClock(c gcpscale.Clock) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	if c == nil {
		c = gcpscale.RealClock{}
	}

	b.clock = c
}

// SetIdleTimeout sets how long an idle instance above the minimum is kept before it scales down.
func (b *InMemoryBackend) SetIdleTimeout(d time.Duration) {
	b.mu.Lock("SetIdleTimeout")
	defer b.mu.Unlock()

	if d > 0 {
		b.idleTimeout = d
	}
}

// SetRetryPolicy sets the redelivery backoff bounds and retention for RETRY_POLICY_RETRY triggers.
func (b *InMemoryBackend) SetRetryPolicy(minDelay, maxDelay, retention time.Duration) {
	b.mu.Lock("SetRetryPolicy")
	defer b.mu.Unlock()

	if minDelay > 0 {
		b.retryMin = minDelay
	}

	if maxDelay > 0 {
		b.retryMax = max(maxDelay, b.retryMin)
	}

	if retention > 0 {
		b.retention = retention
	}
}

// stopEvents cancels event redeliveries in flight and waits for them.
func (b *InMemoryBackend) stopEvents() {
	b.mu.Lock("stopEvents")
	cancel, wg := b.evCancel, b.evWG
	b.evCtx, b.evCancel = context.WithCancel(context.Background())
	b.evWG = nil
	b.mu.Unlock()

	cancel()
	wg.Wait()

	b.mu.Lock("stopEvents.wg")
	b.evWG = wg
	b.mu.Unlock()
}

type eventTarget struct {
	name string
	t    target
}

// matching returns the active functions whose trigger has type typ and passes match.
func (b *InMemoryBackend) matching(typ string, match func(*functionspb.EventTrigger, resName) bool) []eventTarget {
	b.mu.RLock("matching")
	defer b.mu.RUnlock()

	var out []eventTarget

	for name, rec := range b.functions {
		et := rec.pb.GetEventTrigger()
		if et.GetEventType() != typ || rec.pb.GetState() != functionspb.Function_ACTIVE {
			continue
		}

		if n, err := parseName(name); err == nil && match(et, n) {
			out = append(out, eventTarget{name: name, t: b.targetOf(name, rec)})
		}
	}

	return out
}

// HandleStorageEvent delivers a Cloud Storage object change to every function whose trigger matches its type and
// bucket filter.
func (b *InMemoryBackend) HandleStorageEvent(ev StorageEvent) {
	typ := gcpevents.StorageTypePrefix + ev.Type
	attrs := map[string]string{attrBucket: ev.Bucket}

	targets := b.matching(typ, func(et *functionspb.EventTrigger, _ resName) bool {
		return matchFilters(et.GetEventFilters(), attrs)
	})
	if len(targets) == 0 {
		return
	}

	ce := gcpevents.StorageEvent(uuid.NewString(), ev.Type, ev.Bucket, ev.Name, ev.Data, ev.Time)
	for _, tg := range targets {
		b.dispatch(tg, ce)
	}
}

// HandleDocumentEvent delivers a Firestore document change to every function whose trigger matches its type
// (created, updated, deleted or written), database, namespace and document filters.
func (b *InMemoryBackend) HandleDocumentEvent(ev DocumentEvent) {
	doc := ev.New
	if doc == nil {
		doc = ev.Old
	}

	head, path, ok := strings.Cut(doc.GetName(), "/documents/")
	if !ok {
		return
	}

	parts := strings.Split(head, "/")
	if len(parts) != 4 { //nolint:mnd // projects/{p}/databases/{d}
		return
	}

	project, database := parts[1], parts[3]
	attrs := map[string]string{attrDatabase: database, attrNamespace: defaultNS, attrDocument: path}
	match := func(et *functionspb.EventTrigger, n resName) bool {
		return n.project == project && matchFilters(et.GetEventFilters(), attrs)
	}

	ce, err := gcpevents.FirestoreEvent(uuid.NewString(), ev.Kind, ev.New, ev.Old, ev.UpdateMask, ev.Time)
	if err != nil {
		return
	}

	for _, typ := range []string{
		gcpevents.FirestoreTypePrefix + ev.Kind, gcpevents.FirestoreTypePrefix + gcpevents.FirestoreWritten,
	} {
		out := ce
		out.Type = typ

		for _, tg := range b.matching(typ, match) {
			b.dispatch(tg, out)
		}
	}
}

// dispatch delivers ce to tg in the background; RETRY_POLICY_RETRY redelivers with exponential backoff.
func (b *InMemoryBackend) dispatch(tg eventTarget, ce gcpevents.CloudEvent) {
	b.mu.RLock("dispatch")
	defer b.mu.RUnlock()

	if b.evWG == nil {
		return
	}

	ctx, clk, lo, hi, keep := b.evCtx, b.clock, b.retryMin, b.retryMax, b.retention

	b.evWG.Go(func() { b.deliverWithRetry(ctx, clk, tg, ce, [3]time.Duration{lo, hi, keep}) })
}

func (b *InMemoryBackend) deliverWithRetry(
	ctx context.Context, clk gcpscale.Clock, tg eventTarget, ce gcpevents.CloudEvent, pol [3]time.Duration,
) {
	delay, giveUp := pol[0], clk.Now().Add(pol[2])

	for {
		rec := &statusRecorder{header: http.Header{}, code: http.StatusOK}
		b.deliver(ctx, rec, tg.t, ce)

		if rec.code < http.StatusBadRequest || !tg.t.retry {
			return
		}

		cur, still := b.byName(tg.name)
		if !still || !cur.retry || ctx.Err() != nil || !clk.Now().Add(delay).Before(giveUp) {
			logger.Load(ctx).DebugContext(ctx, "gcpfunctions: event dropped", "function", tg.name, "status", rec.code)

			return
		}

		timer := clk.NewTimer(delay)

		select {
		case <-timer.C():
		case <-ctx.Done():
			timer.Stop()

			return
		}

		delay = min(delay*2, pol[1]) //nolint:mnd // exponential backoff
	}
}
