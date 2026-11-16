package gcplogging

import (
	"slices"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	tailBuffer      = 1024
	tailBatch       = 100
	maxTailSubs     = 10
	maxTailParents  = 100
	maxTailTotal    = 1000
	maxBufferWindow = time.Minute
)

type tailSub struct {
	ch      chan *loggingpb.LogEntry
	parents map[string]struct{}
	f       *filter
	dropped int32
	limited int32
}

type tailRate struct {
	start time.Time
	count int
}

type tailHub struct {
	mu    *lockmetrics.RWMutex
	subs  map[*tailSub]struct{}
	rates map[string]*tailRate
	done  chan struct{}
}

func newTailHub() *tailHub {
	return &tailHub{
		mu:    lockmetrics.New("gcplogging.tail"),
		subs:  make(map[*tailSub]struct{}),
		rates: make(map[string]*tailRate),
		done:  make(chan struct{}),
	}
}

func (h *tailHub) add(s *tailSub) error {
	h.mu.Lock("add")
	defer h.mu.Unlock()

	if len(h.subs) >= maxTailTotal {
		return status.Errorf(codes.ResourceExhausted, "The server allows at most %d tailing sessions.", maxTailTotal)
	}

	for p := range s.parents {
		open := 0

		for o := range h.subs {
			if _, ok := o.parents[p]; ok {
				open++
			}
		}

		if open >= maxTailSubs {
			return status.Errorf(codes.ResourceExhausted, "%s already has %d open tailing sessions.", p, maxTailSubs)
		}
	}

	h.subs[s] = struct{}{}

	return nil
}

func (h *tailHub) remove(s *tailSub) {
	h.mu.Lock("remove")
	delete(h.subs, s)
	h.mu.Unlock()
}

// takeSuppressed returns and clears the counts of entries withheld by the quota and by a slow consumer.
func (h *tailHub) takeSuppressed(s *tailSub) (int32, int32) {
	h.mu.Lock("takeSuppressed")
	defer h.mu.Unlock()

	limited, dropped := s.limited, s.dropped
	s.limited, s.dropped = 0, 0

	return limited, dropped
}

// allow charges one delivered entry to a parent and reports whether the per-minute tailing quota has room.
func (h *tailHub) allow(parent string, now time.Time) bool {
	if len(h.rates) > maxTrackedParents {
		for p, r := range h.rates {
			if now.Sub(r.start) >= quotaWindow {
				delete(h.rates, p)
			}
		}
	}

	r := h.rates[parent]
	if r == nil || now.Sub(r.start) >= quotaWindow {
		r = &tailRate{start: now}
		h.rates[parent] = r
	}

	if r.count >= tailEntriesPerMinute {
		return false
	}

	r.count++

	return true
}

func (h *tailHub) publish(entries []*loggingpb.LogEntry, now time.Time) {
	h.mu.Lock("publish")
	defer h.mu.Unlock()

	for s := range h.subs {
		for _, e := range entries {
			parent := parentOf(e.GetLogName())
			if _, ok := s.parents[parent]; !ok || !s.f.matches(newDoc(e)) {
				continue
			}

			if !h.allow(parent, now) {
				s.limited++

				continue
			}

			select {
			case s.ch <- e:
			default:
				s.dropped++
			}
		}
	}
}

func (b *InMemoryBackend) closeTails() {
	h := b.tails
	h.mu.Lock("closeTails")
	close(h.done)
	h.done = make(chan struct{})
	h.mu.Unlock()
}

func (h *tailHub) doneChan() <-chan struct{} {
	h.mu.RLock("doneChan")
	defer h.mu.RUnlock()

	return h.done
}

func (b *InMemoryBackend) newTail(req *loggingpb.TailLogEntriesRequest) (*tailSub, error) {
	names := req.GetResourceNames()
	if len(names) == 0 || len(names) > maxTailParents {
		return nil, errInvalid("resource_names must contain between 1 and %d resources.", maxTailParents)
	}

	if w := req.GetBufferWindow(); w != nil && (w.AsDuration() < 0 || w.AsDuration() > maxBufferWindow) {
		return nil, errInvalid("buffer_window must be between 0 and %d milliseconds.", maxBufferWindow.Milliseconds())
	}

	f, err := compileFilter(req.GetFilter())
	if err != nil {
		return nil, err
	}

	parents := make(map[string]struct{}, len(names))

	for _, n := range names {
		p := parentOf(n)
		if p == "" {
			return nil, errInvalid("Invalid resource name [%s].", n)
		}

		parents[p] = struct{}{}
	}

	sub := &tailSub{ch: make(chan *loggingpb.LogEntry, tailBuffer), parents: parents, f: f}

	return sub, b.tails.add(sub)
}

const defaultBufferWindow = 2 * time.Second

func tailWindow(req *loggingpb.TailLogEntriesRequest) time.Duration {
	if w := req.GetBufferWindow(); w != nil {
		return w.AsDuration()
	}

	return defaultBufferWindow
}

type heldEntry struct {
	due   time.Time
	entry *loggingpb.LogEntry
}

// Tail streams new matching entries to send until the client goes away or the server stops. Entries wait
// buffer_window on the server and leave in timestamp order, so late arrivals do not reach the client out of order.
func (b *InMemoryBackend) Tail(
	stream loggingpb.LoggingServiceV2_TailLogEntriesServer,
	req *loggingpb.TailLogEntriesRequest,
) error {
	sub, err := b.newTail(req)
	if err != nil {
		return err
	}

	defer b.tails.remove(sub)

	ctx := stream.Context()
	done := b.tails.doneChan()
	window := tailWindow(req)

	go drainRecv(stream)

	var held []heldEntry

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for {
		var due <-chan time.Time

		if len(held) > 0 {
			timer.Reset(max(time.Until(held[0].due), 0))
			due = timer.C
		}

		select {
		case <-ctx.Done():
			return nil
		case <-done:
			return status.Error(codes.Unavailable, "server is shutting down")
		case e := <-sub.ch:
			if window == 0 {
				err = b.sendEntries(stream, sub, drainReady(sub, e))
			} else if held = append(held, heldEntry{due: time.Now().Add(window), entry: e}); len(held) >= tailBuffer {
				err, held = b.flushHeld(stream, sub, held), nil
			}
		case <-due:
			err, held = b.flushHeld(stream, sub, held), nil
		}

		if err != nil {
			return err
		}
	}
}

// drainReady collects first and every entry already queued, up to one batch.
func drainReady(sub *tailSub, first *loggingpb.LogEntry) []*loggingpb.LogEntry {
	out := []*loggingpb.LogEntry{first}

	for len(out) < tailBatch {
		select {
		case e := <-sub.ch:
			out = append(out, e)

			continue
		default:
		}

		break
	}

	return out
}

// flushHeld sends every held entry, ordered by timestamp: the oldest one's window has elapsed, and entries that
// arrived within it leave together so a late arrival is not delivered after a newer entry.
func (b *InMemoryBackend) flushHeld(
	stream loggingpb.LoggingServiceV2_TailLogEntriesServer,
	sub *tailSub,
	held []heldEntry,
) error {
	ready := make([]*loggingpb.LogEntry, len(held))
	for i, h := range held {
		ready[i] = h.entry
	}

	slices.SortStableFunc(ready, func(a, c *loggingpb.LogEntry) int {
		return a.GetTimestamp().AsTime().Compare(c.GetTimestamp().AsTime())
	})

	return b.sendEntries(stream, sub, ready)
}

// drainRecv consumes further client messages so the receive side never blocks.
func drainRecv(stream loggingpb.LoggingServiceV2_TailLogEntriesServer) {
	for {
		if _, err := stream.Recv(); err != nil {
			return
		}
	}
}

func (b *InMemoryBackend) sendEntries(
	stream loggingpb.LoggingServiceV2_TailLogEntriesServer,
	sub *tailSub,
	entries []*loggingpb.LogEntry,
) error {
	limited, dropped := b.tails.takeSuppressed(sub)

	for first := true; first || len(entries) > 0; first = false {
		n := min(len(entries), tailBatch)
		resp := &loggingpb.TailLogEntriesResponse{Entries: entries[:n]}
		entries = entries[n:]

		if first {
			resp.SuppressionInfo = suppression(limited, dropped)
		}

		if len(resp.GetEntries()) == 0 && len(resp.GetSuppressionInfo()) == 0 {
			return nil
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

func suppression(limited, dropped int32) []*loggingpb.TailLogEntriesResponse_SuppressionInfo {
	var out []*loggingpb.TailLogEntriesResponse_SuppressionInfo

	if limited > 0 {
		out = append(out, &loggingpb.TailLogEntriesResponse_SuppressionInfo{
			Reason:          loggingpb.TailLogEntriesResponse_SuppressionInfo_RATE_LIMIT,
			SuppressedCount: limited,
		})
	}

	if dropped > 0 {
		out = append(out, &loggingpb.TailLogEntriesResponse_SuppressionInfo{
			Reason:          loggingpb.TailLogEntriesResponse_SuppressionInfo_NOT_CONSUMED,
			SuppressedCount: dropped,
		})
	}

	return out
}
