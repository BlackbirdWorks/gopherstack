package gcplogging

import (
	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	tailBuffer     = 1024
	tailBatch      = 100
	maxTailSubs    = 10
	maxTailParents = 100
)

type tailSub struct {
	ch      chan *loggingpb.LogEntry
	parents map[string]struct{}
	f       *filter
	dropped int32
}

type tailHub struct {
	mu   *lockmetrics.RWMutex
	subs map[*tailSub]struct{}
	done chan struct{}
}

func newTailHub() *tailHub {
	return &tailHub{
		mu:   lockmetrics.New("gcplogging.tail"),
		subs: make(map[*tailSub]struct{}),
		done: make(chan struct{}),
	}
}

func (h *tailHub) add(s *tailSub) error {
	h.mu.Lock("add")
	defer h.mu.Unlock()

	if len(h.subs) >= maxTailSubs {
		return status.Error(codes.ResourceExhausted, "Too many concurrent tailing sessions.")
	}

	h.subs[s] = struct{}{}

	return nil
}

func (h *tailHub) remove(s *tailSub) {
	h.mu.Lock("remove")
	delete(h.subs, s)
	h.mu.Unlock()
}

// takeDropped returns and clears the count of entries the subscriber missed.
func (h *tailHub) takeDropped(s *tailSub) int32 {
	h.mu.Lock("takeDropped")
	defer h.mu.Unlock()

	n := s.dropped
	s.dropped = 0

	return n
}

func (h *tailHub) publish(entries []*loggingpb.LogEntry) {
	h.mu.Lock("publish")
	defer h.mu.Unlock()

	for s := range h.subs {
		for _, e := range entries {
			if _, ok := s.parents[parentOf(e.GetLogName())]; !ok || !s.f.matches(newDoc(e)) {
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

// Tail streams new matching entries to send until the client goes away or the server stops.
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

	go drainRecv(stream)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-done:
			return status.Error(codes.Unavailable, "server is shutting down")
		case e := <-sub.ch:
			if err = b.sendBatch(stream, sub, e); err != nil {
				return err
			}
		}
	}
}

// drainRecv consumes further client messages so the receive side never blocks.
func drainRecv(stream loggingpb.LoggingServiceV2_TailLogEntriesServer) {
	for {
		if _, err := stream.Recv(); err != nil {
			return
		}
	}
}

func (b *InMemoryBackend) sendBatch(
	stream loggingpb.LoggingServiceV2_TailLogEntriesServer,
	sub *tailSub,
	first *loggingpb.LogEntry,
) error {
	resp := &loggingpb.TailLogEntriesResponse{Entries: []*loggingpb.LogEntry{first}}

	for len(resp.GetEntries()) < tailBatch {
		select {
		case e := <-sub.ch:
			resp.Entries = append(resp.Entries, e)

			continue
		default:
		}

		break
	}

	if n := b.tails.takeDropped(sub); n > 0 {
		resp.SuppressionInfo = []*loggingpb.TailLogEntriesResponse_SuppressionInfo{{
			Reason:          loggingpb.TailLogEntriesResponse_SuppressionInfo_RATE_LIMIT,
			SuppressedCount: n,
		}}
	}

	return stream.Send(resp)
}
