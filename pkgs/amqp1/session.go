package amqp1

import (
	"context"
	"math"
	"sync"
)

const (
	sessionWindow uint32 = 2048
	linkCredit    uint32 = 512
)

type sentDelivery struct {
	link *sendLink
	out  *Outgoing
}

type session struct {
	c         *conn
	links     map[uint32]any
	unsettled map[uint32]*sentDelivery
	winSig    chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc

	mu             sync.Mutex
	remoteWindow   int64
	nextOutgoingID uint32
	nextIncomingID uint32
	incomingWindow uint32
	channel        uint16
}

func newSession(c *conn, channel uint16, b *Begin) *session {
	ctx, cancel := context.WithCancel(c.ctx)

	return &session{
		c: c, channel: channel, ctx: ctx, cancel: cancel,
		links:          map[uint32]any{},
		unsettled:      map[uint32]*sentDelivery{},
		winSig:         make(chan struct{}),
		remoteWindow:   int64(b.IncomingWindow),
		nextIncomingID: b.NextOutgoingID,
		incomingWindow: sessionWindow,
	}
}

func (s *session) baseFlow() *Flow {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := s.nextIncomingID

	return &Flow{
		NextIncomingID: &next, IncomingWindow: s.incomingWindow, NextOutgoingID: s.nextOutgoingID,
		OutgoingWindow: sessionWindow,
	}
}

func (s *session) send(p Performative) bool {
	return s.c.writePerformative(FrameAMQP, s.channel, p) == nil
}

// handle processes one performative; false means the connection must end.
func (s *session) handle(p Performative) bool {
	switch v := p.(type) {
	case *Attach:
		return s.attach(v)
	case *Flow:
		s.onFlow(v)
	case *Transfer:
		return s.onTransfer(v)
	case *Disposition:
		s.onDisposition(v)
	case *Detach:
		s.detach(v)
	case *End:
		s.send(&End{})
		s.teardown()
		s.c.dropSession(s.channel)
	default:
		s.c.closeWithError(&Error{Condition: CondIllegalState, Description: "unexpected performative"})

		return false
	}

	return true
}

func (s *session) onFlow(f *Flow) {
	s.mu.Lock()

	if f.NextIncomingID != nil {
		w := *f.NextIncomingID + f.IncomingWindow - s.nextOutgoingID
		if w > math.MaxInt32 {
			w = 0
		}

		s.remoteWindow = int64(w)
		close(s.winSig)
		s.winSig = make(chan struct{})
	}

	var l any
	if f.Handle != nil {
		l = s.links[*f.Handle]
	}

	s.mu.Unlock()

	if sl, ok := l.(*sendLink); ok {
		sl.onFlow(f)
	}

	if f.Echo && f.Handle == nil {
		s.send(s.baseFlow())
	}
}

// acquireWindow blocks until a transfer may be sent, returning its transfer-id.
func (s *session) acquireWindow(ctx context.Context) (uint32, bool) {
	for {
		s.mu.Lock()

		if s.remoteWindow > 0 {
			s.remoteWindow--
			id := s.nextOutgoingID
			s.nextOutgoingID++
			s.mu.Unlock()

			return id, true
		}

		sig := s.winSig
		s.mu.Unlock()

		select {
		case <-sig:
		case <-ctx.Done():
			return 0, false
		}
	}
}

func (s *session) teardown() {
	s.cancel()

	s.mu.Lock()
	links := make([]any, 0, len(s.links))

	for _, l := range s.links {
		links = append(links, l)
	}

	s.links = map[uint32]any{}
	s.mu.Unlock()

	for _, l := range links {
		if rl, ok := l.(*recvLink); ok {
			rl.ep.Close()
		}
	}
}

func (s *session) onDisposition(d *Disposition) {
	if !d.Role {
		return
	}

	last := d.First
	if d.Last != nil {
		last = *d.Last
	}

	s.mu.Lock()
	hits := make(map[uint32]*sentDelivery)

	for id, sd := range s.unsettled {
		if id >= d.First && id <= last {
			hits[id] = sd
		}
	}

	for id := range hits {
		delete(s.unsettled, id)
	}

	s.mu.Unlock()

	for id, sd := range hits {
		s.settleSent(id, sd, d)
	}
}

func (s *session) settleSent(id uint32, sd *sentDelivery, d *Disposition) {
	if g, ok := sd.link.ep.(SettleGuard); ok {
		if e := g.SettleError(sd.out); e != nil {
			sd.link.ep.Release(sd.out)
			sd.link.abort(e)

			return
		}
	}

	if d.State == nil || d.State.Kind == StateReceived {
		sd.link.ep.Release(sd.out)
	} else {
		sd.link.ep.Settle(sd.out, *d.State)
	}

	if !d.Settled {
		s.send(&Disposition{Role: false, First: id, Settled: true, State: d.State})
	}
}
