//nolint:mnd // AMQP window/handshake constants
package amqp1

import (
	"context"
	"errors"
	"math"
	"sync"
)

type sendLink struct {
	s       *session
	ep      SenderEndpoint
	ctx     context.Context
	cancel  context.CancelFunc
	wake    chan struct{}
	stopNxt context.CancelFunc
	mu      sync.Mutex
	handle  uint32
	credit  uint32
	count   uint32
	settled bool
	drain   bool
}

func (l *sendLink) signalLocked() {
	close(l.wake)
	l.wake = make(chan struct{})
}

func (l *sendLink) onFlow(f *Flow) {
	l.mu.Lock()

	if f.LinkCredit != nil {
		var peerCount uint32
		if f.DeliveryCount != nil {
			peerCount = *f.DeliveryCount
		}

		credit := peerCount + *f.LinkCredit - l.count
		if credit > math.MaxInt32 {
			credit = 0
		}

		l.credit = credit
	}

	l.drain = f.Drain
	if l.drain && l.stopNxt != nil {
		l.stopNxt()
	}

	l.signalLocked()
	l.mu.Unlock()

	if f.Echo {
		l.s.send(l.flowFrame())
	}
}

func (l *sendLink) flowFrame() *Flow {
	fl := l.s.baseFlow()

	l.mu.Lock()
	h, dc, lc := l.handle, l.count, l.credit
	fl.Drain = l.drain
	l.mu.Unlock()

	fl.Handle, fl.DeliveryCount, fl.LinkCredit = &h, &dc, &lc

	return fl
}

func (l *sendLink) waitCredit() bool {
	for {
		l.mu.Lock()

		if l.credit > 0 {
			l.mu.Unlock()

			return true
		}

		ch := l.wake
		l.mu.Unlock()

		select {
		case <-ch:
		case <-l.ctx.Done():
			return false
		}
	}
}

func (l *sendLink) finishDrain() {
	l.mu.Lock()
	l.count += l.credit
	l.credit = 0
	l.mu.Unlock()

	l.s.send(l.flowFrame())

	l.mu.Lock()
	l.drain = false
	l.mu.Unlock()
}

func (l *sendLink) run() {
	defer l.finish()

	for l.waitCredit() {
		nctx, stop := context.WithCancel(l.ctx)

		l.mu.Lock()
		l.stopNxt = stop
		draining := l.drain
		l.mu.Unlock()

		if draining {
			stop()
		}

		out, err := l.ep.Next(nctx)
		stop()

		switch {
		case l.ctx.Err() != nil:
			if out != nil {
				l.ep.Release(out)
			}

			return
		case err != nil && nctx.Err() == nil:
			l.fail(err)

			return
		case out == nil:
			l.endDrainIfNeeded()
		case !l.transmit(out):
			return
		}
	}
}

func (l *sendLink) endDrainIfNeeded() {
	l.mu.Lock()
	d := l.drain
	l.mu.Unlock()

	if d {
		l.finishDrain()
	}
}

func (l *sendLink) fail(err error) {
	e := &Error{Condition: CondInternal, Description: err.Error()}
	_ = errors.As(err, &e)

	l.s.mu.Lock()
	delete(l.s.links, l.handle)
	l.s.mu.Unlock()

	l.s.send(&Detach{Handle: l.handle, Closed: true, Error: e})
}

func (l *sendLink) finish() {
	l.cancel()

	l.s.mu.Lock()

	var mine []*sentDelivery

	for id, sd := range l.s.unsettled {
		if sd.link == l {
			mine = append(mine, sd)
			delete(l.s.unsettled, id)
		}
	}

	l.s.mu.Unlock()

	for _, sd := range mine {
		l.ep.Release(sd.out)
	}

	l.ep.Close()
}

func (l *sendLink) chunkLimit(t *Transfer) int {
	probe := *t
	probe.More = true

	body, err := EncodePerformative(&probe)
	if err != nil {
		return 1
	}

	return max(int(l.s.c.maxFrame)-frameHeaderLen-len(body)-8, 1)
}

func (l *sendLink) takeCredit() bool {
	if !l.waitCredit() {
		return false
	}

	l.mu.Lock()
	l.credit--
	l.count++
	l.mu.Unlock()

	return true
}

func (l *sendLink) transmit(out *Outgoing) bool {
	if !l.takeCredit() {
		l.ep.Release(out)

		return false
	}

	out.Settled = out.Settled || l.settled
	payload := out.Payload

	for first := true; ; first = false {
		tid, ok := l.s.acquireWindow(l.ctx)
		if !ok {
			if first {
				l.ep.Release(out)
			}

			return false
		}

		t := &Transfer{Handle: l.handle}
		if first {
			format := out.Format
			t.DeliveryID, t.DeliveryTag, t.MessageFormat, t.Settled = &tid, out.Tag, &format, out.Settled
			l.register(tid, out)
		}

		chunk := payload
		if limit := l.chunkLimit(t); len(chunk) > limit {
			chunk, t.More = chunk[:limit], true
		}

		payload = payload[len(chunk):]

		if l.s.c.writeWithPayload(FrameAMQP, l.s.channel, t, chunk) != nil {
			return false
		}

		if !t.More {
			return true
		}
	}
}

func (l *sendLink) register(id uint32, out *Outgoing) {
	if out.Settled {
		return
	}

	l.s.mu.Lock()
	l.s.unsettled[id] = &sentDelivery{link: l, out: out}
	l.s.mu.Unlock()
}
