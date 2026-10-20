//nolint:mnd // AMQP window/handshake constants
package amqp1

type recvLink struct {
	s             *session
	ep            ReceiverEndpoint
	buf           []byte
	tag           []byte
	format        uint32
	id            uint32
	handle        uint32
	deliveryCount uint32
	credit        uint32
	started       bool
	settled       bool
}

func (l *recvLink) flow() *Flow {
	fl := l.s.baseFlow()
	h, dc, lc := l.handle, l.deliveryCount, l.credit
	fl.Handle, fl.DeliveryCount, fl.LinkCredit = &h, &dc, &lc

	return fl
}

func (s *session) onTransfer(t *Transfer) bool {
	s.mu.Lock()
	l := s.links[t.Handle]
	s.nextIncomingID++

	if s.incomingWindow > 0 {
		s.incomingWindow--
	}

	replenish := s.incomingWindow < sessionWindow/2
	if replenish {
		s.incomingWindow = sessionWindow
	}

	s.mu.Unlock()

	rl, ok := l.(*recvLink)
	if !ok {
		s.c.closeWithError(&Error{Condition: CondIllegalState, Description: "transfer on unknown link"})

		return false
	}

	if replenish {
		s.send(s.baseFlow())
	}

	rl.onTransfer(t)

	return true
}

func (l *recvLink) reset() {
	l.buf, l.tag, l.started, l.settled = nil, nil, false, false
}

func (l *recvLink) onTransfer(t *Transfer) {
	if !l.started {
		if t.DeliveryID == nil {
			l.s.closeLink(l.handle, &Error{Condition: CondDecode, Description: "transfer without delivery-id"})

			return
		}

		l.started, l.tag, l.id = true, t.DeliveryTag, *t.DeliveryID
		if t.MessageFormat != nil {
			l.format = *t.MessageFormat
		}
	}

	if t.Aborted {
		l.reset()

		return
	}

	l.settled = l.settled || t.Settled

	if uint64(len(l.buf))+uint64(len(t.Payload)) > l.s.c.srv.cfg.MaxMessageSize {
		l.s.closeLink(l.handle, &Error{Condition: CondMessageTooLarge, Description: "message too large"})

		return
	}

	l.buf = append(l.buf, t.Payload...)
	if t.More {
		return
	}

	l.complete()
}

func (l *recvLink) complete() {
	in := &Incoming{Tag: l.tag, Payload: l.buf, Format: l.format, Settled: l.settled}
	id := l.id

	l.reset()

	l.deliveryCount++
	if l.credit > 0 {
		l.credit--
	}

	st := l.ep.Deliver(l.s.ctx, in)

	if !in.Settled {
		l.s.send(&Disposition{Role: true, First: id, Settled: true, State: &st})
	}

	if l.credit < linkCredit/2 {
		l.credit = linkCredit
		l.s.send(l.flow())
	}
}

// closeLink detaches a link with an error and releases its endpoint.
func (s *session) closeLink(handle uint32, e *Error) {
	s.mu.Lock()
	l := s.links[handle]
	delete(s.links, handle)
	s.mu.Unlock()

	switch v := l.(type) {
	case *recvLink:
		v.ep.Close()
	case *sendLink:
		v.cancel()
	}

	s.send(&Detach{Handle: handle, Closed: true, Error: e})
}
