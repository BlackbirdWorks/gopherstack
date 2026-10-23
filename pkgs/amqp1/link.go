package amqp1

import "context"

func (s *session) attach(a *Attach) bool {
	s.mu.Lock()
	_, dup := s.links[a.Handle]
	full := len(s.links) >= maxLinksPer
	s.mu.Unlock()

	if dup || full {
		s.c.closeWithError(&Error{Condition: CondResourceLimit, Description: "cannot attach link"})

		return false
	}

	if a.Role {
		return s.attachSender(a)
	}

	return s.attachReceiver(a)
}

// refuse answers an attach with a null terminus followed by a closing detach.
func (s *session) refuse(a *Attach, e *Error) bool {
	reply := &Attach{Name: a.Name, Handle: a.Handle, Role: !a.Role, SndSettleMode: a.SndSettleMode}
	if !s.send(reply) {
		return false
	}

	return s.send(&Detach{Handle: a.Handle, Closed: true, Error: e})
}

func (s *session) attachSender(a *Attach) bool {
	ep, e := s.c.ch.AttachSender(a)
	if e != nil {
		return s.refuse(a, e)
	}

	ctx, cancel := context.WithCancel(s.ctx)
	l := &sendLink{
		s:       s,
		handle:  a.Handle,
		ep:      ep,
		ctx:     ctx,
		cancel:  cancel,
		wake:    make(chan struct{}),
		settled: a.SndSettleMode == SenderSettleSettled,
	}

	s.mu.Lock()
	s.links[a.Handle] = l
	s.mu.Unlock()

	if d, ok := ep.(DeferredAttach); ok {
		go l.openAsync(d, a)

		return true
	}

	return l.open(a)
}

func (l *sendLink) open(a *Attach) bool {
	reply := &Attach{
		Name: a.Name, Handle: a.Handle, Role: false, SndSettleMode: a.SndSettleMode, RcvSettleMode: a.RcvSettleMode,
		Source: a.Source, Target: a.Target, MaxMessageSize: 0,
	}

	if r, ok := l.ep.(AttachReplier); ok {
		r.ReplyAttach(reply)
	}

	if !l.s.send(reply) {
		return false
	}

	go l.run()

	return true
}

func (l *sendLink) openAsync(d DeferredAttach, a *Attach) {
	e := d.Ready(l.ctx)
	if l.ctx.Err() != nil {
		l.ep.Close()

		return
	}

	if e != nil {
		l.s.mu.Lock()
		delete(l.s.links, l.handle)
		l.s.mu.Unlock()
		l.cancel()
		l.ep.Close()
		l.s.refuse(a, e)

		return
	}

	if !l.open(a) {
		l.cancel()
		l.ep.Close()
	}
}

func (l *sendLink) abort(e *Error) {
	l.s.mu.Lock()
	delete(l.s.links, l.handle)
	l.s.mu.Unlock()

	l.s.send(&Detach{Handle: l.handle, Closed: true, Error: e})
	l.cancel()
}

func (s *session) attachReceiver(a *Attach) bool {
	ep, e := s.c.ch.AttachReceiver(a)
	if e != nil {
		return s.refuse(a, e)
	}

	l := &recvLink{s: s, handle: a.Handle, ep: ep, deliveryCount: a.InitialDeliveryCount, credit: linkCredit}

	s.mu.Lock()
	s.links[a.Handle] = l
	s.mu.Unlock()

	reply := &Attach{
		Name: a.Name, Handle: a.Handle, Role: true, SndSettleMode: a.SndSettleMode, RcvSettleMode: ReceiverSettleFirst,
		Source: a.Source, Target: a.Target, MaxMessageSize: s.c.srv.cfg.MaxMessageSize,
	}
	if !s.send(reply) {
		return false
	}

	return s.send(l.flow())
}

func (s *session) detach(d *Detach) {
	s.mu.Lock()
	l := s.links[d.Handle]
	delete(s.links, d.Handle)
	s.mu.Unlock()

	switch v := l.(type) {
	case *sendLink:
		v.cancel()
	case *recvLink:
		v.ep.Close()
	case nil:
		return
	}

	s.send(&Detach{Handle: d.Handle, Closed: true})
}
