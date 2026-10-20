package azureservicebus

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	amqpPollWait = 30 * time.Second
	replyBuffer  = 64
)

// amqpConn is the per-connection link resolver.
type amqpConn struct {
	h       *Handler
	b       AMQPBackend
	mu      *lockmetrics.RWMutex
	replies map[string]chan *amqp1.Outgoing
	claimed bool
	tagSeq  atomic.Uint64
}

func newAMQPConnLock() *lockmetrics.RWMutex { return lockmetrics.New("azureservicebus.amqpconn") }

func (c *amqpConn) Close() { c.mu.Close() }

func (c *amqpConn) lock() *lockmetrics.RWMutex { return c.mu }

func notFound(addr string) *amqp1.Error {
	return &amqp1.Error{
		Condition:   amqp1.CondNotFound,
		Description: "The messaging entity '" + addr + "' could not be found.",
	}
}

func (c *amqpConn) authorized() bool {
	if !c.h.ValidateSAS {
		return true
	}

	mu := c.lock()
	mu.RLock("authorized")
	defer mu.RUnlock()

	return c.claimed
}

func (c *amqpConn) markClaimed() {
	mu := c.lock()
	mu.Lock("markClaimed")
	c.claimed = true
	mu.Unlock()
}

func (c *amqpConn) replyChan(addr string, create bool) chan *amqp1.Outgoing {
	mu := c.lock()
	mu.Lock("replyChan")
	defer mu.Unlock()

	ch, ok := c.replies[addr]
	if !ok && create {
		ch = make(chan *amqp1.Outgoing, replyBuffer)
		c.replies[addr] = ch
	}

	return ch
}

func unsupportedFilter(src *amqp1.Source) bool {
	if src == nil {
		return false
	}

	if src.Dynamic {
		return true
	}

	for k := range src.Filter {
		if strings.Contains(strings.ToLower(amqpKey(k)), "session") {
			return true
		}
	}

	return false
}

func amqpKey(k any) string {
	switch v := k.(type) {
	case string:
		return v
	case amqp1.Symbol:
		return string(v)
	}

	return ""
}

func (c *amqpConn) AttachSender(a *amqp1.Attach) (amqp1.SenderEndpoint, *amqp1.Error) {
	if a.Source == nil || a.Target == nil {
		return nil, notFound("")
	}

	addr, err := parseAMQPAddr(a.Source.Address)
	if err != nil {
		return nil, notFound(a.Source.Address)
	}

	if addr.cbs || addr.mgmt {
		if addr.mgmt && !addr.resolve(c.b) {
			return nil, notFound(a.Source.Address)
		}

		return &rpcSender{ch: c.replyChan(a.Target.Address, true)}, nil
	}

	if unsupportedFilter(a.Source) {
		return nil, &amqp1.Error{Condition: amqp1.CondNotImplemented, Description: "sessions are not supported"}
	}

	if !c.authorized() {
		return nil, &amqp1.Error{Condition: amqp1.CondUnauthorized, Description: noClaim}
	}

	if !addr.resolve(c.b) || addr.ref.Subscription == "" && !addr.ref.IsQueue() {
		return nil, notFound(a.Source.Address)
	}

	return &entitySender{
		b:        c.b,
		ref:      addr.ref,
		dlq:      addr.dlq,
		peekLock: a.RcvSettleMode == amqp1.ReceiverSettleSecond,
	}, nil
}

func (c *amqpConn) AttachReceiver(a *amqp1.Attach) (amqp1.ReceiverEndpoint, *amqp1.Error) {
	if a.Target == nil {
		return nil, notFound("")
	}

	addr, err := parseAMQPAddr(a.Target.Address)
	if err != nil {
		return nil, notFound(a.Target.Address)
	}

	switch {
	case addr.cbs:
		return &rpcReceiver{c: c, ref: EntityRef{}, cbs: true}, nil
	case addr.mgmt:
		if !addr.resolve(c.b) {
			return nil, notFound(a.Target.Address)
		}

		return &rpcReceiver{c: c, ref: addr.ref}, nil
	}

	if !c.authorized() {
		return nil, &amqp1.Error{Condition: amqp1.CondUnauthorized, Description: noClaim}
	}

	if addr.dlq || !addr.resolve(c.b) {
		return nil, notFound(a.Target.Address)
	}

	return &entityReceiver{b: c.b, ref: addr.ref}, nil
}

// entityReceiver accepts messages sent to a queue or topic.
type entityReceiver struct {
	b   AMQPBackend
	ref EntityRef
}

func (*entityReceiver) Close() {}

func (r *entityReceiver) Deliver(_ context.Context, in *amqp1.Incoming) amqp1.DeliveryState {
	msgs, err := decodeIncoming(in)
	if err != nil {
		return rejected(amqp1.CondDecode, err.Error())
	}

	for _, m := range msgs {
		if _, serr := r.b.Send(r.ref, fromAMQP(m)); serr != nil {
			return rejected(amqp1.CondNotFound, serr.Error())
		}
	}

	return amqp1.DeliveryState{Kind: amqp1.StateAccepted}
}

func rejected(cond amqp1.Symbol, desc string) amqp1.DeliveryState {
	return amqp1.DeliveryState{
		Kind:  amqp1.StateRejected,
		Error: &amqp1.Error{Condition: cond, Description: desc},
	}
}

func decodeIncoming(in *amqp1.Incoming) ([]*amqp1.Message, error) {
	m, err := amqp1.DecodeMessage(in.Payload)
	if err != nil {
		return nil, err
	}

	if in.Format != amqp1.BatchMessageFormat {
		return []*amqp1.Message{m}, nil
	}

	return splitBatch(m)
}

// entitySender feeds a receiving client from a queue or subscription.
type entitySender struct {
	b        AMQPBackend
	ref      EntityRef
	dlq      bool
	peekLock bool
}

type sentRef struct {
	msgID string
	token string
}

func (*entitySender) Close() {}

func (s *entitySender) Next(ctx context.Context) (*amqp1.Outgoing, error) {
	for {
		info, err := s.b.PeekLockWait(ctx, s.ref, s.dlq, 0, amqpPollWait)

		switch {
		case err == nil:
			if out, ok := s.outgoing(info); ok {
				return out, nil
			}
		case ctx.Err() != nil:
			return nil, ctx.Err()
		case !errors.Is(err, ErrMessageNotFound):
			return nil, notFound(s.ref.Queue + s.ref.Topic)
		}
	}
}

func (s *entitySender) outgoing(info MessageInfo) (*amqp1.Outgoing, bool) {
	payload, err := toAMQP(info, time.Now()).Encode()
	if err != nil {
		_ = s.b.Abandon(s.ref, s.dlq, info.MessageID, info.LockToken)

		return nil, false
	}

	tag := []byte(info.LockToken)
	if u, ok := parseUUID(info.LockToken); ok {
		tag = guidTag(u)
	}

	out := &amqp1.Outgoing{Tag: tag, Payload: payload, Ref: &sentRef{msgID: info.MessageID, token: info.LockToken}}

	if !s.peekLock {
		if s.b.Complete(s.ref, s.dlq, info.MessageID, info.LockToken) != nil {
			return nil, false
		}

		out.Settled = true
	}

	return out, true
}

func (s *entitySender) Settle(o *amqp1.Outgoing, st amqp1.DeliveryState) {
	r, ok := o.Ref.(*sentRef)
	if !ok {
		return
	}

	switch st.Kind {
	case amqp1.StateAccepted:
		_ = s.b.Complete(s.ref, s.dlq, r.msgID, r.token)
	case amqp1.StateRejected:
		s.rejectMessage(r, st.Error)
	case amqp1.StateModified:
		if st.UndeliverableHere && !st.DeliveryFailed {
			_ = s.b.Defer(s.ref, r.msgID, r.token)

			return
		}

		_ = s.b.Abandon(s.ref, s.dlq, r.msgID, r.token)
	default:
		_ = s.b.Abandon(s.ref, s.dlq, r.msgID, r.token)
	}
}

func (s *entitySender) rejectMessage(r *sentRef, e *amqp1.Error) {
	if s.dlq {
		_ = s.b.Complete(s.ref, true, r.msgID, r.token)

		return
	}

	reason, desc := "", ""
	if e != nil {
		reason, desc = infoString(e.Info, appDeadLetterWhy), infoString(e.Info, appDeadLetterMsg)
		if desc == "" {
			desc = e.Description
		}
	}

	_ = s.b.DeadLetter(s.ref, r.msgID, r.token, reason, desc)
}

func infoString(m amqp1.Map, key string) string {
	v, _ := m[key].(string)

	return v
}

func (s *entitySender) Release(o *amqp1.Outgoing) {
	if r, ok := o.Ref.(*sentRef); ok {
		_ = s.b.Abandon(s.ref, s.dlq, r.msgID, r.token)
	}
}
