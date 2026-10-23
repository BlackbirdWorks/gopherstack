package azureeventhubs

import (
	"context"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	maxEpoch         = uint64(1) << 62
	propEpoch        = "com.microsoft:epoch"
	propReceiverName = "com.microsoft:receiver-name"
	condStolen       = amqp1.Symbol("amqp:link:stolen")
	noClaim          = "no valid claim"
)

type amqpConn struct {
	h       *Handler
	b       *InMemoryBackend
	mu      *lockmetrics.RWMutex
	replies map[string]chan *amqp1.Outgoing
	rights  []string
	claimed bool
	tagSeq  uint64
}

func newConnLock() *lockmetrics.RWMutex { return lockmetrics.New("azureeventhubs.amqpconn") }

func (c *amqpConn) Close() { c.mu.Close() }

func notFound(addr string) *amqp1.Error {
	return &amqp1.Error{
		Condition:   amqp1.CondNotFound,
		Description: "The messaging entity '" + addr + "' could not be found.",
	}
}

func unauthorized() *amqp1.Error {
	return &amqp1.Error{Condition: amqp1.CondUnauthorized, Description: noClaim}
}

func (c *amqpConn) can(right string) bool {
	if !c.h.ValidateSAS {
		return true
	}

	c.mu.RLock("can")
	defer c.mu.RUnlock()

	return c.claimed && hasRight(c.rights, right)
}

func (c *amqpConn) authenticated() bool {
	if !c.h.ValidateSAS {
		return true
	}

	c.mu.RLock("authenticated")
	defer c.mu.RUnlock()

	return c.claimed
}

func (c *amqpConn) grant(rights []string) {
	c.mu.Lock("grant")
	c.claimed = true
	c.rights = rights
	c.mu.Unlock()
}

func (c *amqpConn) replyChan(addr string, create bool) chan *amqp1.Outgoing {
	c.mu.Lock("replyChan")
	defer c.mu.Unlock()

	ch, ok := c.replies[addr]
	if !ok && create {
		ch = make(chan *amqp1.Outgoing, replyBuffer)
		c.replies[addr] = ch
	}

	return ch
}

func (c *amqpConn) nextTag() []byte {
	c.mu.Lock("nextTag")
	c.tagSeq++
	n := c.tagSeq
	c.mu.Unlock()

	return []byte(strconv.FormatUint(n, 10))
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

func propInt64(m amqp1.Map, name string) (int64, bool) {
	for k, v := range m {
		if amqpKey(k) != name {
			continue
		}

		switch x := v.(type) {
		case int64:
			return x, true
		case int32:
			return int64(x), true
		case uint64:
			return int64(min(x, maxEpoch)), true
		case uint32:
			return int64(x), true
		}
	}

	return 0, false
}

func propString(m amqp1.Map, name string) string {
	for k, v := range m {
		if amqpKey(k) == name {
			s, _ := v.(string)

			return s
		}
	}

	return ""
}

func (c *amqpConn) resolve(a amqpAddr, raw string) (HubRef, *amqp1.Error) {
	ref, ok := c.b.ResolveHub(a.hub)
	if !ok {
		return HubRef{}, notFound(raw)
	}

	return ref, nil
}

func (c *amqpConn) resolveMgmt(a amqpAddr, raw string) (HubRef, *amqp1.Error) {
	if a.hub == "" {
		return HubRef{}, nil
	}

	return c.resolve(a, raw)
}

func (c *amqpConn) AttachSender(a *amqp1.Attach) (amqp1.SenderEndpoint, *amqp1.Error) {
	if a.Source == nil || a.Target == nil {
		return nil, notFound("")
	}

	addr, err := parseAMQPAddr(a.Source.Address)
	if err != nil {
		return nil, notFound(a.Source.Address)
	}

	switch addr.kind {
	case addrCBS:
		return &rpcSender{ch: c.replyChan(a.Target.Address, true)}, nil
	case addrMgmt:
		if _, e := c.resolveMgmt(addr, a.Source.Address); e != nil {
			return nil, e
		}

		return &rpcSender{ch: c.replyChan(a.Target.Address, true)}, nil
	case addrReceive:
		return c.attachReader(a, addr)
	case addrSend:
	}

	return nil, notFound(a.Source.Address)
}

func (c *amqpConn) AttachReceiver(a *amqp1.Attach) (amqp1.ReceiverEndpoint, *amqp1.Error) {
	if a.Target == nil {
		return nil, notFound("")
	}

	addr, err := parseAMQPAddr(a.Target.Address)
	if err != nil {
		return nil, notFound(a.Target.Address)
	}

	switch addr.kind {
	case addrCBS:
		return &rpcReceiver{c: c, cbs: true}, nil
	case addrMgmt:
		ref, e := c.resolveMgmt(addr, a.Target.Address)
		if e != nil {
			return nil, e
		}

		return &rpcReceiver{c: c, ref: ref}, nil
	case addrSend:
		return c.attachWriter(a, addr)
	case addrReceive:
	}

	return nil, notFound(a.Target.Address)
}

func (c *amqpConn) attachWriter(a *amqp1.Attach, addr amqpAddr) (amqp1.ReceiverEndpoint, *amqp1.Error) {
	if !c.can(RightSend) {
		return nil, unauthorized()
	}

	ref, e := c.resolve(addr, a.Target.Address)
	if e != nil {
		return nil, e
	}

	if addr.partition >= 0 {
		if _, err := c.b.PartitionInfo(ref, addr.partition); err != nil {
			return nil, notFound(a.Target.Address)
		}
	}

	return &eventWriter{b: c.b, ref: ref, pid: addr.partition}, nil
}

// detachingReader attaches and then detaches with e, as the service does for a
// refused epoch receiver, so SDKs see a link error rather than a refused attach.
func detachingReader(e *amqp1.Error) (amqp1.SenderEndpoint, *amqp1.Error) {
	if e.Condition == amqp1.CondNotFound {
		return nil, e
	}

	return &failedReader{err: e}, nil
}

type failedReader struct{ err *amqp1.Error }

func (f *failedReader) Next(context.Context) (*amqp1.Outgoing, error) { return nil, f.err }
func (*failedReader) Settle(*amqp1.Outgoing, amqp1.DeliveryState)     {}
func (*failedReader) Release(*amqp1.Outgoing)                         {}
func (*failedReader) Close()                                          {}

func (c *amqpConn) attachReader(a *amqp1.Attach, addr amqpAddr) (amqp1.SenderEndpoint, *amqp1.Error) {
	if !c.can(RightListen) {
		return nil, unauthorized()
	}

	ref, e := c.resolve(addr, a.Source.Address)
	if e != nil {
		return nil, e
	}

	sel, err := selectorOf(a.Source)
	if err != nil {
		return nil, &amqp1.Error{Condition: amqp1.CondNotAllowed, Description: err.Error()}
	}

	cursor, err := c.b.Resolve(ref, addr.partition, sel)
	if err != nil {
		return nil, notFound(a.Source.Address)
	}

	var epoch *int64
	if v, ok := propInt64(a.Properties, propEpoch); ok {
		epoch = &v
	}

	name := propString(a.Properties, propReceiverName)
	if name == "" {
		name = a.Name
	}

	reg, err := c.b.RegisterReceiver(ref, addr.group, addr.partition, name, epoch)
	if err != nil {
		return detachingReader(receiverError(err, a.Source.Address))
	}

	return &eventReader{c: c, b: c.b, ref: ref, pid: addr.partition, reg: reg, cursor: cursor}, nil
}
